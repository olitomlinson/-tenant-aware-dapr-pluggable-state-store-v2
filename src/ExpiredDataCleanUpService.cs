using Helpers;
using Npgsql;
using System.Collections.Concurrent;

public class ExpiredDataCleanUpService : BackgroundService
{
    private ConcurrentDictionary<string, Tuple<string, TaskCompletionSource>> _registeredComponents;
    private int _sequence = 0;
    private readonly ILogger<ExpiredDataCleanUpService> _logger;
    private bool _isMetadataTableEstablished = false;

    public ExpiredDataCleanUpService(ILogger<ExpiredDataCleanUpService> logger)
    {
        _logger = logger;
        _registeredComponents = new ConcurrentDictionary<string, Tuple<string,TaskCompletionSource>>();
    }

    public void TryRegisterStateStore(string instanceId, string connectionString, TaskCompletionSource allowInit)
    {
        _registeredComponents.TryAdd(instanceId, new Tuple<string,TaskCompletionSource>(connectionString, allowInit));
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Timed Hosted Service running.");

        await DoWork();

        using PeriodicTimer timer = new(TimeSpan.FromSeconds(5));

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                await DoWork();
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Timed Hosted Service is stopping.");
        }
    }

    private async Task DoWork()
    {
        _sequence += 1;

        if(!_registeredComponents.Any())
        {   
            _logger.LogInformation("No components found yet... Seq: {seq}", _sequence);
            return;
        }
        else
        {
            _logger.LogInformation($"{_registeredComponents.Count} components found. Seq: {_sequence}");
        }

        foreach(var component in _registeredComponents)
        {
            _logger.LogDebug($"component '{component.Key}' initialised. Seq: {_sequence}'");
            _logger.LogDebug($"component '{component.Key}' connnection string : '{component.Value}. Seq: {_sequence}'");  
        }


        foreach(var cs in _registeredComponents
            .Select(x => x.Value)
            .Distinct())
        {
            var connection = new NpgsqlConnection(cs.Item1);
            await connection.OpenAsync();  

            if (!_isMetadataTableEstablished)
            {
                await CreateTenantMetadataTableIfNotExistsAsync(connection);
                
                _isMetadataTableEstablished = true;
            }

            // ensure all the component inits are unblocked.
            foreach(var r in _registeredComponents){
                if (r.Value.Item2.Task.Status == TaskStatus.WaitingForActivation)
                    r.Value.Item2.SetResult();
            }

            List<string> tenantIdsToDelete = new List<string>();
            string tenantsToCheckForTTLdeletion = 
                @$"
                SELECT 
                    tenant_id, schema_id, table_id
                FROM 
                    ""pluggable_metadata"".""tenant"" 
                ORDER BY 
                    last_expired_at ASC NULLS FIRST
                LIMIT 1;
                ";
            using (var cmd = new NpgsqlCommand(tenantsToCheckForTTLdeletion, connection, null))
            {
                using (var reader = await cmd.ExecuteReaderAsync())
                while (reader.Read())
                {
                    var schemaAndTenant = reader.GetString(0);
                    var schemaId = reader.GetString(1);
                    var tableId = reader.GetString(2);
                    
                    _logger.LogInformation($"tenant : {schemaAndTenant}, schema: {schemaId}, table: {tableId}");
                    tenantIdsToDelete.Add(schemaAndTenant);
                }
            }

            foreach(var tenantId in tenantIdsToDelete)
            {
                var rowsAffected = await DeleteExpiredKeysAsync(tenantId, connection);
                rowsAffected = await UpdateLastExpiredTimestampAsync(tenantId, connection);
            }

            await connection.CloseAsync();  

        } 
    }

    public async Task CreateTenantMetadataTableIfNotExistsAsync(NpgsqlConnection connection)
    {
        var sql = 
            @$"CREATE SCHEMA IF NOT EXISTS ""pluggable_metadata"" 
            AUTHORIZATION postgres;
            
            CREATE TABLE IF NOT EXISTS ""pluggable_metadata"".""tenant""
            ( 
                tenant_id text NOT NULL PRIMARY KEY COLLATE pg_catalog.""default"" 
                ,schema_id text NOT NULL
                ,table_id text NOT NULL
                ,insert_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                ,last_expired_at TIMESTAMP WITH TIME ZONE NULL
            ) 
            TABLESPACE pg_default; 
            ALTER TABLE IF EXISTS ""pluggable_metadata"".""tenant"" OWNER to postgres;

            CREATE INDEX IF NOT EXISTS pluggable_metadata_tenant_last_expired_at ON ""pluggable_metadata"".""tenant"" (last_expired_at ASC NULLS FIRST);
            ";

        _logger.LogDebug($"{nameof(CreateTenantMetadataTableIfNotExistsAsync)} - {sql}");
        
        await using (var cmd = new NpgsqlCommand(sql, connection, null))
        await cmd.ExecuteNonQueryAsync();

        _logger.LogDebug($"{nameof(CreateTenantMetadataTableIfNotExistsAsync)} - 'pluggable_metadata.tenant' created");
        
    }

    private async Task<int> UpdateLastExpiredTimestampAsync(string schemaAndTable, NpgsqlConnection connection)
    {
        var query = @$"
            UPDATE ""pluggable_metadata"".""tenant"" 
            SET 
                last_expired_at = CURRENT_TIMESTAMP
            WHERE 
                tenant_id = '{schemaAndTable}'
            ;";

        using (var cmd = new NpgsqlCommand(query, connection, null))
        {
            return await cmd.ExecuteNonQueryAsync();       
        }
    }

    private async Task<int> DeleteExpiredKeysAsync(string schemaAndTable, NpgsqlConnection connection)
    {
        var sql = $"DELETE FROM {schemaAndTable} WHERE expiredate IS NOT NULL AND expiredate < CURRENT_TIMESTAMP";
        using (var cmd = new NpgsqlCommand(sql, connection, null))
        {
            var rowsAffected = await cmd.ExecuteNonQueryAsync();
            _logger.LogInformation($"rows deleted from '{schemaAndTable}': {rowsAffected}");
            return rowsAffected;
        }
    }
    public Task StopAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Expired Data Clean Up Service is stopping.");

        return Task.CompletedTask;
    }
}