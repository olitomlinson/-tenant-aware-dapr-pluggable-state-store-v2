using Npgsql;
using System.Collections.Concurrent;

namespace Helpers
{
    public class Pgsql
    {
        private readonly string _SafeSchema;
        private readonly string _SafeTable;
        private readonly string _schema;
        private readonly string _table;
        private ILogger _logger;
        private readonly NpgsqlConnection _connection;

        /*  DATABASE RESOURCE LOCKS 
                These locks are important to prevent a storm of resource creation 
                on a highly parallel cold start. Yes, this does cause a performance 
                degredation but this provides a more predictable & stable experience,
                without having to run all database transactions in SERIALIZABLE isolation mode.
                
                Risk : The dictionaries will grow due to the unbounded nature of tenants.
                To mitigate this, the dictionarys could potentially be replaced with MemoryCache
                objects. (Assuming modern MemoryCache in .NET 6 is good to go)
        */
        static private ConcurrentDictionary<string, object> _locks = new ConcurrentDictionary<string, object>();
        static private ConcurrentDictionary<string, string> _resourcesLedger = new ConcurrentDictionary<string, string>();

        public Pgsql(string schema, string table, NpgsqlConnection connection, ILogger logger)
        {
            if (string.IsNullOrEmpty(schema))
                throw new ArgumentException("'schema' is not set");
            _SafeSchema = Safe(schema);
            _schema = schema;

            if (string.IsNullOrEmpty(table))
                throw new ArgumentException("'table' is not set");
            _SafeTable = Safe(table);
            _table = table;

            _logger = logger;

            _connection = connection;
        }

        public async Task CreateSchemaIfNotExistsAsync(NpgsqlTransaction transaction = null)
        {
            var sql = 
                @$"CREATE SCHEMA IF NOT EXISTS {_SafeSchema} 
                AUTHORIZATION postgres;
                
                CREATE OR REPLACE FUNCTION {_SafeSchema}.delete_key_with_etag_v1(
                    tbl regclass,
                    keyvalue text,
                    etagvalue xid,
                    OUT success boolean)
                    RETURNS boolean
                    LANGUAGE 'plpgsql'
                    COST 100
                    VOLATILE PARALLEL UNSAFE
                AS $BODY$     
                BEGIN
                EXECUTE format('
                    DELETE FROM %s
                    WHERE  key = $1 AND xmin = $2
                    RETURNING TRUE', tbl)
                USING   keyvalue, etagvalue
                INTO    success;
                RETURN;  -- optional in this case
                END
                $BODY$;

                ALTER FUNCTION {_SafeSchema}.delete_key_with_etag_v1(regclass, text, xid)
                    OWNER TO postgres;

                CREATE OR REPLACE FUNCTION {_SafeSchema}.delete_key_v1(
                    tbl regclass,
                    keyvalue text,
                    OUT success boolean)
                    RETURNS boolean
                    LANGUAGE 'plpgsql'
                    COST 100
                    VOLATILE PARALLEL UNSAFE
                AS $BODY$     
                BEGIN
                EXECUTE format('
                    DELETE FROM %s
                    WHERE  key = $1
                    RETURNING TRUE', tbl)
                USING   keyvalue
                INTO    success;
                RETURN;  -- optional in this case
                END
                $BODY$;

                ALTER FUNCTION {_SafeSchema}.delete_key_v1(regclass, text)
                    OWNER TO postgres;
                ";
            
            _logger.LogDebug($"{nameof(CreateSchemaIfNotExistsAsync)} - {sql}");
            
            await using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
            await cmd.ExecuteNonQueryAsync();

            _logger.LogDebug($"{nameof(CreateSchemaIfNotExistsAsync)} - Schema Created : {_SafeSchema}");
        }
        
        public async Task CreateTableIfNotExistsAsync(NpgsqlTransaction transaction = null)
        {
            var sql = 
                @$"CREATE TABLE IF NOT EXISTS {SchemaAndTable} 
                ( 
                    key text NOT NULL PRIMARY KEY COLLATE pg_catalog.""default"" 
                    ,value jsonb NOT NULL
                    ,insertdate TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                    ,updatedate TIMESTAMP WITH TIME ZONE NULL
                    ,expiredate TIMESTAMP WITH TIME ZONE NULL
                ) 
                TABLESPACE pg_default; 
                ALTER TABLE IF EXISTS {SchemaAndTable} OWNER to postgres;
                
                INSERT INTO ""pluggable_metadata"".""tenant"" (tenant_id, schema_id, table_id) VALUES ('{SchemaAndTable}', '{_schema}', '{_table}') ON CONFLICT (tenant_id) DO NOTHING;
                ";

            _logger.LogDebug($"{nameof(CreateTableIfNotExistsAsync)} - SQL : [{sql}]");

            await using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
            await cmd.ExecuteNonQueryAsync();

            _logger.LogDebug($"{nameof(CreateTableIfNotExistsAsync)} - Table Created : {SchemaAndTable}");
        }


        private string Safe(string input)
        {
            return $"\"{input}\"";
        }

        public string SchemaAndTable 
        { 
            get {
                return $"{_SafeSchema}.{_SafeTable}";
            }
        }

        public async Task<Tuple<string,string>> GetAsync(string key, NpgsqlTransaction transaction = null)
        {
            string value = "";
            string etag = "";
            string sql = 
                @$"SELECT 
                    key
                    ,value
                    ,xmin::text
                FROM {SchemaAndTable} 
                WHERE 
                    key = (@key)
                    AND (expiredate IS NULL OR expiredate > CURRENT_TIMESTAMP)       
                    ";

            _logger.LogInformation($"{nameof(GetAsync)} - key: [{key}], value: [{value}], sql: [{sql}]");

            await using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
            {
                cmd.Parameters.AddWithValue("key", key);
                await using (var reader = await cmd.ExecuteReaderAsync())
                while (await reader.ReadAsync())
                {
                    value = reader.GetString(1);
                    etag = reader.GetString(2);
                    _logger.LogDebug($"{nameof(GetAsync)} - Result - key: {reader.GetString(0)}, value: {value}, etag : {etag}");

                    return new Tuple<string,string>(value, etag);
                }
            }
            return new Tuple<string,string>(null,null);
        }

        public async Task UpsertAsync(string key, string value, string etag, int ttl, NpgsqlTransaction transaction = null)
        {
            await EnsureDatabaseResourcesExistAsync(
                transaction, 
                onDatabaseResourcesExist: async () => {
                    await InsertOrUpdateAsync(key, value, etag, ttl, transaction);
                }
            );
        }

        private async Task EnsureDatabaseResourcesExistAsync(NpgsqlTransaction transaction, Func<Task> onDatabaseResourcesExist)
        {
            var removeResourcesFromCache =  new []{ 
                await GateAccessToResourceCreationAsync($"S:{_schema}", () => CreateSchemaIfNotExistsAsync(transaction)), 
                await GateAccessToResourceCreationAsync($"T:{_schema}-{_table}", () => CreateTableIfNotExistsAsync(transaction))
            };    

            // It's possible for the local resource cache to become unsyncronised. One example is if a record can't be inserted into a brand new tenant, 
            // the transaction will fail, and the new schema/table will be rolledback, however the cache (resourceLedger) will still think the schema/table are created.
            // To avoid this, if any db exception occurs, we remove that schema/table from resourceLedger cache, so that the next operation for that schema/tenant
            // will be offered the opportunity to create the schema/table if it does not exist in the db.
            try 
            {
                await onDatabaseResourcesExist();
            }
            catch(PostgresException ex) when (ex.TableDoesNotExist())
            {
                foreach(var action in removeResourcesFromCache)
                    action();
                throw ex;
            } 
        }

        private async Task<Action> GateAccessToResourceCreationAsync(string resourceName, Func<Task> resourceFactory)
        {
            // check the in-memory ledger to see if the resource has already been created
            // (remember that this ledger is not global, it's per pluggable component instance (think pod instance)!)
            if (_resourcesLedger.TryGetValue(resourceName, out string _)) 
                return () => _resourcesLedger.TryRemove(new KeyValuePair<string, string>(resourceName,""));
             
            // get the lock for this particular resource
            var _lock = _locks.GetOrAdd(resourceName, (x) => { return new (); });

            // wait patiently until we have exlusive access of the resource...
            lock (_lock) 
            {
                // check ledger again to make sure the resource hasn't been created by some other racing thread...
                if (_resourcesLedger.TryGetValue(resourceName, out string _)) 
                    return () => _resourcesLedger.TryRemove(new KeyValuePair<string, string>(resourceName,""));
                
                // resource doesn't exist, no other thread has exlusive access, so create it now...
                resourceFactory().Wait();

                // while we have exlusive write-access, update the ledger to show it has been created
                _resourcesLedger.TryAdd(resourceName, DateTime.UtcNow.ToString());
            } 

            return () => _resourcesLedger.TryRemove(new KeyValuePair<string, string>(resourceName,""));
        }

        private async Task InsertOrUpdateAsync(string key, string value, string etag, int ttlInSeconds = 0, NpgsqlTransaction transaction = null)
        {
            int rowsAffected = 0;  
            var correlationId = Guid.NewGuid().ToString("N").Substring(23);

            var queryExpiredate = "NULL";
            if (ttlInSeconds > 0)
		        queryExpiredate = $"CURRENT_TIMESTAMP + interval '{ttlInSeconds} seconds'";


            if (String.IsNullOrEmpty(etag))
            {
                var query = @$"INSERT INTO {SchemaAndTable} 
                (
                    key
                    ,value
                    ,expiredate
                ) 
                VALUES 
                (
                    @1 
                    ,@2
                    ,{queryExpiredate}
                )
                ON CONFLICT (key)
                DO
                UPDATE SET 
                    value = @2
                    ,updatedate = CURRENT_TIMESTAMP
                    ,expiredate = {queryExpiredate}
                ;";

                _logger.LogDebug($"{nameof(InsertOrUpdateAsync)} ({correlationId}) - Etag not present - key: [{key}], value: [{value}], sql: [{query}]");

                await using (var cmd = new NpgsqlCommand(query, _connection, transaction))
                {
                    cmd.Parameters.AddWithValue("1", NpgsqlTypes.NpgsqlDbType.Text, key);
                    cmd.Parameters.AddWithValue("2", NpgsqlTypes.NpgsqlDbType.Jsonb, value);

                    rowsAffected = await cmd.ExecuteNonQueryAsync();
                    _logger.LogDebug($"{nameof(InsertOrUpdateAsync)} ({correlationId}) - Row inserted/updated: {rowsAffected} ");
                }
            }
            else
            {
                uint etagi = 0;
                try 
                { 
                    etagi = Convert.ToUInt32(etag,10); 
                }
                catch(Exception ex)
                {
                    throw new Dapr.PluggableComponents.Components.StateStore.ETagInvalidException();
                }

                var query = @$"
                UPDATE {SchemaAndTable} 
                SET 
                    value = @2
                    ,updatedate = CURRENT_TIMESTAMP
                    ,expiredate = {queryExpiredate}
                WHERE 
                    key = (@1)
                    AND xmin = (@3)
                    AND (expiredate IS NULL OR expiredate > CURRENT_TIMESTAMP) 
                ;";
                
                _logger.LogDebug($"{nameof(InsertOrUpdateAsync)} ({correlationId}) - Etag present - key: [{key}], value: [{value}], etag: [{etag}], sql: [{query}]");

                await using (var cmd = new NpgsqlCommand(query, _connection, transaction))
                {
                    cmd.Parameters.AddWithValue("1", NpgsqlTypes.NpgsqlDbType.Text, key);
                    cmd.Parameters.AddWithValue("2", NpgsqlTypes.NpgsqlDbType.Jsonb, value);
                    cmd.Parameters.AddWithValue("3", NpgsqlTypes.NpgsqlDbType.Xid, etagi);

                    rowsAffected = await cmd.ExecuteNonQueryAsync();
                    _logger.LogDebug($"{nameof(InsertOrUpdateAsync)} ({correlationId}) - Row updated: {rowsAffected}");
                }
            }

            if (rowsAffected == 0 && !string.IsNullOrEmpty(etag))
            {
                _logger.LogDebug($"{nameof(InsertOrUpdateAsync)} ({correlationId}) - Etag present but no rows modified, throwing EtagMismatchException");
                throw new Dapr.PluggableComponents.Components.StateStore.ETagMismatchException();
            }
        }

        public async Task DeleteAsync(string key, string etag, NpgsqlTransaction transaction = null)
        {      
            var sql = "";
            if (string.IsNullOrEmpty(etag))
                sql = $"SELECT * FROM {_SafeSchema}.delete_key_v1(tbl := '{_schema}.{_table}', keyvalue := '{key}')";
            else
                sql = $"SELECT * FROM {_SafeSchema}.delete_key_with_etag_v1(tbl := '{_schema}.{_table}', keyvalue := '{key}', etagvalue := '{etag}')";
            _logger.LogDebug($"{nameof(DeleteAsync)} - Sql : [{sql}]");

            using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
            {
                cmd.Parameters.Add(new NpgsqlParameter("success", System.Data.DbType.Boolean) { Direction = System.Data.ParameterDirection.Output });
                var result = await cmd.ExecuteScalarAsync();

                if (!string.IsNullOrEmpty(etag) && result is System.DBNull){
                    _logger.LogDebug($"{nameof(DeleteAsync)} - Etag present but no rows deleted, throwing EtagMismatchException");
                    throw new Dapr.PluggableComponents.Components.StateStore.ETagMismatchException();
                }
                else if (result is System.DBNull)
                    _logger.LogDebug($"{nameof(DeleteAsync)} - Result : DBnull");
                else if (result is true)
                    _logger.LogDebug($"{nameof(DeleteAsync)} - Result : {(bool)result}");
            }
        }
    }
}


public static class PostgresExtensions{
    public static bool TableDoesNotExist(this PostgresException ex){
        return (ex.SqlState == "42P01");
    }

    public static bool FunctionDoesNotExist( this PostgresException ex){
        return (ex.SqlState == "42883");
    }
}
