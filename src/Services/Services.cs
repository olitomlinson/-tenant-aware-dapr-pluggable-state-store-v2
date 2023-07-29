// ------------------------------------------------------------------------
// Copyright 2022 The Dapr Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ------------------------------------------------------------------------

using Helpers;
using Npgsql;
using Dapr.PluggableComponents.Components;
using Dapr.PluggableComponents.Components.StateStore;

namespace DaprComponents.Services;

public class StateStoreService : IStateStore, IPluggableComponentFeatures,  ITransactionalStateStore
{
    private readonly string _instanceId;
    private readonly ILogger<StateStoreService> _logger;
    private StateStoreInitHelper _stateStoreInitHelper;

    public StateStoreService(string instanceId, ILogger<StateStoreService> logger, StateStoreInitHelper stateStoreInitHelper)
    {
        _instanceId = instanceId;
        _logger = logger;
        _stateStoreInitHelper = stateStoreInitHelper;

    }

    public async Task InitAsync(MetadataRequest request, CancellationToken cancellationToken = default)
    {
        return;
    }

    public async Task<string[]> GetFeaturesAsync(CancellationToken cancellationToken = default)
    {
        using (_logger.BeginNamedScope("GetFeatures", ( "DaprInstanceId", _instanceId)))
        {
            string[] features = { "ETAG", "TRANSACTIONAL" };
            _logger.LogInformation($"Registering State Store Features : {string.Join(",", features)}");
            return features;
        }
    }

    public async Task DeleteAsync(StateStoreDeleteRequest request, CancellationToken cancellationToken = default)
    {
         _logger.LogInformation($"{nameof(DeleteAsync)}");
        
        (var dbfactory, var conn) = await _stateStoreInitHelper.GetDbFactory();
        using (conn)
        {
            var tran = await conn.BeginTransactionAsync();
            try 
            {
                await dbfactory(request.Metadata).DeleteAsync(request.Key, request.ETag ?? String.Empty, tran);
            }
            catch(Exception ex)
            {   
                await tran.RollbackAsync();
                _logger.LogDebug($"{nameof(DeleteAsync)} - rolled back transaction");
                throw;
            }
            await tran.CommitAsync();
            _logger.LogDebug($"{nameof(DeleteAsync)} - transaction commited");
        }
        return;
    }

    public async Task<StateStoreGetResponse?> GetAsync(StateStoreGetRequest request, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation($"{nameof(GetAsync)}");

        (var dbfactory, var conn) = await _stateStoreInitHelper.GetDbFactory();
        using (conn)
        {
            try 
            {
                var (value, etag) = await dbfactory(request.Metadata).GetAsync(request.Key);              
                if (value != null)
                    return new StateStoreGetResponse
                    {
                        Data = System.Text.Encoding.UTF8.GetBytes(value),
                        ETag = etag
                    };  
            } 
            catch(StateStoreInitHelperException ex) when (ex.Message.StartsWith("Missing Tenant Id"))
            {
                // TODO : This needs turning into a meaningful error to the client, but it is currently not possible
                throw ex;
            }
            catch(PostgresException ex) when (ex.TableDoesNotExist())
            {
                _logger.LogError(ex, "Table does not exist");
            }

            _logger.LogDebug($"{nameof(GetAsync)} - State not found with key : [{request.Key}]");
            return new StateStoreGetResponse();            
        }
    }

    public async Task SetAsync(StateStoreSetRequest request, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation($"{nameof(SetAsync)}");
                
        (var dbfactory, var conn) = await _stateStoreInitHelper.GetDbFactory();
        using (conn)
        {
            NpgsqlTransaction tran = null;
            try
            {
                tran = await conn.BeginTransactionAsync();
                var value = System.Text.Encoding.UTF8.GetString(request.Value.Span);
                await dbfactory(request.Metadata).UpsertAsync(request.Key, value, request.ETag ?? String.Empty, GetTTLfromOperationMetadata(request.Metadata), tran);   
                await tran.CommitAsync();
            }
            catch(Exception ex)
            {
                await tran.RollbackAsync();
                _logger.LogError(ex, $"{nameof(SetAsync)} - Rollback");
                throw;
            }
        }   
        return;
    }

    public async Task TransactAsync(StateStoreTransactRequest request, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation($"{nameof(TransactAsync)} - Set/Delete");

        if (!request.Operations.Any())
            return;

        (var dbfactory, var conn) = await _stateStoreInitHelper.GetDbFactory();
        using (conn)
        {
            var tran = await conn.BeginTransactionAsync();
            try 
            {
                foreach(var op in request.Operations)
                {
                    await op.Visit(
                        onDeleteRequest: async (delete) => {
                            var db = dbfactory(delete.Metadata);
                            await db.DeleteAsync(delete.Key, delete.ETag ?? String.Empty, tran);
                        },
                        onSetRequest: async (set) => {     
                            var db = dbfactory(set.Metadata);
                            // TODO : Need to implement 'something' here with regards to 'isBinary',
                            // but I do not know what this is trying to achieve. See existing pgSQL built-in component 
                            // https://github.com/dapr/components-contrib/blob/d3662118105a1d8926f0d7b598c8b19cd9dc1ccf/state/postgresql/postgresdbaccess.go#L135
                            var value = System.Text.Encoding.UTF8.GetString(set.Value.Span);
                            await db.UpsertAsync(set.Key, value, set.ETag ?? String.Empty, GetTTLfromOperationMetadata(request.Metadata), tran); 
                        }
                    );
                }
                await tran.CommitAsync();
            }
            catch(Exception ex)
            {
                await tran.RollbackAsync();
                _logger.LogError(ex, $"{nameof(TransactAsync)} - Rollback");
                throw;
            } 
        }
    }

    private int GetTTLfromOperationMetadata(IReadOnlyDictionary<string,string> metadata)
    {
        if (metadata.TryGetValue("ttlInSeconds", out string ttl))
            return Convert.ToInt32(ttl);
        return 0;
    }
}

public static class LoggerExtensions
{
    public static IDisposable BeginNamedScope(this ILogger logger, string name, params ValueTuple<string, object>[] properties)
    {
        var dictionary = properties.ToDictionary(p => p.Item1, p => p.Item2);
        dictionary[name + ".Scope"] = Guid.NewGuid();
        return logger.BeginScope(dictionary);
    }
}