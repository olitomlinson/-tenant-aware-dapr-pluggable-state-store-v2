using Dapr.PluggableComponents.Components;
using Npgsql;

namespace Helpers
{
    public class StateStoreInitHelper
    {
        private const string TABLE_KEYWORD = "table";
        private const string SCHEMA_KEYWORD = "schema";
        private const string TENANT_KEYWORD = "tenant";
        private const string CONNECTION_STRING_KEYWORD = "connectionString";
        private const string DEFAULT_TABLE_NAME = "state";
        private const string DEFAULT_SCHEMA_NAME = "public";
        private IPgsqlFactory _pgsqlFactory;
        private ILogger _logger;
        public Func<IReadOnlyDictionary<string, string>, NpgsqlConnection, Pgsql>? TenantAwareDatabaseFactory { get; private set; }
        private string _connectionString;

        public StateStoreInitHelper(IPgsqlFactory pgsqlFactory, ILogger logger, IReadOnlyDictionary<string,string> componentMetadataProperties){
            _pgsqlFactory = pgsqlFactory;
            _logger = logger;
            
            var tenantMode = GetTenantMode(componentMetadataProperties);        
            
            _connectionString = GetConnectionString(componentMetadataProperties);

            var defaultSchema = GetDefaultSchemaName(componentMetadataProperties);

            string defaultTable = GetDefaultTableName(componentMetadataProperties);  

            TenantAwareDatabaseFactory = 
                (operationMetadata, connection) => {
                    /* 
                        Why is this a func? 
                        Schema and Table are not known until a state operation is requested, 
                        as we rely on a combination on the component metadata and operation metadata,
                    */
                    
                    var tenantId = GetTenantIdFromMetadata(operationMetadata);
                    
                    switch(tenantMode){
                        case SCHEMA_KEYWORD :
                            return _pgsqlFactory.Create(
                                schema:             $"{tenantId}-{defaultSchema}", 
                                table:              defaultTable, 
                                connection); 
                        case TABLE_KEYWORD : 
                            return _pgsqlFactory.Create(
                                schema:             defaultSchema, 
                                table:              $"{tenantId}-{defaultTable}",
                                connection);
                        default:
                            throw new Exception("Couldn't instanciate the correct tenant-aware Pgsql wrapper");
                    }
                };
        }

        public async Task<(Func<IReadOnlyDictionary<string,string>, Pgsql>, NpgsqlConnection)> GetDbFactory()
        {
            var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();
            Func<IReadOnlyDictionary<string,string>,Pgsql> factory = (metadata) => {
                return TenantAwareDatabaseFactory(metadata, connection);
            };   
            return (factory, connection);
        }

        private string GetTenantMode(IReadOnlyDictionary<string,string> properties){
            bool isTenantAware = (properties.TryGetValue(TENANT_KEYWORD, out string tenantTarget));
            if (!isTenantAware)
                throw new StateStoreInitHelperException($"Mandatory '{TENANT_KEYWORD}' metdadata property not specified.");
            
            if (!(new string[]{ SCHEMA_KEYWORD, TABLE_KEYWORD }.Contains(tenantTarget)))
                throw new StateStoreInitHelperException($"Unsupported 'tenant' metadata property value of '{tenantTarget}'. Use 'schema' or 'table' instead.");
            
            return tenantTarget;
        }

        private string GetDefaultSchemaName(IReadOnlyDictionary<string,string> properties){
            if (!properties.TryGetValue(SCHEMA_KEYWORD, out string defaultSchema))
                defaultSchema = DEFAULT_SCHEMA_NAME;
            return defaultSchema;
        }

        private string GetDefaultTableName(IReadOnlyDictionary<string,string> properties){
           if (!properties.TryGetValue(TABLE_KEYWORD, out string defaultTable))
                defaultTable = DEFAULT_TABLE_NAME;
            return defaultTable;
        }

        private string GetConnectionString(IReadOnlyDictionary<string,string> properties){
            if (!properties.TryGetValue(CONNECTION_STRING_KEYWORD, out string connectionString))
                throw new StateStoreInitHelperException($"Mandatory '{CONNECTION_STRING_KEYWORD}' metadata property not specified'");
            return connectionString;
        }

        private string GetTenantIdFromMetadata(IReadOnlyDictionary<string, string> operationMetadata){
            operationMetadata.TryGetValue("tenantId", out string tenantId);   
            if (String.IsNullOrEmpty(tenantId))
                throw new StateStoreInitHelperException("Missing Tenant Id - 'metadata.tenantId' is a mandatory property");
            return tenantId;
        }
    }

    [System.Serializable]
    public class StateStoreInitHelperException : System.Exception
    {
        public StateStoreInitHelperException() { }
        public StateStoreInitHelperException(string message) : base(message) { }
        public StateStoreInitHelperException(string message, System.Exception inner) : base(message, inner) { }
        protected StateStoreInitHelperException(
            System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }
}
