using DaprComponents.Services;
using Helpers;
using Dapr.PluggableComponents;

const string CONNECTION_STRING_KEYWORD = "connectionString";

var app = DaprPluggableComponentsApplication.Create();

app.Services.AddSingleton<ExpiredDataCleanUpService>(sp => {
    return new ExpiredDataCleanUpService(
        sp.GetService<ILogger<ExpiredDataCleanUpService>>()); });

app.Services.AddHostedService<ExpiredDataCleanUpService>(sp => {
    return sp.GetService<ExpiredDataCleanUpService>(); });

app.RegisterService(
    "postgresql-tenant",
    serviceBuilder =>
    {
        serviceBuilder.RegisterDeferredStateStore(
            async context =>
            {
                if (context.MetadataRequest is null)
                    throw new InvalidOperationException("MetadataRequest is not set");

                var logger = context.ServiceProvider.GetRequiredService<ILogger<StateStoreService>>();
        
                var expiredDataCleanUpService = context.ServiceProvider.GetRequiredService<ExpiredDataCleanUpService>();
                
                if (!context.MetadataRequest.Properties.TryGetValue(CONNECTION_STRING_KEYWORD, out string connectionString))
                    throw new Exception($"Mandatory '{CONNECTION_STRING_KEYWORD}' metadata property not specified'");
                
                await expiredDataCleanUpService.TryRegisterStateStoreAsync(context.InstanceId, connectionString);
                var helper = new StateStoreInitHelper(new PgsqlFactory(logger), logger, context.MetadataRequest.Properties );
                var dbfactory = helper.GetDbFactory;
                
                return new StateStoreService(context.InstanceId, logger, dbfactory);
            });
    });
app.Run();

