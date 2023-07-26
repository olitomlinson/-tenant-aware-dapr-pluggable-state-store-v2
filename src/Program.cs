using DaprComponents.Services;
using Helpers;
using Dapr.PluggableComponents;
using Dapr.PluggableComponents.Components.StateStore;
using Dapr.PluggableComponents.Components;

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
            context =>
            {
                if (context.MetadataRequest is null)
                    throw new InvalidOperationException("MetadataRequest is not set");

                var logger = context.ServiceProvider.GetRequiredService<ILogger<StateStoreService>>();
                var helper = new StateStoreInitHelper(new PgsqlFactory(logger), logger, context.MetadataRequest.Properties );
                var expiredDataCleanUpService = context.ServiceProvider.GetService<ExpiredDataCleanUpService>();
                
                if (!context.MetadataRequest.Properties.TryGetValue(CONNECTION_STRING_KEYWORD, out string connectionString))
                    throw new Exception($"Mandatory '{CONNECTION_STRING_KEYWORD}' metadata property not specified'");
                
                TaskCompletionSource allowInit = new TaskCompletionSource();
                expiredDataCleanUpService.TryRegisterStateStore(context.InstanceId, connectionString, allowInit);
                
                return new StateStoreService(context.InstanceId, logger, helper, allowInit);
            });
    });
app.Run();

