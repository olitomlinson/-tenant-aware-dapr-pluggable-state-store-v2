using DaprComponents.Services;
using Helpers;
using Dapr.PluggableComponents;

var app = DaprPluggableComponentsApplication.Create();

var ensureMetadataTableIsCreated = new SemaphoreSlim(0,1);

app.Services.AddSingleton<ExpiredDataCleanUpService>(sp => {
    return new ExpiredDataCleanUpService(
        sp.GetService<ILogger<ExpiredDataCleanUpService>>(),
        ensureMetadataTableIsCreated ); });

app.Services.AddHostedService<ExpiredDataCleanUpService>(sp => {
    return sp.GetService<ExpiredDataCleanUpService>(); });

app.RegisterService(
    "postgresql-tenant",
    serviceBuilder =>
    {
        serviceBuilder.RegisterStateStore(
            context =>
            {                   
                var logger = context.ServiceProvider.GetRequiredService<ILogger<StateStoreService>>();
                var helper = new StateStoreInitHelper(new PgsqlFactory(logger), logger);
                var expiredDataCleanUpService = context.ServiceProvider.GetService<ExpiredDataCleanUpService>();

                TaskCompletionSource<string> registrationTask = expiredDataCleanUpService.TryRegisterStateStore(context.InstanceId);
                return new StateStoreService(context.InstanceId, logger, helper, ensureMetadataTableIsCreated, registrationTask);
            });
    });
app.Run();
