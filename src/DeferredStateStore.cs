
using Dapr.PluggableComponents;
using Dapr.PluggableComponents.Components.StateStore;
using Dapr.PluggableComponents.Components;

internal sealed record DeferredContext(MetadataRequest? MetadataRequest, IServiceProvider ServiceProvider, string InstanceId, TaskCompletionSource AllowInitToComplete);

internal static class DaprPluggableComponentsServiceBuilderExtensions
{
    public static void RegisterDeferredStateStore<TStateStore>(this DaprPluggableComponentsServiceBuilder serviceBuilder, Func<DeferredContext, TStateStore> componentFactory)
        where TStateStore : IStateStore, IPluggableComponentFeatures, ITransactionalStateStore
    {
        serviceBuilder.RegisterStateStore(context => new DeferredStateStore<TStateStore>(context.ServiceProvider, componentFactory, context.InstanceId, new TaskCompletionSource()));
    }
}

internal sealed class DeferredStateStore<T> : IStateStore, IPluggableComponentFeatures, ITransactionalStateStore
    where T : IStateStore, IPluggableComponentFeatures, ITransactionalStateStore
{
    private readonly Func<DeferredContext, T> componentFactory;
    private T stateStore;
    private readonly IServiceProvider serviceProvider;
    private readonly string instanceId;
    private readonly TaskCompletionSource allowInitToComplete;

    public DeferredStateStore(IServiceProvider serviceProvider, Func<DeferredContext, T> componentFactory, string instanceId, TaskCompletionSource allowInitToComplete)
    {
        this.componentFactory = componentFactory;
        this.serviceProvider = serviceProvider;
        this.instanceId = instanceId;
        this.allowInitToComplete = allowInitToComplete;
    }

    #region IStateStore Members

    public Task DeleteAsync(StateStoreDeleteRequest request, CancellationToken cancellationToken = default)
    {
        return this.stateStore.DeleteAsync(request, cancellationToken);
    }

    public Task<StateStoreGetResponse?> GetAsync(StateStoreGetRequest request, CancellationToken cancellationToken = default)
    {
        return this.stateStore.GetAsync(request, cancellationToken);
    }

    public async Task InitAsync(MetadataRequest request, CancellationToken cancellationToken = default)
    {
        this.stateStore = this.componentFactory(new DeferredContext(request, serviceProvider, instanceId, allowInitToComplete));

        await allowInitToComplete.Task;
    }

    public Task SetAsync(StateStoreSetRequest request, CancellationToken cancellationToken = default)
    {
        return this.stateStore.SetAsync(request, cancellationToken);
    }

    #endregion

    #region IPluggableComponentFeatures Members

    public Task<string[]> GetFeaturesAsync(CancellationToken cancellationToken = default)
    {
        return this.stateStore.GetFeaturesAsync(cancellationToken);
    }

    #endregion

    #region ITransactionalStateStore Members

    public Task TransactAsync(StateStoreTransactRequest request, CancellationToken cancellationToken = default)
    {
        return this.stateStore.TransactAsync(request, cancellationToken);
    }

    #endregion
}