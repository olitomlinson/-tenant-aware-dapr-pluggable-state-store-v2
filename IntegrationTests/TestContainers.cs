using System.Net;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using DotNet.Testcontainers.Volumes;
using Testcontainers.PostgreSql;
using JetBrains.Annotations;
using Dapr.Client;

namespace IntegrationTests;

[UsedImplicitly]
public sealed class TestContainers : IAsyncLifetime
{
    private static readonly PluggableImage Image = new();
    private readonly IVolume _socketVolume;
    private readonly INetwork _network;
    private readonly IContainer _postgresContainer;
    private readonly IContainer _pluggableContainer;
    private readonly IContainer _daprContainer;
    private readonly ushort _dapr_http_port = 3501;
    private readonly ushort _dapr_grpc_port = 50002;
    private DaprClient _daprClient;
    private string _dapr_app_id;

    public TestContainers()
    {
        var daprComponentsDirectory = $"{Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)}/DaprComponents";

        var containerSuffix = Guid.NewGuid().ToString("N").Substring(23);
        _dapr_app_id = $"pluggableapp-{containerSuffix}";
        
        _network = new NetworkBuilder()
            .WithName($"network-{containerSuffix}")
            .Build();

        _postgresContainer = new PostgreSqlBuilder()
            .WithImage("postgres:15.2-alpine")
            .WithNetwork(_network)
            .WithName($"postgres-{containerSuffix}")
            .WithCommand("-c", "log_statement=all")
            .WithNetworkAliases("db")
            .Build();

        _socketVolume = new VolumeBuilder().Build();

        _daprContainer = new ContainerBuilder()
            .WithImage("daprio/daprd:1.12.0-rc.2-mariner")
            .WithName($"dapr-{containerSuffix}")
            .WithNetwork(_network)
            .WithNetworkAliases("dapr")
            .WithPortBinding(_dapr_http_port, true)
            .WithPortBinding(_dapr_grpc_port, true)
            .WithVolumeMount(_socketVolume, "/tmp/dapr-components-sockets")
            .WithResourceMapping($"{daprComponentsDirectory}/pluggable-postgres-table.yaml", "/DaprComponents/pluggable-postgres-table.yaml")
            .WithResourceMapping($"{daprComponentsDirectory}/pluggable-postgres-schema.yaml", "/DaprComponents/pluggable-postgres-schema.yaml")
            .WithResourceMapping($"{daprComponentsDirectory}/standard-postgres.yaml", "/DaprComponents/standard-postgres.yaml")
            .WithEntrypoint("./daprd")
            .WithCommand("-app-id", _dapr_app_id)
            .WithCommand("-dapr-http-port", _dapr_http_port.ToString())
            .WithCommand("-dapr-grpc-port", _dapr_grpc_port.ToString())
            .WithCommand("-resources-path", "/DaprComponents")
            .WithCommand("-log-level", "debug")
            .WithWaitStrategy(
                Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(request =>  
                request
                    .ForPort(_dapr_http_port)
                    .ForPath("/v1.0/healthz")
                    .ForStatusCode(HttpStatusCode.NoContent)))
            .Build();

        _pluggableContainer = new ContainerBuilder()
            .WithImage(Image)
            .WithName($"pluggable-component-{containerSuffix}")
            .WithVolumeMount(_socketVolume, "/tmp/dapr-components-sockets")
            .WithNetwork(_network)
            .Build();
    }

    public async Task InitializeAsync()
    {
        await Image.InitializeAsync().ConfigureAwait(false);
        await _network.CreateAsync().ConfigureAwait(false);
        await _socketVolume.CreateAsync().ConfigureAwait(false);
        await _pluggableContainer.StartAsync().ConfigureAwait(false);
        await _postgresContainer.StartAsync().ConfigureAwait(false);
        await _daprContainer.StartAsync().ConfigureAwait(false);

        Environment.SetEnvironmentVariable("DAPR_HTTP_PORT", _daprContainer.GetMappedPublicPort(_dapr_http_port).ToString());
        Environment.SetEnvironmentVariable("DAPR_GRPC_PORT", _daprContainer.GetMappedPublicPort(_dapr_grpc_port).ToString());
        _daprClient = new DaprClientBuilder().Build();
    }

    public async Task DisposeAsync()
    {
        await Image.DisposeAsync().ConfigureAwait(false);
        await _pluggableContainer.DisposeAsync().ConfigureAwait(false);
        await _postgresContainer.DisposeAsync().ConfigureAwait(false);
        await _daprContainer.DisposeAsync().ConfigureAwait(false);
        await _network.DeleteAsync().ConfigureAwait(false);
        await _socketVolume.DeleteAsync().ConfigureAwait(false);
    }
    public DaprClient GetDaprClient(){
        return _daprClient;
    }
}