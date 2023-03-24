﻿using System.Net;
using Dapr.Client;

namespace IntegrationTests;

public class StateIsolationTests : IClassFixture<PluggableContainer>
{
    private readonly PluggableContainer _pluggableContainer;
    private readonly DaprClient _daprClient;

    public StateIsolationTests(PluggableContainer pluggableContainer)
    {
        _pluggableContainer = pluggableContainer;
        _daprClient = _pluggableContainer.GetDaprClient();
        _pluggableContainer.SetBaseAddress();
    }

    [Fact]
    public async Task DaprHealthCheck()
    {
        const string path = "/v1.0/healthz";

        var response = await _pluggableContainer.GetAsync(path)
        .ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
    }

    [Fact]
    public async Task StateIsSharedWithinTenant()
    {
        var ct = new CancellationTokenSource(5000).Token;

        var key = "Foo";
        var value = "Bar";
        var tenantId = "123";

        await _daprClient.SaveStateAsync("pluggable-postgres", key, value, metadata: tenantId.AsMetaData(), cancellationToken: ct);
        
        var retrievedState = await _daprClient.GetStateAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: ct);

        Assert.Equal(value, retrievedState);
    }

    [Fact]
    public async Task StateIsNotSharedAcrossTenants()
    {
        var ct = new CancellationTokenSource(5000).Token;

        var key = "Foo";
        var value = "Bar";
        var tenantId = "123";
        var illegalTenantId = "567";

        await _daprClient.SaveStateAsync("pluggable-postgres", key, value, metadata: tenantId.AsMetaData(), cancellationToken: ct);
        
        var retrievedState = await _daprClient.GetStateAsync<string>("pluggable-postgres", key, metadata: illegalTenantId.AsMetaData(), cancellationToken: ct);

        Assert.Null(retrievedState);
    }

    public async Task ScanEntireDatabaseForStateBleedAcrossTenants()
    {
        // TODO : Write a SQL query that scans through all tables in all schemas,
        // Looking for a uniqely generated value (value being state stored against a dapr key).
        // The value should only appear once, and it should be stored against the 
        // correct key, in the correct table, in the correct schema.

        // If the key appears more than once, or in an unexpected location, there 
        // has been a catastrophic error.
    }
}

public static class StringExtensions
{
    public static IReadOnlyDictionary<string,string> AsMetaData(this string tenantId){
        return new Dictionary<string, string> {{ "tenantId", tenantId}};
    }
}