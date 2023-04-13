﻿using Dapr.Client;
using Xunit.Abstractions;

namespace IntegrationTests;

[Collection("Sequence")]  
public class StateIsolationTests : IClassFixture<PluggableContainer>
{
    private readonly ITestOutputHelper output;
    private readonly PluggableContainer _pluggableContainer;
    private readonly DaprClient _daprClient;
    private Func<string> GetRandomKey;
    private static string _pluggableStoreTable = "pluggable-postgres-table";
    private static string _pluggableStoreSchema = "pluggable-postgres-schema";
    private static string _InTreeStore = "standard-postgres";
    private Random _random = new Random();

    public static IEnumerable<object[]> AllStores(){
        foreach(var store in OnlyTenantStores())
            yield return store;
        yield return new object[] { _InTreeStore };
    }

    public static IEnumerable<object[]> OnlyTenantStores(){
        yield return new object[] { _pluggableStoreTable };
        yield return new object[] { _pluggableStoreSchema };
    }

    public StateIsolationTests(PluggableContainer pluggableContainer, ITestOutputHelper output)
    {
        _pluggableContainer = pluggableContainer;
        _daprClient = _pluggableContainer.GetDaprClient();
        this.output = output;
        GetRandomKey = () => {  return $"key-{_random.Next(1000000, 9999999)}"; };
    }

    [Fact]
    public async Task CheckDaprSideCarIsHealthy()
    {
        var healthy = await _daprClient.CheckHealthAsync();
        Assert.True(healthy);
    }

    [Theory]
    [MemberData(nameof(OnlyTenantStores))]
    public async Task StateIsSharedWithinTenant(string store)
    {
        var key = GetRandomKey();
        var value = "Bar";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync(store, key, value, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        
        var retrievedState = await _daprClient.GetStateAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Equal(value, retrievedState);
    }

    [Theory]
    [MemberData(nameof(OnlyTenantStores))]
    public async Task StateIsNotSharedAcrossTenants(string store)
    {
        var key = GetRandomKey();
        var value = "Bar";
        var tenantId = Guid.NewGuid().ToString();
        var illegalTenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync(store, key, value, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var retrievedState = await _daprClient.GetStateAsync<string>(store, key, metadata: illegalTenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Null(retrievedState);
    }

    [Theory]
    [MemberData(nameof(AllStores))]
    public async Task ObjectsCanBeStoredAndRetrieved(string store)
    {
        var key = GetRandomKey();
        var tenantId = Guid.NewGuid().ToString();
        var metadata = tenantId.AsMetaData();

        var seedValue = new TestClass() {
            TestStr = "foo",
            TestInt = 99999
        };

        await _daprClient.SaveStateAsync<TestClass>(store, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var get = await _daprClient.GetStateAsync<TestClass>(store, key, metadata: metadata);     

        Assert.Multiple(
            () => Assert.Equal(seedValue.TestInt, get.TestInt),
            () => Assert.Equal(seedValue.TestStr, get.TestStr)
        );
    }

    [Theory]
    [MemberData(nameof(AllStores))]
    public async Task ObjectUpdatesWithoutEtag(string store)
    {
        var key = GetRandomKey();
        var tenantId = Guid.NewGuid().ToString();
        var metadata = tenantId.AsMetaData();

        var seedValue = new TestClass() {
            TestStr = "Chicken",
            TestInt = 99999
        };

        await _daprClient.SaveStateAsync<TestClass>(store, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var firstGet = await _daprClient.GetStateAsync<TestClass>(store, key, metadata: metadata);   

        var updatedValue = new TestClass {
            TestStr = seedValue.TestStr,
            TestInt = seedValue.TestInt
        };

        await _daprClient.SaveStateAsync<TestClass>(store, key, updatedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<TestClass>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);  

        Assert.Multiple(
            () => Assert.Equal(seedValue.TestInt, firstGet.TestInt),
            () => Assert.Equal(seedValue.TestStr, firstGet.TestStr),
            () => Assert.Equal(updatedValue.TestStr, secondGet.TestStr),
            () => Assert.Equal(updatedValue.TestInt, secondGet.TestInt)
        );
    }

    [Theory]
    [MemberData(nameof(AllStores))]
    public async Task UpdatesWithoutEtag(string store)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync(store, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var firstGet = await _daprClient.GetStateAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";
        await _daprClient.SaveStateAsync(store, key, updatedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Multiple(
            () => Assert.Equal(seedValue,firstGet),
            () => Assert.Equal(updatedValue, secondGet)
        );
    }

[Theory]
    [MemberData(nameof(AllStores))]
    public async Task UpdatesWithEtag(string store)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(store, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";

        var success = await _daprClient.TrySaveStateAsync<string>(store, key, updatedValue, etag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
  
        Assert.Multiple(
            () => Assert.Equal(seedValue,firstGet),
            () => Assert.Equal(updatedValue, secondGet)
        );
    }

    [Theory]
    [MemberData(nameof(AllStores))]
    public async Task UpdatesAndEtagInvalidIsThrown(string store)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(store, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";
        var malformedEtag = $"not-a-valid-etag";

        await Assert.ThrowsAsync<Dapr.DaprException>(async () => { 
            await _daprClient.TrySaveStateAsync<string>(store, key, updatedValue, malformedEtag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token); });
    }

    [Theory(Skip = "blocked by https://github.com/dapr/components-contrib/issues/2773")]
    [MemberData(nameof(AllStores))]
    public async Task UpdatesCantUseOldEtags(string store)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(store, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";
        await _daprClient.SaveStateAsync<string>(store, key, updatedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
  
        updatedValue = "Goose";
        var success = await _daprClient.TrySaveStateAsync<string>(store, key, updatedValue, etag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var thirdGet = await _daprClient.GetStateAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
  
        Assert.Multiple(
            () => Assert.False(success),
            () => Assert.Equal(secondGet, thirdGet)
        );
    }

    [Theory]
    [MemberData(nameof(AllStores))]
    public async Task DeleteWithoutEtag(string store)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(store, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var firstGet = await _daprClient.GetStateAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        await _daprClient.DeleteStateAsync(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Multiple(
            () => Assert.Equal(seedValue, firstGet),
            () => Assert.Null(secondGet)
        );
    }

    [Theory]
    [MemberData(nameof(AllStores))]
    public async Task DeleteWithEtag(string store)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(store, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var success = await _daprClient.TryDeleteStateAsync(store, key, etag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Multiple(
            () => Assert.True(success),
            () => Assert.Null(secondGet)
        );
    }

    [Theory]
    [MemberData(nameof(AllStores))]
    public async Task DeleteWithWrongEtagDoesNotDelete(string store)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(store, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var wrongEtag = $"123";
        var success = await _daprClient.TryDeleteStateAsync(store, key, wrongEtag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
       
        Assert.Multiple(
            () => Assert.Equal(seedValue, firstGet),
            () => Assert.False(success)
        );
    }

    [Theory(Skip = "test")]
    [MemberData(nameof(AllStores))]
    public async Task ParallelUpdatesAcrossUniqueTenants(string store)
    {

        IEnumerable<(string,string)> produceTestData(int upto)
        {
            string randomSuffix = GetRandomKey();
            for (int i = 0; i < upto; i ++)
            {
                yield return ($" {i}+{randomSuffix}", $"{i} some data to save");
            }
        }
    
        var cts = new CancellationTokenSource();
        var options = new ParallelOptions() { MaxDegreeOfParallelism = 50, CancellationToken = cts.Token };
        await Parallel.ForEachAsync(produceTestData(1000), options, async (input, token) =>
        {
            string tenantId = Guid.NewGuid().ToString();
            var metadata = tenantId.AsMetaData();
            await _daprClient.SaveStateAsync<string>(store, input.Item1, input.Item2, metadata: metadata, cancellationToken: token);
            var get = await _daprClient.GetStateAsync<string>(store, input.Item1, metadata: metadata);
            if (input.Item2 != get)
                Assert.Fail("get value did not match what was persisted");
        });
           
        Assert.True(true);
    }

    [Theory(Skip = "test")]
    [MemberData(nameof(AllStores))]
    public async Task ParallelUpdatesOnSingleTenant(string store)
    {
        var tenantId = Guid.NewGuid().ToString();
        var metadata = tenantId.AsMetaData();
       
        /*  PROBE
                This is a probe to warm up the tenant, otherwise 
                the table won't exist in some requests due to READ COMMITED transaction isolation.
                The quick fix is to warm up the tenant before hitting it with lots of parallelism.
                Maybe a better long term solution is to move to SERIALIZABLE isolation, but this
                will come with a performance degredation. */

        await _daprClient.SaveStateAsync<string>(store, "probe", "probe", metadata: metadata);
        await Task.Delay(500);
        /*  END PROBE */

        IEnumerable<(string,string)> produceTestData(int upto)
        {
            string randomSuffix = GetRandomKey();
            for (int i = 0; i < upto; i ++)
            {
                yield return ($" {i}+{randomSuffix}", $"{i} some data to save");
            }
        }
    
        var cts = new CancellationTokenSource();
        var options = new ParallelOptions() { MaxDegreeOfParallelism = 50, CancellationToken = cts.Token };
        await Parallel.ForEachAsync(produceTestData(1000), options, async (input, token) =>
        {
            await _daprClient.SaveStateAsync<string>(store, input.Item1, input.Item2, metadata: metadata, cancellationToken: token);
            var get = await _daprClient.GetStateAsync<string>(store, input.Item1, metadata: metadata);
            if (input.Item2 != get)
                Assert.Fail("get value did not match what was persisted");
        });
              
        Assert.True(true);
    }

    
    [Fact(Skip = "Not yet implemented")]
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
public class TestClass 
{
    public string TestStr { get; set; }
    public int TestInt { get; set; }
}