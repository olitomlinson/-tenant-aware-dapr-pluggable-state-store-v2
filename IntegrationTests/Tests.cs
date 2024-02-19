using Dapr.Client;
using Microsoft.Extensions.ObjectPool;
using Xunit.Abstractions;

namespace IntegrationTests;

[Collection("Sequence")]  
public class StateIsolationTests : IClassFixture<TestContainers>
{
    private readonly TestContainers _testContainers;
    private readonly DaprClient _daprClient;
    private Func<string> GetRandomKey;
    private Random _random = new Random();

    public static IEnumerable<object[]> AllStores{
        get {
            foreach(var store in AllStoresWithoutPostgresV2)
                yield return store;
            foreach(var store in PostgresV2)
                yield return store;
        }
    }

    public static IEnumerable<object[]> AllStoresWithoutPostgresV2
    {
        get {
            foreach(var store in OnlyTenantStores)
                yield return store;
            foreach(var store in PostgresV1)
                yield return store;
        }
    }

    public static IEnumerable<object[]> OnlyTenantStores
    {
        get {
            yield return new object[] { "pluggable-postgres-table" };
            yield return new object[] { "pluggable-postgres-schema" };
        }
    }

    public static IEnumerable<object[]> PostgresV1
    {
        get { yield return new object[] { "standard-postgres" }; }
    }

    public static IEnumerable<object[]> PostgresV2
    {
        get { yield return new object[] { "standard-postgres-v2" }; }
    }

    public StateIsolationTests(TestContainers testContainers, ITestOutputHelper output)
    {
        _testContainers = testContainers;
        _daprClient = _testContainers.GetDaprClient();
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
    [MemberData(nameof(AllStoresWithoutPostgresV2))]
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

    [Theory]
    [MemberData(nameof(PostgresV2))]
    public async Task PostgresV2_UpdatesAndEtagIsMismatched(string store)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(store, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";
        var malformedEtag = $"not-a-valid-etag";

        var result = await _daprClient.TrySaveStateAsync<string>(store, key, updatedValue, malformedEtag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token); 
        Assert.False(result);
    }   


    [Theory]
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

    [Theory]
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

    [Theory]
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

    [Theory]
    [MemberData(nameof(OnlyTenantStores))]
    public async Task ExpiredKeysAreNotReturned(string store)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync(store, key, seedValue, metadata: tenantId.AsMetaData(ttl: "5"), cancellationToken: new CancellationTokenSource(5000).Token);
        var firstGet = await _daprClient.GetStateAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Equal(seedValue,firstGet);

        await Task.Delay(6000);
        var SecondGet = await _daprClient.GetStateAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Null(SecondGet);
    }

    [Theory]
    [MemberData(nameof(OnlyTenantStores))]
    public async Task ExpireyCanBeExtended(string store)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync(store, key, seedValue, metadata: tenantId.AsMetaData(ttl: "5"), cancellationToken: new CancellationTokenSource(5000).Token);
        var firstGet = await _daprClient.GetStateAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        Assert.Equal(seedValue,firstGet);

        //extend the timeout for another 10, which goes beyond the original 5 seconds
        await _daprClient.SaveStateAsync(store, key, seedValue, metadata: tenantId.AsMetaData(ttl: "10"), cancellationToken: new CancellationTokenSource(5000).Token);
        await Task.Delay(7000);
        var SecondGet = await _daprClient.GetStateAsync<string>(store, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Equal(seedValue,SecondGet);
    }
}

public static class StringExtensions
{
    public static IReadOnlyDictionary<string,string> AsMetaData(this string tenantId, string? ttl = null){
        var dic = new Dictionary<string, string> {{ "tenantId", tenantId}};
        if (!string.IsNullOrEmpty(ttl)){
            dic.Add("ttlInSeconds", ttl);
        }
        return dic;
    }
}
public class TestClass 
{
    public string TestStr { get; set; }
    public int TestInt { get; set; }
}