using Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NSubstitute;
using Npgsql;
using Dapr.PluggableComponents.Components;

namespace Tests;

[TestClass]
public class HappyPathTests
{
    [TestMethod]
    public async Task TenantIdIsPrefixedToDefaultSchemaName()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var componentMetadata = new Dictionary<string,string>() {
            { "connectionString",   "some-c-string" },
            { "tenant",             "schema"        }};
        var h = new StateStoreInitHelper(pgsqlFactory, Substitute.For<ILogger>(),componentMetadata);

        var operationMetadata = new Dictionary<string, string>(){
            { "tenantId", "123"}};

        h.TenantAwareDatabaseFactory?.Invoke(operationMetadata, null);

        pgsqlFactory.Received().Create("123-public", "state", null);
    }

    [TestMethod]
    public async Task TenantIdIsPrefixedToCustomSchemaName()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var componentMetadata = new Dictionary<string,string>() {
        {"connectionString",    "some-c-string" },
        {"tenant",              "schema"        },
        {"schema",              "custom"        }};
        var h = new StateStoreInitHelper(pgsqlFactory, Substitute.For<ILogger>(),componentMetadata);

        var operationMetadata = new Dictionary<string, string>(){
        {"tenantId", "123"}};
        h.TenantAwareDatabaseFactory?.Invoke(operationMetadata, null);

        pgsqlFactory.Received().Create("123-custom", "state", null);
    }

    [TestMethod]
    public async Task TenantIdIsPrefixedToDefaultTableName()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var componentMetadata = new Dictionary<string,string>(){
            { "connectionString", "some-c-string"},
            { "tenant", "table"}
        };
        var h = new StateStoreInitHelper(pgsqlFactory, Substitute.For<ILogger>(), componentMetadata);

        var operationMetadata = new Dictionary<string, string>();
        operationMetadata.Add("tenantId", "123");
        h.TenantAwareDatabaseFactory?.Invoke(operationMetadata, null);

        pgsqlFactory.Received().Create("public", "123-state", null);
    }

    [TestMethod]
    public async Task TenantIdIsPrefixedToCustomTableName()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var componentMetadata = new Dictionary<string,string>(){
            {"connectionString", "some-c-string"},
            {"tenant", "table"},
            {"table", "custom"}
        };
        var h = new StateStoreInitHelper(pgsqlFactory, Substitute.For<ILogger>(),componentMetadata);

        var operationMetadata = new Dictionary<string, string>();
        operationMetadata.Add("tenantId", "123");
        h.TenantAwareDatabaseFactory?.Invoke(operationMetadata, null);

        pgsqlFactory.Received().Create("public", "123-custom", null);
    }
}