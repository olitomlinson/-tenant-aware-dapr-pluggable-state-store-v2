[![.NET](https://github.com/olitomlinson/dapr-pluggable-state-store/actions/workflows/dotnet.yml/badge.svg)](https://github.com/olitomlinson/dapr-pluggable-state-store/actions/workflows/dotnet.yml)

### Status

This project is currently a Work in Progress. It is 99% functional, but requires some tidying up & security hardening.

### Purpose

A complete .NET 6 implementation of a [Dapr state store](https://docs.dapr.io/developing-applications/building-blocks/state-management/state-management-overview/) using the [Pluggable Components API](https://docs.dapr.io/developing-applications/develop-components/pluggable-components/pluggable-components-overview/).

#### What makes this different from the current in-tree Postgres Dapr Component? 

This component is specialised with tenant-aware state operations, such as 'Schema-per-Tenant' and 'Table-per-Tenant''

### Working capabilities

- Standard state store behaviors (`Set`, `Get`, `Delete`)
- Transactional API
- Etags

### What are tenant-aware state operations?

Tenant-aware state operations requires a client to specify a `tenantId` as part of the `metadata` on each State Store operation, which will dynamically prefix the `Schema`, or `Table` with the given `tenantId`, allowing the logical separation of data in a multi-tenant environment.

### To do

- Implement a native `BulkGet` and `BulkSet`
- Support `IsBinary` - Blocked by https://github.com/dapr/dapr/pull/6244"
- ~~Look again at `XMIN` for Etag~~
- Review Indexes (particulary around `key` and `etag`)

### Won't do

- Query API capability support

---

### Instructions to build and run (Inner dev loop)

- Obtain an instance of a postgresdb (With a user named 'postgres' with sufficient permissions to create schemas and tables in the db)
- Install Dapr CLI and ensure https://docs.dapr.io/getting-started/install-dapr-selfhost/ Dapr works.
- Pull this repo and open the `src` folder

Build & run the pluggable component :

`dotnet run`

Create a new `component.yaml` for the pluggable component and place it the default directory where Dapr discovers your components on your machine. Replace with your connection string to your postgresql db instance.

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: pluggable-postgres-schema
spec:
  type: state.postgresql-tenant
  version: v1
  metadata:
  - name: connectionString
    value: "<REPLACE-WITH-YOUR-CONNECTION-STRING>"
  - name: tenant
    value: schema
```

Run the dapr process :

`dapr run --app-id myapp --dapr-http-port 3500`

Persist a value against a key :

`POST http://localhost:3500/v1.0/state/pluggable-postgres-schema`

```json
[
  {
    "key": "1",
    "value": {
      "name": "Dave Mustaine"
    },
    "metadata": {
      "tenantId": "123"
    }
  },
  {
    "key": "2",
    "value": {
      "name": "Kirk Hammett"
    },
    "metadata": {
      "tenantId": "123"
    }
  }
]
```

Observe a new Scehma in your posgresql database has been created called `"123-public"`. Observe the persisted Key Value persisted in the `"state"` Table

<img width="702" alt="image" src="https://user-images.githubusercontent.com/4224880/202821328-95b9f1d6-49a3-431d-bd48-d673178a1f8f.png">

---

### Run with Docker Compose

This will create 
- Postgres container + volume
- Pluggable app (which uses the Pluggable Component .NET SDK)
- A dapr sidecar (dapr-http-port 3500)
- Docker network
- Volume for sharing the unix domain socket

Ensure the correct connection string is uncommended in `/DaprComponents/pluggable-postgres-table.yaml`. Look for the string starting with `host=db` and uncomment this, comment  any other connection strings!

`docker compose build`

`docker compose up`

Perform State Management queries against the pluggable State Store, hosted at `http://localhost:3500/v1.0/state/pluggable-postgres-table`

---

### Run on Kubernetes

Install postgres in your k8s cluster :

`helm repo add bitnami https://charts.bitnami.com/bitnami`

`helm install my-release bitnami/postgresql`

- Retain the password that is provided in the output
- Retain the DNS address thait is provided in the output, it may look something like `my-release-postgresql.default.svc.cluster.local`


Edit `/DaprComponents/pluggable-postgres-table.yaml` - Modify the connection string with the above DNS address and password.

It may look something like this ; 

`value: "host=my-release-postgresql.default.svc.cluster.local;port=5432;username=postgres;password=<REPLACE WITH PASSWORD FROM BITNAMI POSTGRES CHART INSTALL>;database=postgres"`

Ensure you comment out any other connection strings in the `pluggable.yaml` file

Build the pluggable component :

` docker build -f dockerfile -t pluggable-component .`

Deploy the pluggable component yaml : 

` kubectl apply -f ./DaprComponents/pluggable-postgres-table.yaml`

Deploy the app : 

` kubectl apply -f ./deploy.yaml`

Once the deployment is complete, port forward onto the dapr sidecars Dapr HTTP port (`dapr-http-port`) so you can access this from your host machine.

Perform State Management queries against the pluggable State Store, hosted at `http://localhost:3500/v1.0/state/pluggable-postgres-table`

---

### Run the Integration Tests

The integration tests use [TestContainers](https://dotnet.testcontainers.org/) to spin up all the dependencies (simimilar to docker compose). Test containers rely on Docker Engine, so ensure you have Docker for Desktop or equivalent installed.

_Note:_ these tests can take a while to complete on the first run through as Images are built & downloaded.

Navigate to the `IntegrationTests` folder : 

` dotnet test`
