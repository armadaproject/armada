# Regenerating code and API clients

Edit the source inputs below, regenerate, then review and commit the related tracked outputs with your source change. Generated files are not the place to edit an API contract or query.

Run Mage commands from the repository root. Follow the [developer prerequisites](../developer_guide.md#pre-requisites) for Go and Mage setup.

Generated output depends on tool versions, so use the versions CI uses or your diff may contain unrelated changes:

* Go-based generators (sqlc, the gateway and swagger protoc plugins) are pinned in [`tools.yaml`](../../tools.yaml). `mage proto` and `mage sql` download and install them for you, so they need network access; make sure `GOBIN` (or `GOPATH/bin`) is on your `PATH`.
* `protoc` itself must be installed on the host. Mage checks a minimum version, but it is not pinned in `tools.yaml`. CI uses version 23.3, so match that for identical output.

| If you change | Run | Review or commit |
| --- | --- | --- |
| Armada `.proto` files or HTTP annotations | `mage proto` | Tracked `*.pb.go`, gateway `*.pb.gw.go`, and the three `api.swagger.json` / `api.swagger.go` pairs under `pkg/api/`, `pkg/api/binoculars/` and `pkg/api/schedulerobjects/` |
| Scheduler or Query API SQL queries or schema inputs | `mage sql` | Tracked `db.go`, `models.go` and `query.sql.go` in both configured database packages |
| API definitions consumed by Lookout UI | Run `mage proto` first for upstream API changes, then follow the [UI install and client-generation instructions](../../internal/lookoutui/README.md#installing-dependencies-and-generating-openapi-client-code) and [build instructions](../../internal/lookoutui/README.md#building-the-application-for-production) | The Swagger inputs are tracked; the generated TypeScript clients and UI build are ignored |

The output lists identify files to inspect; a particular source change may affect only a subset.

## Protobuf and API changes

Armada's API is defined in Protocol Buffers (`.proto`) files under [`pkg/api`](../../pkg/api). From these, `protoc` generates the Go message and [gRPC](https://grpc.io) code (`*.pb.go`; gRPC is the RPC framework Armada's services use), a gateway that exposes the gRPC services as a REST API over HTTP (`*.pb.gw.go`), and Swagger (OpenAPI) specifications used by clients such as Lookout UI. See the [API guide](armada-api.md) for how the API is organised.

As an example, [`pkg/api/submit.proto`](../../pkg/api/submit.proto) defines:

* the `SubmitJobs` rpc (a method) on the `Submit` service,
* the `JobSubmitRequest` message (the request type, generated as a Go struct), and
* an HTTP annotation on `SubmitJobs` (`option (google.api.http)`) which tells the gateway to serve the rpc as `POST /v1/job/submit`.

After editing any of these, run:

```bash
mage proto
```

Review the files this changes:

* `pkg/api/submit.pb.go`: the Go messages and gRPC code
* `pkg/api/submit.pb.gw.go`: the HTTP gateway
* `pkg/api/api.swagger.json`: the OpenAPI specification
* `pkg/api/api.swagger.go`: the specification embedded in the Go binary

The command regenerates all configured packages, so inspect the full diff. The [proto pipeline](../../magefiles/proto.go) lists every input and performs the Swagger merge and embedding steps, so running `protoc` alone will not reproduce it.

The root `proto/` directory is a copy of imported dependencies, is ignored by git and is refilled on each run. Edit Armada's own `.proto` files, not that copy.

### Other proto clients

CI also checks that the .NET client builds from the protos. To reproduce that locally run `mage dotnet`, which builds the client inside Docker (so Docker must be running; a host .NET SDK is not needed). It also needs the same host `protoc` and Go setup as `mage proto`, and leaves a stopped container behind, which you can remove with `docker rm`. Other language clients have their own workflows, such as [Python client development](../../client/python/CONTRIBUTING.md).

## SQL changes

Armada uses [sqlc](https://sqlc.dev) to generate type-safe Go database code. You write SQL queries in a `query.sql` file and the schema in migration files (see the table below for paths); sqlc reads both and generates Go methods (`query.sql.go`) and table structs (`models.go`). To change database access, edit the SQL, not the Go.

There are two sqlc configurations, one for the scheduler database and one for the Query API database. `mage sql` runs both:

| Configuration | Editable queries | Editable schema | Tracked output directory |
| --- | --- | --- | --- |
| [Scheduler](../../internal/scheduler/database/sql.yaml) | `internal/scheduler/database/query/query.sql` | `internal/scheduler/database/migrations/` | `internal/scheduler/database/` |
| [Query API](../../internal/server/queryapi/database/sql.yaml) | `internal/server/queryapi/database/query.sql` | `internal/lookout/schema/migrations/` | `internal/server/queryapi/database/` |

For example, edit `SelectNewJobs` in the scheduler query file and run:

```bash
mage sql
```

Review its generated method in `internal/scheduler/database/query.sql.go` and any other generated changes. The focused command `sqlc generate -f internal/scheduler/database/sql.yaml` runs only the scheduler configuration; it does not reproduce CI's aggregate check. Use the sqlc version installed by `mage sql` (pinned in `tools.yaml`), including for focused generation.

These configurations read schema files without starting Postgres or applying migrations. Successful generation does not test queries against a database or verify migration compatibility.

## Inspecting results and choosing validation

After generation, inspect `git diff` and `git status --short`:

* On unchanged inputs with matching tool versions, expect no tracked changes. Investigate unexpected drift rather than hand-editing generated files.
* Commit the tracked outputs together with your input changes.
* Do not add ignored dependency, client or build directories.
* The UI's handwritten `src/openapi/workarounds.d.ts` is tracked and must be preserved.

The proto and SQLC jobs in the [test workflow](../../.github/workflows/test.yml) check tracked drift after generation; they do not check ignored output. UI CI installs dependencies, generates clients, builds and runs tests. `yarn build` alone does not generate clients. Root `mage ui` wraps UI install, generation and build, while `mage generate` only runs `go generate ./...`.

Successful generation does not establish API compatibility, and it does not replace tests. Choose checks for the affected code using the [developer guide](../developer_guide.md) and [UI README](../../internal/lookoutui/README.md#run-unit-tests).
