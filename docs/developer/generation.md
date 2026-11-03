# Regenerating code and API clients

Edit the source inputs below, regenerate, then review and commit the related tracked outputs with your source change. Generated files are not the place to edit an API contract or query.

Run Mage commands from the repository root. Follow the [developer prerequisites](../developer_guide.md#pre-requisites) for Go and Mage setup. Use the tool pins in [`tools.yaml`](../../tools.yaml) and the [test workflow](../../.github/workflows/test.yml) for reproducible output, including CI's protoc version. The local minimum-version checks alone do not guarantee identical output. `mage proto` and `mage sql` bootstrap the whole Go tool list; put `GOBIN` (or `GOPATH/bin`) on `PATH`. Downloads need network access or populated caches and writable tool/cache directories.

| If you change | Run | Review or commit |
| --- | --- | --- |
| Armada `.proto` files, HTTP annotations or Go models used by Swagger | `mage proto`, then `mage dotnet` to match the proto CI check | Tracked `*.pb.go`, gateway `*.pb.gw.go`, and the three `api.swagger.json` / `api.swagger.go` pairs under `pkg/api/`, `pkg/api/binoculars/` and `pkg/api/schedulerobjects/` |
| Scheduler or Query API SQL queries or schema inputs | `mage sql` | Tracked `db.go`, `models.go` and `query.sql.go` in both configured database packages |
| API definitions consumed by Lookout UI | Run `mage proto` first for upstream API changes, then follow the [UI install and client-generation instructions](../../internal/lookoutui/README.md#installing-dependencies-and-generating-openapi-client-code) and [build instructions](../../internal/lookoutui/README.md#building-the-application-for-production) | The Swagger inputs are tracked; the generated TypeScript clients and UI build are ignored |

## Protobuf and API changes

For example, `Submit.SubmitJobs`, its `JobSubmitRequest` and the `POST /v1/job/submit` annotation are editable in [`pkg/api/submit.proto`](../../pkg/api/submit.proto). Run:

```bash
mage proto
mage dotnet
```

Review `pkg/api/submit.pb.go`, `pkg/api/submit.pb.gw.go`, `pkg/api/api.swagger.json` and `pkg/api/api.swagger.go` for the corresponding Go, gateway, REST specification and embedded specification changes. The command regenerates all configured packages, so inspect the full diff. The [proto pipeline](../../magefiles/proto.go) owns the complete input list and Swagger merge/embedding steps; calling protoc alone does not reproduce it. See the [API guide](armada-api.md) for API ownership and usage.

`mage proto` needs host protoc and the bootstrapped Go generators. Its imported dependency tree at root `proto/` is ignored and recreated from Go modules; edit Armada's source files rather than that copied tree.

`mage dotnet` builds the protobuf/gRPC client in Docker using the [client project's selected protos](../../client/DotNet/ArmadaProject.Io.Client/ArmadaProject.Io.Client.csproj). It requires a running Docker daemon, repository bind-mount access, SDK image access and NuGet restore access; a host .NET SDK is not required. Generated C# and build outputs under `obj/` and `bin/` are ignored. This build is part of the proto drift check, but does not run the separate [DotNet Client workflow's solution tests](../../.github/workflows/dotnet-client.yml). Other language clients have their own workflows, such as [Python client development](../../client/python/CONTRIBUTING.md).

## SQL changes

`mage sql` runs both configurations:

| Configuration | Editable queries | Editable schema | Tracked output directory |
| --- | --- | --- | --- |
| [Scheduler](../../internal/scheduler/database/sql.yaml) | `internal/scheduler/database/query/query.sql` | `internal/scheduler/database/migrations/` | `internal/scheduler/database/` |
| [Query API](../../internal/server/queryapi/database/sql.yaml) | `internal/server/queryapi/database/query.sql` | `internal/lookout/schema/migrations/` | `internal/server/queryapi/database/` |

For example, edit `SelectNewJobs` in the scheduler query file and run:

```bash
mage sql
```

Review its generated method in `internal/scheduler/database/query.sql.go` and any other generated changes. The focused command `sqlc generate -f internal/scheduler/database/sql.yaml` runs only the scheduler configuration; it does not reproduce CI's aggregate check. Use the pinned SQLC from `tools.yaml`, including for focused generation.

These configurations read schema files without starting Postgres or applying migrations. Successful generation does not test queries against a database or verify migration compatibility.

## Inspecting results and choosing validation

After generation, inspect `git diff` and `git status --short`. On unchanged inputs with matching tools, expect no tracked changes; investigate unexpected drift rather than hand-editing generated output. Commit required tracked outputs with intentional input changes. Do not add ignored dependency, client or build trees; the UI's handwritten `src/openapi/workarounds.d.ts` is a tracked exception and must be preserved.

The proto and SQLC jobs in the [test workflow](../../.github/workflows/test.yml) check tracked drift after generation; they do not check ignored output. UI CI installs dependencies, generates clients, builds and runs tests. `yarn build` alone does not generate clients. Root `mage ui` wraps UI install, generation and build, while `mage generate` only runs `go generate ./...`.

Use an isolated checkout when checking for drift. Generation and builds also write caches and ignored intermediates. The .NET Docker helper leaves its stopped container behind; identify and remove only the container created by your run. Generation and client builds do not replace tests: choose checks for the affected code using the [developer guide](../developer_guide.md) and [UI README](../../internal/lookoutui/README.md#run-unit-tests).
