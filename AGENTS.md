# AGENTS.md — Armada

Armada is a multi-Kubernetes cluster batch job meta-scheduler. Queues jobs via Pulsar + PostgreSQL, schedules across clusters with fair-share, gang scheduling, preemption, and per-queue resource limits.

## Codebase

Components follow: `cmd/<name>/main.go` (entry) → `internal/<name>/` (code) → `config/<name>/config.yaml` (defaults).

Data flow: Client → **Server** → Pulsar → **Ingesters** {SchedulerIngester→PostgreSQL, EventIngester→Redis, LookoutIngester→PostgreSQL} → **Scheduler** (reads PostgreSQL, caches in-memory) → **Executor** (creates pods on K8s).

| Area | Start here |
|------|-----------|
| Scheduler main loop | `internal/scheduler/scheduler.go` |
| Scheduling algorithm | `internal/scheduler/scheduling/scheduling_algo.go` |
| Preemption | `internal/scheduler/scheduling/preempting_queue_scheduler.go` |
| Gang scheduling | `internal/scheduler/scheduling/gang_scheduler.go` |
| Job DB (in-memory cache) | `internal/scheduler/jobdb/jobdb.go` |
| Node matching | `internal/scheduler/nodedb/nodematching.go` |
| Job submission API | `internal/server/submit/submit.go` |

## Commands

```bash
go build ./cmd/<component>                       # Build single component
mage buildDockers                                # Build all Docker images
go test ./internal/scheduler/...                 # Test a package
go test ./internal/scheduler/... -run TestName   # Single test
mage tests                                        # All unit tests
cd internal/lookoutui && yarn test                # Frontend (Vitest)
mage lintCheck                                    # Lint all Go — must pass before commit
mage lintFix                                      # Auto-fix lint
golangci-lint run ./internal/scheduler/...        # Lint specific package
cd internal/lookoutui && yarn lint                # Lint TypeScript
mage proto                                        # Regenerate after .proto changes
mage sql                                          # Regenerate after .sql query changes (sqlc)
cd internal/lookoutui && yarn openapi             # Regenerate after Swagger changes
```

### Integration Tests

```bash
testsuite test --tests ./testsuite/testcases/basic/*.yaml           # All basic e2e tests
testsuite test --tests ./testsuite/testcases/basic/submit_1x1.yaml  # Single test
```

YAML test cases in `testsuite/testcases/` (`basic/`, `performance/`, `gpu/`, `debug/`). Read an existing test case to understand the format.

## Gotchas

- **Proto source files** live in `pkg/` and `internal/`. The `proto/` directory contains vendored deps only.
- **SQL queries**: Scheduler at `internal/scheduler/database/query/query.sql`, QueryAPI at `internal/server/queryapi/database/query.sql`. Regenerate with `mage sql` — produces `db.go`, `models.go`, `query.sql.go`.
- **Config source of truth**: read the Go struct in `internal/<component>/configuration/`, not the YAML. Main file is usually `types.go` (scheduler uses `configuration.go`, scheduleringester uses `config.go` in package root).

## Local Dev

```bash
docker compose -f _local/docker-compose-deps.yaml up -d     # Redis, PostgreSQL, Pulsar
scripts/localdev-init.sh                                   # Create DBs + run migrations
goreman -f _local/procfiles/no-auth.Procfile start         # Start all components
go run cmd/armadactl/main.go                               # Use armadactl CLI to interact with Armada (default config is in ~/.armadactl.yaml)
```

Other profiles: `auth.Procfile` (adds Keycloak on :8180), `fake-executor.Procfile` (no K8s needed), `mage localdev minimal` (Kind-based, used in CI).

Examples are in `example/` folder, make sure queue is created first via `armadactl create queue` and wait 2-3 seconds for queue to propagate. After a fresh start, wait until you see the scheduler logging `starting scheduler cycle` and the executor logging `Reporting current free resource` before submitting jobs.

## Before Committing

1. `mage lintCheck` — all changed code must pass
2. `go test` changed packages **and** packages that directly import them (grep for the import path to find dependents)
3. `mage proto` if `.proto` files changed, `mage sql` if SQL queries changed
4. All commits require DCO sign-off: `git commit -s -m "message"`
5. Commit messages focus on **why**: `Fix scheduler crash when gang job has zero cardinality` not `Update scheduler.go`

## Boundaries

**Ask first:** Adding deps to `go.mod`. Changing protobuf messages or gRPC API signatures. Modifying migration files or CI/CD. Deleting/renaming exported types. Changes to scheduling fairness or preemption logic.

**Never:** Commit secrets or `.env` files. Force-push to `master`. Introduce data races — use `go test -race` on changed scheduler packages.
