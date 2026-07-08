# _local

Everything the local dev stack uses. `mage dev:up <profiles>` wires these pieces together. For how to use the stack (profiles, procfiles, service ports, authentication, debugging), see the [developer guide](../docs/developer_guide.md).

| Path                      | Purpose                                                                                                                                                   |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------|
| `<component>/config.yaml` | Per-component config for host-run processes (server, scheduler, scheduleringester, eventingester, executor, fakeexecutor, lookout, lookoutingester, lookouthc, lookouthcingester, binoculars) |
| `compose/stack.yaml`      | Dependency containers: redis, postgres, pulsar, plus keycloak (`auth` profile) and prometheus (`prometheus` profile)                                      |
| `compose/full.yaml`       | Fully containerized Armada stack, used by `mage dev:full` and CI                                                                                          |
| `compose/postgres-init.sql` | Creates the scheduler and lookout databases when the postgres container first initialises                                                               |
| `procfiles/`              | Goreman procfiles: `no-auth`, `auth`, `fake-executor`, each with a `-dap` (Delve debug) variant                                                           |
| `scripts/`                | `init.sh` (migrations + priority classes) and helpers for port conflicts, dlv readiness, and pre-building components                                      |
| `kind/`                   | Kind cluster config for running a real executor locally                                                                                                  |
| `keycloak/`               | Keycloak realm import for the `auth` profile                                                                                                             |
| `airflow/`                | Separate docker compose ecosystem for the Airflow operator e2e tests                                                                                     |
| `prometheus.yml`          | Scrape config for the `prometheus` compose profile                                                                                                       |
| `.armadactl.yaml`         | Example armadactl config with contexts for all supported auth flows                                                                                      |
| `readiness-job.yaml`      | Smoke-test job CI submits to verify Armada is accepting jobs                                                                                              |
