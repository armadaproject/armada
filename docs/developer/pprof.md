# Use of pprof

- Go provides a profiling tool called pprof. It's documented at https://pkg.go.dev/net/http/pprof.
- To use pprof with Armada, enable the profiling socket with the following config (this should be under `applicationConfig` if using the helm charts). This config will listen on port `6060` with no auth.
  ```
  profiling:
    port: 6060
    auth:
      anonymousAuth: true
      permissionGroupMapping:
        pprof: ["everyone"]
  ```
- It's possible to put pprof behind auth, see [api.md#authentication](./api.md#authentication) and [oidc.md](./oidc.md).
- For the scheduler, the helm chart will make a service and ingress for every pod. These are named `armada-scheduler-0-profiling` etc.
- For other services, the helm charts do not currently expose the profiling port. You can use `kubectl port-forward` to access these.
