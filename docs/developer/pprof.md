# Use of pprof

- Go provies a profiling tool called pprof. It's documented at https://pkg.go.dev/net/http/pprof.
- If you wish to use this with armada, enable the profiling socket with the following config (this should be under `applicationConfig` if using the helm charts). This config will listen on the relevant port with no auth.
  ```
  profiling:
    port: 6060
    auth:
      anonymousAuth: true
      permissionGroupMapping:
        pprof: ["everyone"]
  ```
- It's possible to put pprof behind auth if you want, see [api.md#authentication](./api.md#authentication) and [oidc.md](./oidc.md).
- You can use `kubectl port-forward` to access the profiling port.
