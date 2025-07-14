# Use of pprof

- Go provides a profiling tool called pprof. It's documented at https://pkg.go.dev/net/http/pprof.
- To use pprof with Armada, enable the profiling socket with the following config (this should be under `applicationConfig` if using the helm charts). This config will listen on port `6060` with no auth.
  ```
  profiling:
    port: 6060
    hostnames:
    - "armada-scheduler-profiling.armada.my-k8s-cluster.com"
    clusterIssuer: "k8s-cluster-issuer"  # CertManager cluster-issuer
    auth:
      anonymousAuth: true
      permissionGroupMapping:
        pprof: ["everyone"]
  ```
- It's possible to put pprof behind auth, see [api.md#authentication](./api.md#authentication) and [oidc.md](./oidc.md).
- For the Armada scheduler, the helm chart will make a service and ingress for every pod. These are named `armada-scheduler-0-profiling` etc.
- For other Armada components, the helm chart will make a single service and ingress called `armada-<component name>-profiling`. Note calls to these may not consistently go to the same pod. Use `kubectl port-forward`, or scale the deployment to size 1, if you need to consistently target one pod.
