# Using pprof

- Go provides a profiling tool called pprof. [Learn more about pprof](https://pkg.go.dev/net/http/pprof).

To use pprof with Armada, enable the profiling socket with the following config. If you're using the Helm charts, this should be available under `applicationConfig`. This config will listen on port `6060` with no auth.

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

If you want to put pprof behind auth, see our [authentication](./armada-api.md#authentication) and [OpenID Connect](./setting-up-oidc.md) documentation.

For the Armada scheduler, the Helm chart makes a service and ingress for every pod. These are named `armada-scheduler-0-profiling`, etc.

For other Armada components, the Helm chart makes a single service and ingress called `armada-<component name>-profiling`. Calls to these may not consistently go to the same pod. If you need to consistently target one pod, use `kubectl port-forward` or scale the deployment to size 1.
