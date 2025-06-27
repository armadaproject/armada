# Floating resources
- [Floating resources](#floating-resources)
  - [Specifying floating resources](#specifying-floating-resources)
  - [Limits on floating resources](#limits-on-floating-resources)

Floating resources are designed to constrain the usage of resources that are not tied to nodes. For example, if you have a fileserver outside your Kubernetes clusters, you may want to limit how many connections to the fileserver can exist at once. In that case, you would add config like the following.

```
      floatingResources:
        - name: fileserver-connections
          resolution: "1"
          pools:
            - name: cpu
              quantity: 1000
            - name: gpu
              quantity: 500
```

**Note:** The `floatingResources` content goes under the `scheduling` section of the Armada scheduler config.

## Specifying floating resources

When submitting a job, you specify floating resources in the same way as normal Kubernetes resources such as `cpu`. For example, if a job needs 3 `cpu` cores and opens 10 connections to the fileserver, the job should specify the following:

```
    resources:
      requests:
        cpu: "3"
        fileserver-connections: "10"
      limits:
        cpu: "3"
        fileserver-connections: "10"
```

## Limits on floating resources

The `requests` section is used for scheduling. For floating resources, the `limits` section is not enforced by Armada (this is not possible in the general case). Instead, the workload must be trusted to respect its limit.

If the jobs submitted to Armada request more of a floating resource than is available, they queue just as if they had exceeded the amount available for a standard Kubernetes resource (for example, `cpu`). Floating resources generally behave like standard Kubernetes resources. They use the same code for queue ordering, pre-emption and so on.
