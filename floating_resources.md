# Floating Resources

Floating resources are designed to constrain the usage of resources that are not tied to nodes. For example, if you have a fileserver outside your Kubernetes clusters, you may want to limit how many connections to the fileserver can exist at once. In that case you would add config like the below (this goes under the `scheduling` section of the Armada scheduler config).

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
When submitting a job, floating resources are specified in the same way as normal Kubernetes resources such as `cpu`. For example if a job needs 3 cpu cores and opens 10 connections to the fileserver, the job should specify
```
    resources:
      requests:
        cpu: "3"
        fileserver-connections: "10"
      limits:
        cpu: "3"
        fileserver-connections: "10"
```
The `requests` section is used for scheduling. For floating resources, the `limits` section is not enforced by Armada (this it not possible in the general case). Instead the workload must be trusted to respect its limit.

If the jobs submitted to Armada request more of a floating resource than is available, they queue just as if they had exceeded the amount available of a standard Kubernetes resource (e.g. `cpu`). Floating resources generally behave like standard Kubernetes resources. They use the same code for queue ordering, pre-emption, etc.
