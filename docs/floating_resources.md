# Floating Resources

Floating resouces are designed to constrain the usage of some resource that is not assigned to nodes. For example, if you have a fileserver outside your Kubernetes cluster, you may want to limit how many jobs can open a connection to the fileserver at once. In that case you would add config like this to the armada scheduler.

```
```
When submitting a job, floating resources are specified in the same way you're specify a normal k8s resource such as `cpu`. For example if my job needs 3 cpu cores and opens 10 connections to the fileserver you could write
```
    resources:
      requests:
        fileserver-connections: "10"
        cpu: "3"
      limits:
        fileserver-connections: "10"
        cpu: "3"         
```
The `requests` section is used for scheduling. The `limits` section is not currently enforced by Armada, as this it not possible in the general case. Instead the workload must be trusted to respect its limit. 
