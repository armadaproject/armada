# Proposal for a new job submission api

## Motivation
The current Armada Job submission API works well for traditional batch jobs, but less well other categories of job such as distributed machine learning.  Specific functionality that is missing or difficult to achieve with the current system includes:
* Scheduling mupltiple pods and treating them as as single logical job
* The concept of a job co-ordinator pod
* Components (e.g. postgres) that need to stay alive for the lifetime of the job
* Pre-emption
* Elasticity (e.g. the ability of the job to vary its resource usage as capacity allows)

The following document describes a proposed extension to the job submission api in order to meet such needs.  It is written in conjunction with the new proposed scheduler specification which can be found [here](https://github.com/G-Research/armada/pull/976/files).

## Public Interfaces

We propose modifying the `JobSubmitRequest` message used in the `SubmitJobs` RPC call to be as follows:
```
// A request to run an Armada job. Each job consists of a set of Kubernetes objects,
// one of which is the main object (typically a pod spec.) and has a priority associated with it.
// When the main object exits, all other objects are cleaned up.
// The priority, together with the queue the job is submitted to, determines the order in which jobs are run.
message JobSubmitRequestItem {

    // User-provided id used for server-side deduplication.
    // I.e., jobs submitted with the same deduplication_id as an existing job are discarded..
    string deduplication_id = 1;

    // Priority of this job. Measured relative to other jobs in the same queue.
    uint32 priority = 2;

    // Kubernetes namespace that the job wil run under
    string namespace = 3;

    // Main object that determines when an application has finished.
    KubernetesMainObject mainObject = 4;

    // Set of additional Kubernetes objects to create as part of the job.
    repeated KubernetesObject objects = 5;

    // Set of labels applied to all Kubernetes objects
    map<string, string> labels = 6;

    // Set of annotations applied to all Kubernetes objects
    map<string, string> annotations = 7;

    // Maximum lifetime of the job in seconds. Zero indicates an infinite lifetime.
    uint32 lifetime = 8;

    // If true, the job is run at most once, i.e., at most one job run will be created for it.
    // If false, the job may be re-leased on failure, which may cause the job to run more than once
    // (e.g., if a job run succeeds but the executor fails before it can report job success).
    bool atMostOnce = 9;

    // If true, Armada may preempt the job while running.
    bool preemptible = 10;

}

// Kubernetes objects that can serve as main objects for an Armada job.
message KubernetesMainObject {
    // Set of labels applied to this object
    map<string, string> labels = 1;
    // Set of annotations applied to this object
    map<string, string> annotations = 2;
    oneof object {
        k8s.io.api.core.v1.PodSpec pod_spec = 3;
        PodGroupSpec pod_group_spec = 4;
    }
}

// Kubernetes objects that can be created as part of an Armada job.
message KubernetesObject {
    // Set of labels applied to this object
    map<string, string> labels = 1;
    // Set of annotations applied to this object
    map<string, string> annotations = 2;
    oneof object {
        IngressConfig ingress = 4;
        ServiceConfig service = 5;
        k8s.io.api.core.v1.ConfigMap configMap = 6;
    }
}

message PodGroupSpec {

	// min_resources define an aggregated minimum resource requirement to run
	// a group of pods, as a fail-past path to speed up the whole scheduling.
	// (optional)
	k8s.io.api.core.v1.PodSpec min_resources = 1;

	// subsets consist of various kinds of pod sets.
	// A PodGroup is schedulable if all subsets can be scheduled.
	repeated Subset subsets = 2;

	// schedule_timeout_seconds defines the timeout threshold to abort
	// an in-progress PodGroup-level scheduling attempt.
	int32 schedule_timeout_seconds = 3;
}

// Subset represents a collection of pods with the same role.
message Subset {

	// role is used to distinguish pods with different roles in the
	// same pod group. Optional if there is only one subset in the
	// pod group.
	string role = 1;

	// MinMember specifies the minimum number of pods required for a
	// subset to be operational. When the job is initially scheduled, armada will ensure that 
	// At least this number of pods are brought up.
	// If the number of pods falls below this, the job will be killed.
    int32 min_member = 2;
    
    // ManMember specifies the max number of pods requested when using (static) elastic scaling
    // (optional)
    int32 max_member = 3;
    
    // All Pods in the pod group are identical
    k8s.io.api.core.v1.PodSpec template = 4;
}

// Existing Ingress Message
message IngressConfig {
    repeated uint32 ports = 2;
    map<string, string> annotations = 3;
    bool tls_enabled = 4;
    string cert_name = 5;
    bool use_clusterIP = 6;
}

// Existing Service Messages
message ServiceConfig {
    ServiceType type = 1;
    repeated uint32 ports = 2;
}

enum ServiceType {
    NodePort = 0;
    Headless = 1;
}

```

A summary of the main changes here are as follows:

1. The message structure itself is made as similar as possible to our [internal message representing a job submission](https://github.com/G-Research/armada/blob/master/pkg/armadaevents/events.proto#L126).  Note that it is deliberately 
not **identical** to our internal message as this allows us to change internal messages without affecting the external api.
3. Jobs are now composed of a single main object along with an arbitrary number of auxillary objects.  This distincton allows us to be sympathetic to 
frameworks that have separate cooordinator and worker processes (e.g. Dask, Spark) and should allow us to detrmine which is the "gateway" application in
terms of errors, ui etc.  The intention is that all pods specified by the job will be gang-scheduled by Armada.  
4. As the user can now specify any number of objects, it becomes possible for them to represent much more complex jobs. We do, however, restrict the 
objects that can be specified at each stage as, for example, it would make no sense for the main object to be a `ConfigMap`, nor (at this stage) do we 
think that something like a `StatefulSet` should be provisioned as part of a job.
5. In addition to the objects available on the existing api (`PodSpec`, `ConfigMap`, `IngressConfig`, `ServiceConfig`) we define a new message `PodGroup` which is
a group of pods that all have the same PodSpec.  This is inspired by similar concepts in [Volcano](https://volcano.sh/en/docs/podgroup/) and [k8s-sigs](https://github.com/kubernetes-sigs/scheduler-plugins/blob/master/pkg/coscheduling/README.md)
and allows an efficient representation of a large number of identical pods.
6. Jobs may be marked as pre-emptible, which is a pre-requisite for enabling pre-emeption in the scheduler.
7. Jobs may be marked as concurrency safe which would allow Armada to optimistically run several instances of the job concurrently if capacity allows.
8. Jobs may be marked as atMostOnce which means that Armada will not try to rerun them on a failure.
9. Jobs may define a lifetime, which a number of seconds after which the job may be pre-empted.  This enables more efficienct scheduling and the plan is to reward the user 
with extra capcity if it is set.   More details can be found in the [scheduling proposal](https://github.com/G-Research/armada/pull/976/files).

## Migration Strategy
It should be possible to add a new rpc endpoint which accepts a new JobSubmissionRequest message containing a list of  new JobSubmitRequestItems. After some period of time, we should be able to  migrate all users to the new endpoint and retire the old rpc call. 
Reprioritization and cancellation should work as before without changes.
