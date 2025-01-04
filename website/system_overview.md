# System overview

At a high level, Armada is a multi-Kubernetes cluster batch job scheduler. Each Armada job represents a computational task to be carried out over a finite amount of time and consists of

- a Kubernetes pod,
- zero or more auxiliary Kubernetes objects (e.g., services and ingresses), and
- metadata specific to Armada (e.g., the name of the queue to which job belongs).

Jobs are submitted to Armada and Armada is responsible for determining when and where each job should be run.

An Armada deployments consists of:
- One or more Kubernetes clusters containing nodes to be used for running jobs. Such clusters are referred to as worker clusters.
- The Armada control plane components responsible for job submission and scheduling. Jobs are submitted to the control plane and are automatically transferred to worker clusters once scheduled.
- The Armada executors responsible for communication between the Armada control plane and the [Kubernetes control plane](https://kubernetes.io/docs/concepts/overview/kubernetes-api/). There is one executor per worker cluster.

The Armada control plane components may run anywhere from which they can be reached by the executors, e.g., in the same cluster as an executor, on a different Kubernetes cluster, or on a node not managed by Kubernetes. Each executor normally runs within the worker cluster it is responsible for.

We show an overview of an Armada deployment with three worker clusters below. Clients here refers to systems submitting jobs to Armada.

```
                                                    Worker clusters                              
                                                                                                 
                    ┌──────────────────────┐        ┌──────────┐ ┌──────────┐ ┌──────────┐       
┌──────────┐        │                      │        │┼────────┼│ │┼────────┼│ │┼────────┼│       
│ Clients  │ ─────► │ Armada control plane │ ─────► ││Executor││ ││Executor││ ││Executor││       
│          │        │                      │        │┼────────┼│ │┼────────┼│ │┼────────┼│       
└──────────┘        │                      │        │┼───▼────┼│ │┼───▼────┼│ │┼───▼────┼│       
                    │                      │        ││kube-api││ ││kube-api││ ││kube-api││       
                    │                      │        │┼────────┼│ │┼────────┼│ │┼────────┼│       
                    └──────────────────────┘        └──────────┘ └──────────┘ └──────────┘       
```

The arrows in the above diagram show the path jobs take though the system. Specifically, each job passes through the following stages:

1. The job is submitted to Armada, where it is stored in one of several queues, e.g., corresponding to different users. At this point, the job is stored in the Armada control plane and does not exist in any Kubernetes cluster. The job is in the *queued* state.
2. The Armada scheduler eventually determines that the job should be run and marks the job with the Kubernetes cluster and node it should be run on. The job is in the *leased* state.
3. The Kubernetes resources that make up the job are created in the cluster it was assigned to via kube-api calls. The pod contained in the job is bound to the node it was assigned to. The job is in the "pending" state.
4. Container images are pulled, volumes and secrets mounted, and init containers run, after which the main containers of the pod start running. The job is in the *running* state.
5. The job terminates once all containers in the pod have stopped running. Depending on the exit codes of the containers, the job transitions to either the *succeeded* or *failed* state.

All Kubernetes resources associated with the job are deleted once the job terminates. For more information on jobs, see the scheduler documentation.

## The Armada control plane

The Armada control plane is the set of components responsible for job submission and scheduling. It consists of the following subsystems:

- Job submission and control. Exposes an API through which clients can submit, re-prioritise, and cancel jobs.
- Job state querying. Allows clients to query the current state of a job.
- Job state querying (streaming). Allows clients to subscribe to a stream of updates to a set of jobs.
- Scheduler. Responsible for assigning jobs to clusters and nodes.
- Lookout. Web UI showing the current state of jobs.

The services that make up these subsystems communicate according to event-sourcing principles, i.e., message routing is handled by a log-based message broker shared by all services. In particular, Armada relies on Apache Pulsar for message routing. Each subsystem then operates in cycles consisting of:

1. Receive messages from Pulsar.
2. Do either or both of:
    - Update the internal state of the subsystem.
    - Publish messages to Pulsar.

Those published messages may then be received by other subsystems, causing them to publish other messages, and so on. Note that the log is the source of truth; each subsystem can recover its internal state by replaying messages from the log. We include a diagram showing this process below with two subsystems.

```
┌────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                    │
│   ┌───────────────────────────────────────┐        ┌───────────────────────────┐   │
│   │ Pulsar                                │        │ Subsystem 1               │   │
└►  │ ┌───────────────────────────────────┐ │ ─────► │                           │ ──┘
    │ │ Multiple topics/topic partitions. │ │        └───────────────────────────┘    
    │ │                                   │ │                                         
    │ │                                   │ │        ┌───────────────────────────┐    
┌►  │ └───────────────────────────────────┘ │ ─────► │ Subsystem 2               │ ──┐
│   │                                       │        │                           │   │
│   └───────────────────────────────────────┘        └───────────────────────────┘   │
│                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────┘
```

See [a note on consistency](./consistency.md) for some more detail on how Armada ensures consistency between components.
