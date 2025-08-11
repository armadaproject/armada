# Armada API Design

## Principles
* Public by default
* All API calls available via both GRPC and OpenAPI specifications.
* Offers Equivalent in functionality to the Slurm Rest API using comparable concepts
* The public API backs both Lookout and Armadactl, there are no private APIs here.
* True multi-language support.  Official clients for:
  * Go
  * Python
  * .Net
  * Java
  * Typescript
* AuthN and AuthZ supported out of te box
* Scales to:
  * 40 million jobs per day
  * 2 million active jobs
  * 10 million queued jobs
  * 5000 concurrent users

Stretch Goal:
- The API is either slurm compatible or (more likely) it is easily possible to build a slurm compatible api on top of it.

## Proposal

### Submit

The submit endpoint should allow users to submit jobs.  Once issue here is that there are actually various types of job
that a user might want to submit.  For example:

* A simple job that just consists of a command line and an image
* A low level job where the user is going to use many K8s features (config maps, services, volumes, init containers)
* An array job where the user wants to submit many identical jobs at once
* An mpi job where the user wants to have several jobs scheduled as one logical job that acts across many nodes.

The solution here is to have specialised job types.  Below we will sketch out one possible implementation.  Note that this
is meant to be illustrative of the approach rather than a final design.

```protobuf
syntax = "proto3";

package armada.api.v1;

import "google/protobuf/duration.proto";
import "google/protobuf/timestamp.proto";

message Job {
  oneof spec {
    RawJob raw = 10;
    SimpleJob simple = 11;
    ArrayJob array = 12;
    MpiJob mpi = 13;
    TorchJob torch = 14;
    // Future types...
  }
}
```

Of these types,  RawJob give you everything you might need on a job at the cost of complexity:
```protobuf
syntax = "proto3";

package armada.api.v1;

import "k8s.io/api/core/v1/generated.proto";
import "k8s.io/api/networking/v1/generated.proto";

// Represents a low-level job with full control over Kubernetes features.
message RawJob {
  map<string, string> labels = 1;
  map<string, string> annotations = 2;
  string namespace = 3;
  string queue = 4;
  string jobset = 5;
  k8s.io.api.core.v1.PodSpec pod = 6;
  
  // All these resources are tied to the job and will share a lifetime with it.
  repeated k8s.io.api.core.v1.ConfigMap config_maps = 7;
  repeated k8s.io.api.core.v1.Service services = 8;
  repeated k8s.io.api.networking.v1.Ingress ingresses = 9;
}
```

SimpleJob can be used if you have an image and a commandLine you just want to run:
```protobuf
syntax = "proto3";

package armada.api.v1;

import "google/protobuf/duration.proto";

// A high-level job definition for simple container execution.
message SimpleJob {
  // Metadata
  map<string, string> labels = 1;
  map<string, string> annotations = 2;
  string namespace = 3;
  string queue = 4;
  string jobset = 5;
  
  // Main job properties  
  string image = 6;
  string command = 7;
  map<string, string> env = 8;
  map<string, string> resources = 9;

  // Optional Maximum runtime
  google.protobuf.Duration timeout = 10;
  
  // Optional working directory.
  string working_dir = 11;
}
```

An array job can run N identical jobs.  Armada will inject an array index env var to each job. We will set a limit
on the maximum size of the array (e.g. 100k):

```protobuf
syntax = "proto3";

package armada.api.v1;

import "google/protobuf/duration.proto";

// A definition for running N identical jobs (with optional parallelism control)
message ArrayJob {
    // Metadata
    map<string, string> labels = 1;
    map<string, string> annotations = 2;
    string namespace = 3;
    string queue = 4;
    string jobset = 5;

    // Number of tasks to run (e.g. 100 array elements)
    int32 task_count = 6;

    // Maximum number of tasks to run concurrently
    int32 max_concurrency = 7;
    
    // Optional timeout per task
    google.protobuf.Duration timeout = 9;

    // The job to run for each task.
    // NOTE: Fields like queue, namespace, and jobset in the inner job will be ignored.
    // These are taken from the outer ArrayJob metadata.
    oneof template {
        SimpleJob simple = 10;
        RawJob raw = 11;
    }
}
```

An MPI Job will allow us to run a single logical job which is actually multiple single-node jobs working together.
```protobuf
syntax = "proto3";

package armada.api.v1;

import "google/protobuf/duration.proto";
import "k8s.io/api/core/v1/generated.proto";

// A job that runs distributed MPI workloads across multiple nodes.
// Compatible with Kubeflow's MPIJob spec.
message MpiJob {
    // Metadata
    map<string, string> labels = 1;
    map<string, string> annotations = 2;
    string namespace = 3;
    string queue = 4;
    string jobset = 5;

    // Number of worker replicas (excluding the launcher)
    int32 num_workers = 6;

    // Slots per worker (typically corresponds to cores or processes per pod)
    int32 slots_per_worker = 7;

    // Pod cleanup policy (e.g., "Running", "None", "OnFailure", "Always")
    string clean_pod_policy = 8;

    // Optional job timeout
    google.protobuf.Duration timeout = 9;

    // PodSpec for the launcher pod
    k8s.io.api.core.v1.PodSpec launcher = 10;

    // PodSpec for the worker pods
    k8s.io.api.core.v1.PodSpec worker = 11;
}
```

A Torch job is a special MPI job that can run torch distributed tasks.
```protobuf
syntax = "proto3";

package armada.api.v1;

import "google/protobuf/duration.proto";
import "k8s.io/api/core/v1/generated.proto";

// A job that runs distributed PyTorch training across multiple nodes.
// Compatible with Kubeflow's TorchJob spec.
message TorchJob {
  // Metadata
  map<string, string> labels = 1;
  map<string, string> annotations = 2;
  string namespace = 3;
  string queue = 4;
  string jobset = 5;

  // Number of worker replicas (excluding the master)
  int32 num_workers = 6;

  // Pod cleanup policy (e.g., "Running", "None", "OnFailure", "Always")
  string clean_pod_policy = 7;

  // Optional job timeout
  google.protobuf.Duration timeout = 8;

  // PodSpec for the master (rank 0)
  k8s.io.api.core.v1.PodSpec master = 9;

  // PodSpec for the worker pods (rank 1+)
  k8s.io.api.core.v1.PodSpec worker = 10;
}
```

#### Job Tracking

*WARNING: the below was ai generated alongside a fair amount of prompting from me!  I don't claim that this is completely consistent, however I hope it demonstrates the general principal!* 

For some job types (ArrayJob, MpiJob, TorchJob), the user submits a **single logical job**, but Armada creates **multiple physical jobs** under the hood to execute it.

Each of these logical jobs is assigned a **top-level job ID** at submission time — this is the **only ID the user needs to track** initially.

##### Hierarchical Model

Internally, Armada models jobs hierarchically:

- The **top-level job** represents the logical job (e.g., an array of 100 tasks, or a 4-node MPI training job).
- Each **sub-job** is a regular Armada job — scheduled, tracked, and monitored individually.
- Each sub-job can have one or more **job runs** (e.g., in the case of retries or preemptions).

The relationship is as follows:

```
Top-Level Job (e.g. ArrayJob)
│
├── SubJob #0
│   └── Run A
│
├── SubJob #1
│   ├── Run A
│   └── Run B (retry)
│
└── SubJob #2
    └── Run A
```

Each sub-job has its own:
- Unique `job_id`
- `parent_job_id` pointing to the top-level job
- Independently tracked state and error information

##### State of Logical Jobs

The state of the top-level (logical) job is **derived** from the states of its sub-jobs. Armada uses a small set of **aggregated states**:

| Aggregated State | Meaning |
|------------------|---------|
| `PENDING`        | All sub-jobs are pending |
| `RUNNING`        | All sub-jobs are running |
| `SUCCEEDED`      | All sub-jobs succeeded |
| `FAILED`         | All sub-jobs failed |
| `MIXED`          | Sub-jobs are in different states |
| `CANCELLED`      | All sub-jobs cancelled |

This aggregation logic is handled by Armada's job tracker and surfaced via both the CLI and API.

##### CLI Integration

From the user’s perspective, the top-level job ID is used for everything:

```bash
# Submit an MPI job
$ armadactl submit -f mpi.yaml
Submitted job: 01hkdye2mqjdvbqxz2nbcdw0wz

# Track overall status
$ armadactl get job 01hkdye2mqjdvbqxz2nbcdw0wz

# Get details of sub-jobs and runs
$ armadactl describe job 01hkdye2mqjdvbqxz2nbcdw0wz

# Fetch logs from the launcher
$ armadactl logs 01hkdye2mqjdvbqxz2nbcdw0wz --run launcher
```

This keeps the user experience simple and consistent — even when the underlying structure is complex.

##### API Structure

In the API, both top-level and sub-jobs are represented as







