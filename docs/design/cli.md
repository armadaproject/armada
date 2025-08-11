# Armadatctl Notes

## Design Goals
* Multiple output formats supported- table, csv, json, yaml
* Everything backed by api
* Follows `kubectl` conventions e.g `armadactl <verb> <resource> <options>`
* Armada native way of getting job related information i.e. `armadactl logs <job_id>` not `kubectl logs <pod_id>`
* At least as much functionality as the Slurm cli.

## Comparison With Slurm

| Command      | Description |
|--------------|-------------|
| **sbatch**   | Submit a batch script for later execution in the Slurm scheduler|
| **srun**     | Launch parallel tasks or run interactively inside an allocation (may allocate if needed)|
| **salloc**   | Obtain an interactive job allocation (set of nodes/CPUs)|
| **squeue**   | Display jobs in the queue (pending, running) with optional filtering by user, state, partition|
| **scancel**  | Cancel or signal jobs/job‑steps (by job ID, user, etc.)|
| **sacct**    | Show accounting data for jobs and job‑steps (past or current), customizable output via `--format`|
| **sstat**    | Query runtime statistics of current/ongoing job steps (e.g. CPU, memory usage)|
| **sshare**   | Display Fair Share resource usage for accounts/projects|
| **sinfo**    | Show status of nodes and partitions (availability, features, memory, CPUs)|
| **scontrol** | View/modify Slurm state and configuration: jobs, steps, nodes, partitions, reservations, etc. (Admin use)|
| **sreport**  | Generate aggregated reports from accounting data (cluster usage, utilization, etc.)|
| **sacctmgr** | Manage Slurm accounts and associations (administrative use)|
| **sdiag**    | Diagnostic tool for scheduling issues analysis|


### sbatch

This allows a user to submit one or more jobs.  Conceptually this is very similar to `armadactl submit`.  The main functionality missing here from Armada is:
- Slurm supports various options to help with distributing tasks across nodes and assigning resources per task.  This is somewhat coupled to MPI. Examples here include the `--nodes`, `--ntasks`, `--ntasks-per-node` and `--cpus-per-task` options.  Armada support most idirectly of this, although you are generally abstracted away from nodes and would need a large amount of scripting to achieve it.
- Slurm supports requesting exclusive access to a node via `--exclusive`
- Slurm supports a `--begin` to ensure that the job is not started before a given time.
- Slurm supports `--requeue` and `--no-requeue` to control whether jobs are requeued after failure or preemption.
- Slurm supports `--dependency` to run only after another job has completed.
- Slurm supports `--array` such that a single job can be run as n replicas each only differing by an array index. `--array-task-max=N` can be used to limit the maximum concurrent number of array jobs running.
- Slurm supports a `--profile` option that allows job level statistics (cpu, memory, network) to be collected.  Our metrics are very similar here but we do no expose this via the cli.

# Notes on Gang jobs
 You'd submit a gang job like so:
```
sbatch --ntasks=16 --nodes=4 --cpus-per-task=2 mpi_job.sh
```
 This tells slurm to reserve 4 nodes ad start 16 tasks, each with 2 cpus per task.  Jobs will only be scheduled when all 4 nodes are available.

Unlike Armada slurm returns with a single Job Id:
```
Submitted batch job 12345
```
You can then use the slurm cli to show either details about the job or the details about the individual tasks that make up the job.

### srun
This is a concept alien to Armada.  Essentially Slurm separates out the reservation of resources from the allocation of work.  You can use Sbatch or SAlloc to effectively allocate
yourself some workers as defined in number of nodes, number of workers, resources per worker) and then submit tasks to these workers using `srun`
```
salloc --nodes=4 --ntasks=16 --cpus-per-task=2
srun preprocess
srun train
exit
```

For example here the salloc step has allocated 4 nodes which will run 16 tasks each with 2 cpus.
The two `srun` commands will execute the `preprocess` and `train` commands against these workers.

For Armada you have two ways of getting most of the way there:
- Submit preprocess and train as separate jobs.  
  - These jobs both have to be scheduled independently and pay any scheduling + statup delay
  - These jobs may both run in parallel.
- Submit a single job and have a script inside the job start the individual tasks
  - A simple example would be single node only
  - A more complex example could be multi node, but would need advanced features such as limiting parallelism
  - In all cases the user would have to develop this functionality themselves.

A further use of `srun` is that you can spawn an interactive shell.  This means that you can essentially get a shell on
one of the nodes youve been allocated and then execute srun commands as you see fit until the shell ends.  This is 
somewhat like `kubectl exec` but is different because you can natively run tasks across all the nodes you have been allocated.

### squeue

This shows details about *active* jobs.  There are options to filter by state, queue, partition (we call that pool) etc and options to control which fields are outputted.

Armadactl does not currently have this functionality but everything squeue can do is essentially covered by lookout and
thus this is mainly a case of integrating Armadactl with the lookout api (or its replacement). In fact I'd say lookout
is strictly more powerful than squeue.


### scancel
This is broadly similar to `armadactl cancel job` but is a bit more pwerful as it can cancel jobs based on;
- name
- partition
- job
- reason 
- state

Of these armadactl only really supports `name (jobid)`.  

`scancel` also supports sending custom signals to jobs.  For example you could SIGTERM a job or send it a Signal to instruct it to checkpoint.


### sacct

This is broadly similar to squeue but shows details about all jobs, not just active jobs.  Again I'd say that lookout is 
strictly more powerful with one exception in that sacct can show you some basic details about your job's resource usage (cpu/memory etc)

Matching this functionality is a matter of adding aggregated jobs stats to lookout and exposing lookout functionality via armadactl

### sstat

`sstat` shows you job resource usage statistics for *live* jobs:
```
> sstat -j 12345 -o JobID,MaxRSS,AveRSS,AveCPU
JobID    MaxRSS   AveRSS   AveCPU
12345.0  1024K    900K     00:00:01
12345.1  512K     480K     00:00:00
```
This can be combined with `watch` to give you live updating metrics.

### sshare

This shows information about usage on the cluster along with fair share.  It is conceptually very similar to the data 
we store in prometheus and we could implement something very similar in `armadactl`

```
> sshare -o "Account,User,FairShare,NormUsage,NormShares"
Account   User    FairShare   NormUsage   NormShares
project1  alice   0.75        0.33        0.50
project1  bob     0.25        0.67        0.50
```

### sinfo
`sinfo` shows information about the nodes available and their states.
```
> sinfo
PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST
compute*  up     infinite   10     idle  node[01-10]
gpu       up     7-00:00:0  4      alloc node[11-14]
debug     up     2:00:00    2      down  node[15-16]
```

`armadactl` has nothing like this but we already have the much of the information available to us in the scheduler.  We
would need to expose this via an api and then via the cli.

### scontrol

scontrol does a bunch of random stuff.  It's summarized below:
### 🛠️ **`scontrol` Command Reference**

| **Object**    | **Command** | **Description** | **Example** |
|---------------|------------|-----------------|-------------|
| **Jobs**       | `show job <jobid>` | Show detailed job info | `scontrol show job 12345` |
|               | `update jobid=<id> <params>` | Update job properties (priority, time limit, etc.) | `scontrol update jobid=12345 TimeLimit=02:00:00` |
|               | `hold <jobid>` | Hold job (prevent start) | `scontrol hold 12345` |
|               | `release <jobid>` | Release held job | `scontrol release 12345` |
|               | `requeue <jobid>` | Requeue a job | `scontrol requeue 12345` |
|               | `update jobid=<id> Priority=<value>` | Change job priority | `scontrol update jobid=12345 Priority=10000` |
|               | `update jobid=<id> Dependency=<deps>` | Add/change job dependencies | `scontrol update jobid=12345 Dependency=afterok:12344` |
| **Job Steps**  | `show step <jobid.stepid>` | Show job step details | `scontrol show step 12345.0` |
|               | `update step <jobid.stepid>` | Update job step properties | `scontrol update step 12345.0 TimeLimit=01:00:00` |
| **Nodes**      | `show node <node>` | Show node details | `scontrol show node node01` |
|               | `update NodeName=<node> State=DRAIN` | Drain a node | `scontrol update NodeName=node05 State=DRAIN` |
|               | `update NodeName=<node> State=RESUME` | Resume a node | `scontrol update NodeName=node05 State=RESUME` |
|               | `update NodeName=<node> State=DOWN` | Mark node as down | `scontrol update NodeName=node05 State=DOWN` |
| **Partitions** | `show partition <name>` | Show partition details | `scontrol show partition compute` |
|               | `update PartitionName=<name> State=UP` | Activate partition | `scontrol update PartitionName=compute State=UP` |
|               | `update PartitionName=<name> State=INACTIVE` | Deactivate partition | `scontrol update PartitionName=compute State=INACTIVE` |
| **Reservations** | `show reservation` | Show active reservations | `scontrol show reservation` |
|               | `create reservation <params>` | Create a reservation | `scontrol create reservation ReservationName=team_reserv StartTime=2025-08-05T10:00:00 Duration=02:00:00 Nodes=node[10-20] Users=alice,bob` |
|               | `delete reservation ReservationName=<name>` | Delete a reservation | `scontrol delete reservation ReservationName=team_reserv` |
| **Cluster Config** | `show config` | Show Slurm configuration | `scontrol show config` |
|               | `show federation` | Show federation details | `scontrol show federation` |
| **General**    | `show <object>` | Show details of any object | `scontrol show license` |
|               | `update <object>` | Update properties of any object | `scontrol update license ...` |

`armadactl` supports almost none of this, however much of ths is given to us by K8s.  It is probably worth going through this and working out what should be implemented as first class `armadactl` commands.

### sreport

This is used by administrators to generate usage reports based on the Slurm accounting database.  I don't think we need to replicate this functionality in our cli as our
Drake/Grafana approach is largely superior.  We could consider implementing some lightweight reports in the CLI but I don't think it's a priority.

### sdiag

This is similar to the `armadactl get scheduling|queue|job-report`.  I'd actually say out reports are more powerful but
the slurm output is much easier to read and looks more professional.  I think here we want to tidy up our reports.
*******************************************************
sdiag output at Mon Aug  4 12:30:00 2025
*******************************************************
Scheduler Statistics for cluster slurm-cluster:
Last cycle:   0.002321 seconds
Mean cycle:   0.001875 seconds
Cycles per second: 530
Last queue length: 125 jobs
Mean queue length: 140 jobs
Jobs scheduled last cycle: 30
Jobs scheduled per second: 320
Backfill stats:
Last cycle backfilled: 5 jobs
Total backfilled jobs: 1200
Depth reached last cycle: 200
Depth reached mean: 185
Queue evaluation duration: 0.0009 seconds

## Proposed Commands

### Submit

This can probably stay as is: i.e.
```
armadact submit -f <file>
```

Things we might want to consider:
* Add `queue`, `--jobset` and `--namespace` parameters to override the queue, jobset and namespace values contained in the file
* Add `--watch` to watch the jobs

### Run
This is a new command that allows someone to easily run a job without having to create a job file.  The logs may be optionally streamed by --follow
```
armadactl run [NAME] --image IMAGE [COMMAND] [args...] [flags]
```
The actual options are up for debate but something like:
```
| Option            | Description                                                     |
|-------------------|-----------------------------------------------------------------|
| --queue string     | Target queue (required if no default).                          |
| --jobset string    | Job set to associate with (optional).                            |
| --namespace string | Job set to associate with (optional).                            |
| --image string     | Container image to run (required).                               |
| --cpu string       | CPU request (e.g., 500m, 4).                                     |
| --memory string    | Memory request (e.g., 1Gi, 16Gi).                                |
| --gpu string       | Number of GPUs (e.g., 1).                                        |
| --env KEY=VALUE    | Environment variables; repeatable.                               |
| --timeout duration | Maximum job runtime (e.g., 1h, 30m).                             |
| --wait             | Wait for job to finish before exiting.                           |
| --follow           | Stream logs until job completion.                                |
| --restart-policy   | Restart policy (Never \| OnFailure). Default: Never.              |
| -o, --output       | Output format (table, json, yaml, csv). Default: table.           |
| --dry-run          | Validate job submission without running it.                      |
```

Examples:
```
# Run a simple job and exit immediately
armadactl run hello --image alpine -- echo "Hello Armada"

# Run with resources and environment variables
armadactl run gpu-task \
  --image tensorflow/tensorflow:2.12-gpu \
  --gpu 1 --cpu 4 --memory 16Gi \
  --env MODE=train --env EPOCHS=10 \
  --follow
```

Ideally we'd also let people submit simple scripts:
```
# Run a job and follow its logs until it completes
armadactl run trainer --image pytorch/pytorch:2.3 --follow -- python train.py
```
The challenge here would be how to make these scripts available to the job. Supporting configmaps in Armada may be helpful for this!

### Cancel
Cancel should be implemented as a series of filters which are logical AND

```
armadactl cancel jobs [<job_id> ...] [flags]
```
| Flag       | Description                                           |
|------------|-------------------------------------------------------|
| --queue    | Cancel jobs in this queue.                            |
| --jobid    | Cancel this job id.                             |
| --jobset   | Cancel jobs in this jobset.                           |
| --pool     | Cancel jobs in this pool.                             |
| --executor | Cancel jobs leased to this executor                   |
| --state    | Cancel jobs in this state (pending, running, queued). |
| --all      | Cancel all jobs                                       |        

examples:
```
# Cancel single job
armadactl cancel --jobid 01hj9w1x0h7h3n4m6y4k1d3a7g

# Cancel multiple jobs
armadactl cancel jobs  --jobid 01hj9w1x0h7h3n4m6y4k1d3a7g --jobid 01hj9w1x0h7h3n4m6y4k1d3a9j

# Cancel all jobs in a specific queue
armadactl cancel jobs --queue foo

# Cancel all jobs in a jobset
armadactl cancel jobs --queue foo --jobset bar

# Cancel all running jobs in a pool
armadactl cancel jobs --pool gpu --state running

# Cancel jobs currently leased to a specific executor
armadactl cancel jobs --executor armada-01

# Cancel all jobs in a queue and pool, only those currently pending
armadactl cancel jobs --queue foo --pool gpu --state pending
```

One challenge here would be to warn the user if they are going to cancel large numbers of jobs.

### Reprioritize

We'll make this analogous to cancel.  Arguably it doesn't need to be as flexible, however consistency ftw.
```
armadactl reprioritize jobs <priority> [flags]
```
| Flag       | Description                                                 |
|------------|-------------------------------------------------------------|
| --jobid    | Reprioritize this job id.                                   |
| --queue    | Reprioritize jobs in this queue.                            |
| --jobset   | Reprioritize jobs in this jobset.                           |
| --pool     | Reprioritize jobs in this pool.                             |
| --executor | Reprioritize jobs leased to this executor                   |
| --state    | Reprioritize jobs in this state (pending, running, queued). |
| --all      | Reprioritize all jobs                                       |        

examples:
```
# Reprioritize single job
armadactl reprioritize jobs --jobid 01hj9w1x0h7h3n4m6y4k1d3a7g 100

# Reprioritize multiple jobs
armadactl reprioritize jobs --jobid 1hj9w1x0h7h3n4m6y4k1d3a7g 01hj9w1x0h7h3n4m6y4k1d3a9j 100

# Reprioritize all jobs in a specific queue
armadactl reprioritize jobs --queue foo 100

# Reprioritize all jobs in a jobset
armadactl reprioritize jobs --queue foo --jobset bar 100

# Reprioritize all running jobs in a pool
armadactl reprioritize jobs --pool gpu --state running 100

# Reprioritize jobs currently leased to a specific executor
armadactl reprioritize jobs --executor armada-01 100

# Reprioritize all jobs in a queue and pool, only those currently pending
armadactl reprioritize jobs --queue foo --pool gpu --state pending 100
```
### Logs
`armadactl logs` is analogous to `kubectl logs` except that it works on a jobid rather than a pod name:

```
armadactl logs <job_id> [flags]
```

| Flag        | Description                                                          |
|-------------|----------------------------------------------------------------------|
| --run       | Specific job run ID (if multiple runs exist). Default is latest runs |
| --container | Specific container name within the job (default: first container).   |
| --follow    | Stream logs until the job completes.                                 |
| --since     | Show logs newer than a relative time (e.g., 5s, 2m, 3h).             |
| --tail      | Show only the last N lines of logs.                                  |

```
# Get logs for a single job
armadactl logs 01hk3n4m0a5h7p8q6r9x1v2b3c

# Stream logs until job completion
armadactl logs 01hk3n4m0a5h7p8q6r9x1v2b3c --follow

# Get logs for a specific job run
armadactl logs 01hk3n4m0a5h7p8q6r9x1v2b3c --run 01hk3n4m0a5h7p8q6r9x1v2b3r

# Tail only last 100 lines
armadactl logs 01hk3n4m0a5h7p8q6r9x1v2b3c --tail 100

# Logs for a specific container
armadactl logs 01hk3n4m0a5h7p8q6r9x1v2b3c --container worker
```

### Describe Job 
Analagous to `kubectl describe pod`: Provides a human readable description of the job:
```
armadactl describe job <job_id>
```

We'll need to think exactly how we format this.


### Get Job
Analagous to `kubectl get pod`

```
armadactl get jobs [flags]
```

| Flag         | Description                                                         |
|--------------|---------------------------------------------------------------------|
| --queue      | Filter jobs in this queue.                                          |
| --jobset     | Filter jobs in this jobset.                                         |
| --pool       | Filter jobs in this pool.                                           |
| --executor   | Filter jobs leased to this executor.                                |
| --state      | Filter jobs by state (pending, running, queued, completed, failed). |
| --limit      | Limit the number of jobs returned.                                  |
| --since      | Show jobs submitted after a relative time (e.g., 5s, 2m, 3h).       |
| --until      | Show jobs submitted before a relative time.                         |
| -o, --output | Output format (table, json, yaml, csv). Default: table.             |
| --wide       | Show extra columns                                                  |
| --watch      | Watch for job updates and stream changes.                           |


```
# Get all jobs
armadactl get jobs

# Get all jobs in a queue
armadactl get jobs --queue foo

# Get running jobs in jobset bar
armadactl get jobs --queue foo --jobset bar --state running

# Get jobs in a pool leased to a specific executor
armadactl get jobs --pool gpu --executor armada-01

# Limit results and output as JSON
armadactl get jobs --limit 50 -o json

# Get jobs submitted in the last hour
armadactl get jobs --since 1h
```

### Get Job Counts
Allows filtered and aggregated counts of jobs similar to lookout:

```
armadactl get job-counts [flags]
```

| Flag       | Description                                                       |
|------------|-------------------------------------------------------------------|
| --queue    | Filter by queue.                                                   |
| --jobset   | Filter by jobset.                                                  |
| --pool     | Filter by pool.                                                    |
| --executor | Filter by executor.                                                |
| --state    | Filter by state (pending, running, completed, failed).             |
| --group-by | Group counts by one or more fields (queue, state, user, pool).      |

```
# Get total job counts
armadactl get job-counts

# Get counts grouped by queue
armadactl get job-counts --group-by queue

# Get counts grouped by queue and state
armadactl get job-counts --group-by queue,state

# Get counts grouped by executor for running jobs
armadactl get job-counts --state running --group-by executor
```

### Stat Job

Analagous to Slurm's `sstat` this would allow you to see performance statistics about a job
```
armadactl stat job <job_id> [<job_id> ...] [flags]
```

| Flag        | Description                                                        |
|-------------|--------------------------------------------------------------------|
| --run       | Show stats for a specific job run.                                 |
| --interval  | Refresh interval for live stats (e.g., 2s).                        |
| --follow    | Continuously stream resource usage until job completion.           |
| -o, --output| Output format (table, json, yaml, csv). Default: table.            |

###  Get error
Get the error for the job

| Flag        | Description                                                    |
|-------------|----------------------------------------------------------------|
| --run       | Get the error for a specific job run (defaults to latest run). |
| --debug     | Show debug info in addition to the main error.                 |

```
# Get the most recent error for a job
armadactl get error 01hkag9n0m6q7r8s9t0u1v2w3x

# Get the error from a specific job run
armadactl get error 01hkag9n0m6q7r8s9t0u1v2w3x --run 01hkag9n0m6q7r8s9t0u1v2w3y

# Get full debug output
armadactl get error 01hkag9n0m6q7r8s9t0u1v2w3x --debug
```

### Get job-runs

List all runs for a job
````
armadactl get job-runs <job_id>
```


### Exec
Analagous to `kubectl exec` this would allow you to exec into a job
```
armadactl exec <job_id> [-- <command> [args...]]
```
| Flag        | Description                                                        |
|-------------|--------------------------------------------------------------------|
| --container | Container name (default: first container).                         |
| --tty       | Allocate a TTY (default: true if stdin is a terminal).             |
| --stdin     | Keep stdin open (for interactive commands).                        |
| --shell     | Override default shell (`/bin/bash`, `/bin/sh`, etc).              |


# Port-forward
Analagous to `kubectl port-forward` this would allow you to port forward to a job container.
```
armadactl port-forward <job_id> <local_port>:<remote_port> [flags]
```

| Flag        | Description                                                        |
|-------------|--------------------------------------------------------------------|
| --container | Container name (default: first container).                         |
| --address   | Local address to bind (default: 127.0.0.1).                        |


# Get Resources
Give a summary of total/pool/cluster resources that armada knows about.

```
armadactl get resources [flags]
```
| Flag     | Description                                                |
|----------|------------------------------------------------------------|
| --pool   | Show resources for a specific pool.                         |
| --cluster| Show resources for a specific cluster.                      |

```
# Total cluster resources
armadactl get resources

# Resources broken down by pool
armadactl get resources --pool gpu

# Resources by cluster
armadactl get resources --cluster armada-01
```

Output should have a number of useful things such as 
* Number of nodes
* Total memory/gpu/cpu/disk
* Total number of running jobs

## Get Nodes
analagous to `kubectl get nodes` but with armada specific filters
```
armadactl get nodes [flags]
```

| Flag      | Description                                            |
|-----------|--------------------------------------------------------|
| --pool    | List nodes in a specific pool.                         |
| --cluster | List nodes in a specific cluster.                      |
| --state   | Filter by node state (ready, drained, down).           |
| --labels  | matching labels                                        |
| --wide    | Show additional info (resources, labels, annotations). |

```
# List all nodes
armadactl get nodes

# List nodes in a pool
armadactl get nodes --pool gpu

# Wide output
armadactl get nodes --pool gpu --wide
```
### Describe Node
Allows describing a node (k8s style) via either a node name or a job id

| Flag    | Description                                              |
|---------|----------------------------------------------------------|
| --jobid | Describe node that is either running or has run this job |
| -o      | Output format (table, json, yaml).                       |

```
# Describe node by name
armadactl describe node node-07

# Describe node which has been assigned a job.  If multiple runs then latest run is used.
armadactl describe node --jobid 01hk7c9n0h2m4u5v6w7x
```

### Find node-with

Allows a user to find how many nodes can match the given amount of resource.

```
armadactl find node-with --pool <pool_name> --resource <cpu|gpu|memory|disk>
```

Output could be something like:

```
MATCHING NODES: 5 available in pool gpu

EXAMPLES:
NODE        CPU(alloc/total)  MEM(alloc/total)  GPU(alloc/total)
node-11     4/16              16Gi/64Gi         1/2
node-14     4/16              32Gi/64Gi         1/2
node-17     8/16              24Gi/64Gi         1/2
```
We'd need to be careful how we deal with taints here:


### Find largest-node

Allows a user to find the largest possible node available:

```
armadactl find largest-node --pool <pool_name> --resource <cpu|gpu|memory|disk>
```

### Get job|queue|scheduling-report

These should work as they do now but we should work to make the output as clear as possible.






