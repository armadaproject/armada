# Armada local caching

## Problem Description
Armada’s API is event driven, preventing it from integrating with tools, such as Apache Airflow, written with the expectation that it can easily fetch status of a running job. It is not scalable to have Airflow subscribe to the event stream to observe status, so we must implement a caching layer which will expose a friendlier API for individual job querying.

## Proposed Change
### Notes
- Add an optional caching API and service to Armada
- Caches job_id:(job_status, message) relationship for subscribed (queue,job_set) tuples
- Service is written in Go for performance and to reuse code from armadactl

### Proposed Airflow Operator flow
1. Create the job_set
2. [do the work to schedule the job]
3. Status polling loop that talks to job service

## Alternative Options

### Change Armada API
Armada could expose a direct endpoint allowing access to status of a running job.
A previous iteration of Armada did provide an endpoint to get status of a running job.  This was found to be a bottleneck for scaling to large number of jobs and/or users.  The switch to an event API was used to alleviate this performance issue.

### Change Airflow DAG API
Airflow could be modified to allow alternate forms of integration which work better with event-based systems.
This is impractical because we do not have Airflow contributors on staff, and the timeline required to get such a change proposed, approved, and merged upstream is much too long and includes lots of risk.
 
## Data Access
- Service will need to insert job-id and state for a given queue and job-set
- Service will need to delete all jobs for a given queue and job-set.
- Service will access by job-id for polling loop.
## Data Model
 - The Job Service will contain a table of queue, job-set, job-id and latest state.
 - What database should store this?
### Redis
Pros
  - Fast access
  - Lightweight
  - Scalable
  - Easy to deploy in Kubernetes
  - Cleanup is simple (Time To Live Configurable values)

Cons
  - Persistence is tricky
  - Accessing all jobs in a given job-set is not ideal
  - Querying more than a key is not performant.

### SQLLite
Pros
  - Lightweight
  - In memory db
  - Part of service
  - Persists database to a file
  - SQL operations for inserting and deleting are simple.

Cons
  - Writing is single threaded.
  - Meant for small amount of concurrent users.
  - Difficult to scale with Kubernetes.
  - Scaling is only possible by increasing the number of job services
  - Logic for deleting is more complicated.

### Postgres

Pros
  - Supports wide range of SQL operations
  - Large amount of concurrent users
  - Deployed alongside service
  - Kubernetes can support via Replicas etc

Cons
  - Requires a separate deployment and operational support
  - TBD

[Pros/Cons of Relation Databases](https://devathon.com/blog/mysql-vs-postgresql-vs-sqlite/) is a good resource for seeing the Pros/Cons of SQLLite, PostGres and MySQL.

## API (impact/changes?)
- What should be the API between Armada cache <-> Airflow?
  - The proto file above will generate a python client where they can call get_job_status with job_id, job_set_id and queue specified.  All of these are known by the Airflow Operator.
  - [API Definition](https://github.com/G-Research/armada/blob/master/pkg/api/jobservice/jobservice.proto)
- JobSet subscription will happen automatically for all tasks in a dag.   

## Security Impact
We must ensure that the Armada cache is implemented in such a way that it does not cross permissions boundaries – we should validate with testing that it’s impossible to get the status to jobs that you don’t have permissions for.

## Documentation Impact
- Update dev and quickstart guides
- Update production deployment guides

## Use Cases

### Airflow Operator
1) User creates a dag
2) Dag setup includes ArmadaPythonClient and JobServiceClient
3) Airflow operator takes both ArmadaPythonClient and JobServiceClient
4) Airflow operator submits job via ArmadaPythonClient
5) Airflow operator polls JobServiceClient via GetJobStatus 
6) Once Armada has a terminal event, the airflow task is complete.

### Implementation Plan

I have a PR that implements this [plan](https://github.com/G-Research/armada/pull/1122).
- Created a jobservice proto definition
- Generated GRPC service for the correspond proto definition
- Created a jobservice cmd that takes an configuration object
- JobService starts a GRPC server
- Added ApiConnection and GRPC configuration parameters


### Subscription

After talking with Chris Martin, I found out that we will be implementing our own redis cache for the events.  We will not be using pulsar, nats, or jetstream to stream events.

The logic for this service should be as follows:

- When a request comes in, check if we already have a subscription to that jobset.
If we don't have a subscription, create one using the armada go client (grpc api).
- Have a function connected to this subscription that updates the jobId key in the local cache with the new state for all jobs in the jobset (even those nobody has asked for yet).  
- The local redis should just store jobId -> state mappings.  Any messages you get that don't correspond to states we care about (ingresses, unableToSchedule) just ignore.
- Return the latest state.  If we just subscribed then it's probably "not found"
The armada operator just polls for the job state. The first poll for a given jobset will cause a subscription to be made for that jobset.

### Airflow Sequence Diagram

![AirflowSequence](./airflow-sequence.svg)


### JobService Server Diagram

![JobService](./job-service.svg)

- EventClient is the GRPC public GRPC client for watching events

- The JobService deployment consists of a GRPC Go Server and a database.

### Open questions:

1) How often do we delete data?
   - One suggestion:  if nobody has asked for a status on any job in the jobset for x mins (x=10?)
   - With Redis, we can set up a TTL for each key.
   - With Relation DB, we can delete the job-set after all jobs are complete
2) What do we need if the cache doesn't exist yet?
   - One suggestion: We make the operator tolerant to wait for the subscribe to "catch up"
   - Another suggestion: We fallback to GetJobSetEvents directly without the cache 

3) Where should we deploy this cache?  Airflow deployment or Armada?
  - General consensus is to deploy alongside Airflow

4) What are the security implications of this cache?
  - For V1, our thoughts are to ignore this.
  - The cache will contain ID, State and potential error message for a job-set.
  - Airflow does not support multitenancy.

5) What database should we use?