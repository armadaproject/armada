# Armada local caching

## Problem Description
Armada’s API is event driven, preventing it from integrating with tools, such as Apache Airflow, written with the expectation that it can easily fetch status of a running job. It is not scalable to have Airflow subscribe to the event stream to observe status, so we must implement a caching layer which will expose a friendlier API for individual job querying.

## Proposed Change
### Notes
- Add an optional caching API and service to Armada
- Caches job_id:job_status relationship for subscribed (queue,job_set) tuples
  - Is there other information this cache might need?
- Probably written in go for performance and to reuse code from armadactl
- Must not need to run N armada cache services for N Airflow DAGs
  - Run alongside of Armada cluster
    - Upside: It just works as part of a documented deployment of armada.
    - Downside: Probably makes security/permissions something that caching has to be aware of and implementing directly, since it would be one cache for all users.
  - Run alongside Airflow cluster
    - Upside: It will already exist for airflow users
    - Downside: Probably makes security/permissions something that caching has to be aware of and implementing directly, since it would be one cache for all users.
  - Run one armada cache per airflow user (single human being or service account)
    - Upside: Armada cache would use creds from the armada user, provided by the human who needs the cache, making security essentially “free”
    - Downside: Much larger setup cost for an airflow user.

### Questions
- Should armadactl be the client used for the new armada cache service
  - I think the go grpc client should be used
- What should the new binary be named (armada-local-cache?)
  - jobservice
- Are there other Armada use cases that could benefit from this cache service, should we consider them in our design?
  - Could any of this be useful for lookout?
- Do we need armada client libraries to all support caching apis as well?
  - Probably best to support them in all Armada client libs, but we need to figure out the API first

### Proposed Airflow Operator flow
1. Create the job_set
2. Sub the armada cache to the job_set:queue tuple needed
3. [do the work to schedule the job]
4. Status polling loop that talks to armada cache
5. Maybe unsubscribe?
   - If we do this, we’d need to reference count subscriptions so one DAG would not unsubscribe from the data other DAGs need.

## Alternative Options

### Change Armada API
Armada could expose a direct endpoint allowing access to status of a running job.
A previous iteration of Armada did provide an endpoint to get status of a running job.  This was found to be a bottleneck for scaling to large number of jobs and/or users.  The switch to an event API was used to alleviate this performance issue.

### Change Airflow DAG API
Airflow could be modified to allow alternate forms of integration which work better with event-based systems.
This is impractical because we do not have Airflow contributors on staff, and the timeline required to get such a change proposed, approved, and merged upstream is much too long and includes lots of risk.
 
## Data Model
- Preference is to not have to run an additional service, but will have to store at least minimal data persistently for recovery after service restart.
  - Redis is likely ideal for this case

## API (impact/changes?)
- What should be the API between Armada <-> Armada cache?
```
message JobServiceRequest {
    string job_id = 1;
    string job_set_id = 2;
    string queue = 3;
}

message JobServiceResponse {
    string state = 1;
    string error = 2;
}

service JobService {
    rpc GetJobStatus (JobServiceRequest) returns (JobServiceResponse) {
    }
}
```
- What should be the API between Armada cache <-> Airflow?
  - The proto file above will generate a python client where they can call get_job_status with job_id, job_set_id and queue specified.  All of these are known by the Airflow Operator.
  - [API Definition](https://github.com/G-Research/armada/blob/master/pkg/api/jobservice/jobservice.proto)
- Need some kind of subscription ability; where we pass a job set id + queue to tell the cache to start caching those events.
  - I don't think we should include an API to start subscription.  I think it should be forced.
  - JobSet subscription will happen automatically for all tasks in a dag.   
- Does Armada’s existing API need to be modified or added to at all?
  - No.

## Security Impact
We must ensure that the Armada cache is implemented in such a way that it does not cross permissions boundaries – we should validate with testing that it’s impossible to get the status to jobs that you don’t have permissions for.

## Documentation Impact
- Update dev and quickstart guides
- Update production deployment guides

## Use Cases
A couple of example ways a local Armada cache might be used in context of an Airflow DAG.

### Simple use case, may not scale
1. Spin up a sidecar container containing armada cache using kubernetes operator
2. Run armada jobs using armada operator
3. Kill the sidecar container when job is complete

### Advanced use case, more scalable
1. Spin up container outside of airflow, save your host:port info for later
2. For each DAG you need to run, ensure it has the host:port to the externally managed armada cache container
3. When you are completed running all airflow jobs, terminate the container

### Implementation Plan

I have a PR that implements this [plan](https://github.com/G-Research/armada/pull/1122).
- Created a jobservice proto definition
- Generated GRPC service for the correspond proto definition
- Created a jobservice cmd that takes an configuration object
- JobService starts a GRPC server
- Added ApiConnection and GRPC configuration parameters
- Implemented the GetJobStatus rpc call that reaches out to events service to get status of job. This is not exactly what we want.
- Reports Error on failed jobs where Events proto has a Reason field

### Subscription

After talking with Chris Martin, I found out that we will be implementing our own redis cache for the events.  We will not be using pulsar, nats, or jetstream to stream events.

The logic for this service should be as follows:

- When a request comes in, check if we already have a subscription to that jobset.
If we don't have a subscription, create one using the armada go client (grpc api).
- Have a function connected to this subscription that updates the jobId key in the local cache with the new state for all jobs in the jobset (even those nobody has asked for yet).  
- The local redis should just store jobId -> state mappings.  Any messages you get that don't correspond to states we care about (ingresses, unableToSchedule) just ignore.
- Return the latest state.  If we just subscribed then it's probably "not found"
The armada operator just polls for the job state. The first poll for a given jobset will cause a subscription to be made for that jobset.

### Open questions:

1) How often do we delete data?
   - One suggestion:  if nobody has asked for a status on any job in the jobset for x mins (x=10?)
2) What do we need if the cache doesn't exist yet?
   - One suggestion: We make the operator tolerant to wait for the subscribe to "catch up"
   - Another suggestion: We fallback to GetJobSetEvents directly without the cache 

3) Where should we deploy this cache?  Airflow deployment or Armada?

4) Should we limit scope to Airflow or should this be general for any user of Armada?

5) What are the security implications of this cache?