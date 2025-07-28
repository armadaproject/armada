# Clickhouse Proposal For Lookout

Here we propose a design for backing Lookout with Clickhouse. This is desirable because we currently back Lookout with 
Postgres which a) struggles to insert effectively under extreme load, b) Cannot effectively run queries when we lack a suitable index.  In 
theory Clickhouse solves both of these issues as it supports very high insertion rates and allows relatively quick OLAP 
style queries even when a suitable index does not exist.

# The Problem

Armada is event-driven with a job generating many events over the course of its lifecycle. 
Below we can look at simplified list of the events for a typical job
```
// Initial job submission
jobSubmitted{
    "jobId:      "job-1"
    "queue":     "my-queue"
    "priority":  "1"
    "submitted": "2025-07-28T14:59:00"
}

// Job is Reprioritised
jobSubmitted{
    "jobId:   "job-1"
    "priority": "2"
}

// Job starts Runnning
jobRunning{
    "jobId: "job-1"
    "node": "my-node"
    "runStart": "2025-07-28T15:00:00"
}

// Job Succeeds
jobSucceeded{
    "jobId: "job-1"
    "runEnd": "2025-07-28T15:05:00"
}
```
Note that these events are sparse (i.e. each event does not contain the full set of fields) and thus to understand the current state of a given job, these events must be merged together. 
For example in the above job, we would expect the final state to be as follows:

```
jobState{
    "jobId:      "job-1"
    "queue":     "my-queue"
    "priority":  "2"
    "submitted": "2025-07-28T14:59:00"
    "node":      "my-node"
    "runStart":  "2025-07-28T15:00:00"
    "runEnd":    "2025-07-28T15:05:00"
}
```
In order to back Lookout we must reproduce this state for every job.  This is currently done in Postgres,
but Clickhouse may be a good fit here 

# Design Constraints

Below are the performance properties that we really want this system to have:
- Supports 2 billion historic jobs
- Supports a sustained event rate of 20-50k per second
- Supports a peak event rate of 200k a second; this would mean that if e.g. someone cancelled a million jobs, it would take 5 seconds for these events to be processed.

From the literature and from brief testing, Clickhouse seems a good fit for the above requirements.

Additionally, we need to think about the sort of queries that users want to see in Lookout. By far the most common
queries here are:
```
-- See details for a given job
SELECT * FROM jobs WHERE jobId = 'foo'

-- Show jobs from all queues ordered by most recent update time in a paginated way
SELECT * FROM jobs ORDER BY last_update_time DESC LIMIT 500;

-- Show jobs from a specific queue ordered by most recent update time in a paginated way
SELECT * FROM jobs WHERE QUEUE = 'foo' ORDER BY last_update_time DESC LIMIT 500;
```

Importantly here we need to show users a coherent view of their jobs which thus gives us some considerations around
how clickhouse might show unmerged results.  As a general guide- if a user has submitted a single jobs:
- Showing them multiple jobs all with the same job id but with different merge states would be very confusing.
- Showing them a single row representing an unmerged state would be confusing.
- Showing them a stale view, specifically the last fully merged row for the job, is probably fine so long as rows are merged in relatively short order.
  - If data is under 5 seconds stale this is amazing
  - If data is under 20 seconds stale this acceptable
  - If data is under 40 seconds stale this will be confusing- e.g. a user will reprioritise a job but won't see that for 40 seconds
  - If data is more than a minute stale this is probably unacceptable.

# Design Proposal

In order to store the jobs, we propose a single table like so:
```
	CREATE TABLE  IF NOT EXISTS jobs(
		job_id FixedString(26),
		last_transition_time SimpleAggregateFunction(anyLast, DateTime),
		queue SimpleAggregateFunction(any, String),
		namespace SimpleAggregateFunction(any, String),
		job_set SimpleAggregateFunction(any, String),
		cpu SimpleAggregateFunction(any, Int64),
		memory SimpleAggregateFunction(any, Int64),
		ephemeral_storage SimpleAggregateFunction(any, Int64),
		gpu SimpleAggregateFunction(any, Int64),
		priority  SimpleAggregateFunction(anyLast, Int64),
		submitted SimpleAggregateFunction(any, DateTime),
		priority_class SimpleAggregateFunction(any, String),
		annotations SimpleAggregateFunction(any, Map(String, String)),
		job_state SimpleAggregateFunction(anyLast,Int32),
		cancelled SimpleAggregateFunction(anyLast, Nullable(DateTime)),
		cancel_reason SimpleAggregateFunction(anyLast, Nullable(String)),
		cancel_user SimpleAggregateFunction(anyLast, Nullable(String)),
		latest_run_id SimpleAggregateFunction(anyLast, Nullable(String)),
		run_cluster SimpleAggregateFunction(anyLast, Nullable(String)),
		run_exit_code SimpleAggregateFunction(anyLast, Nullable(Int32)),
		run_finished SimpleAggregateFunction(anyLast, Nullable(DateTime)),
		run_state SimpleAggregateFunction(anyLast, Nullable(Int32)),
		run_node SimpleAggregateFunction(anyLast, Nullable(String)),
		run_leased SimpleAggregateFunction(anyLast, Nullable(DateTime)),
		run_pending SimpleAggregateFunction(anyLast, Nullable(DateTime)),
		run_started SimpleAggregateFunction(anyLast, Nullable(DateTime)),
		merged SimpleAggregateFunction(anyLast, Nullable(Bool))
	)
		ENGINE = AggregatingMergeTree()
		ORDER BY (job_id)
        SETTINGS deduplicate_merge_projection_mode = 'drop';
```
This is a relatively straightforward Clickhouse table that stores all the state that we want to accumulate on a job. In
general each column is one of two types:
- Present on the initial submission message and not on any subsequent messages.  In this case we define it as non-nullable and use any (which takes any non-null value) to merge.
- Not present on the initial submission message but added by subsequent messages. In this case we define as nullable and use anyLast (which takes the last non-null value).

The next question becomes how we can filter out unmerged rows as they are likely to be confusing for users.  Initial testing 
has show that using "final" on queries is not feasibly due to the extra cost of the query.  Instead we propose to add an extra column to the table:
```
merged SimpleAggregateFunction(anyLast, Nullable(Bool))
```
This will be set to true on the first insert but will be missing from all subsequent messages. To consider how this helps, lets consider the simplified example 
from above with the addition of this merged column.
```
// Initial job submission
jobSubmitted{
    "jobId:      "job-1"
    "queue":     "my-queue"
    "priority":  "1"
    "submitted": "2025-07-28T14:59:00"
    "merged": "true"
}

// Job is Reprioritised
jobSubmitted{
    "jobId:   "job-1"
    "priority": "2"
}
```
Now let's assume we've written these two events to clickhouse but a merge hasn't happened yet.  In that case we'll get:
```
-- Select the jobs without any tricks
SELECT jobId, queue, priority, merged FROM jobs WHERE jobId = 'job-1';
| jobId | queue     | priority | merged |
|-------|----------|----------|--------|
| job-1 | my-queue | 1        | true   |
| job-1 | *(NULL)* | 2        | *(NULL)*|


-- Use final to force an in-memory merge
SELECT jobId, queue, priority, merged FROM jobs FINAL WHERE jobId = 'job-1';
| jobId | queue     | priority | merged |
|-------|----------|----------|--------|
| job-1 | my-queue | 2        | true   |

-- Select only the jobs that have been merrged
SELECT jobId, queue, priority, merged FROM jobs WHERE jobId = 'job-1' AND merged = 'true';
| jobId | queue     | priority | merged |
|-------|----------|----------|--------|
| job-1 | my-queue | 1        | true   |
```
Of these:
- The first query is no good.  Users would get confused seeing this!
- The second query is perfect but the use of `final` makes it take too long
- The third query shows us a consistent albeit stale row

Of these filtering to just merged jobs is acceptable if we know that the merge will happen in a relatively timely fashion. For example
if we could guaretee that a merge would happen within 5 seocnds of an unpdate, then five seconds later all three queries would return the same result:
```
-- Select the jobs without any tricks
SELECT jobId, queue, priority, merged FROM jobs WHERE jobId = 'job-1';
| jobId | queue     | priority | merged |
|-------|----------|----------|--------|
| job-1 | my-queue | 2        | true   |


-- Use final to force an in-memory merge
SELECT jobId, queue, priority, merged FROM jobs FINAL WHERE jobId = 'job-1';
| jobId | queue     | priority | merged |
|-------|----------|----------|--------|
| job-1 | my-queue | 2        | true   |

-- Select only the jobs that have been merrged
SELECT jobId, queue, priority, merged FROM jobs WHERE jobId = 'job-1' AND merged = 'true';
| jobId | queue     | priority | merged |
|-------|----------|----------|--------|
| job-1 | my-queue | 2        | true   |
```
The advantage of filtering only to merged rows here would be that although users would see data that was potentially 5 seconds out of date, they would never receive
unmerged duplicate rows and thus become confused.

In order to make our desired queries quick- we would need to add projections to the table as follows:
```
-- Fast lookup by jobid filtering to merged jobs only
`ALTER TABLE jobs
		ADD PROJECTION merge_jobid_lookup (
		   SELECT *
		   ORDER BY (merged, job_id)
		)`,
		
-- Fast ordering by last_transition_time filtering to merged jobs only		
`ALTER TABLE jobs
		ADD PROJECTION merge_ last_transition_time_lookup (
		   SELECT *
		   ORDER BY (merged, last_transition_time, job_id)
		)`,				
		
-- Fast lookup by queue filtering to merged jobs only		
`ALTER TABLE jobs
		ADD PROJECTION merge_queue_lookup (
		   SELECT *
		   ORDER BY (merged, queue, last_transition_time, job_id)
		)`,		
```

# Outstanding Questions

The main set of outstanding questions here are:

- Given our performance requirements- is it feasible to get Clickhouse to ensure we're merging every 5-30 seconds?  And if we do what does this mean for both read and write performance?
- Will the projections yield the expected good query performance
- What does `SETTINGS deduplicate_merge_projection_mode = 'drop';` mean for the projections?  Can we get into a state where our projections permanently diverge from our main table due because clickhouse has dropped the "wrong" row
