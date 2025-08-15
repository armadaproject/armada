-- +goose Up
/* ======================= *
 * jobs (summary per job)  *
 * ======================= */
CREATE TABLE IF NOT EXISTS jobs (
  job_id FixedString(26),
  queue                SimpleAggregateFunction(any, String),
  namespace            SimpleAggregateFunction(any, String),
  job_set              SimpleAggregateFunction(any, String),
  cpu                  SimpleAggregateFunction(any, Int64),
  memory               SimpleAggregateFunction(any, Int64),
  ephemeral_storage    SimpleAggregateFunction(any, Int64),
  gpu                  SimpleAggregateFunction(any, Int64),
  priority             SimpleAggregateFunction(anyLast, Nullable(Int64)),
  submit_ts            SimpleAggregateFunction(any, DateTime64(3)),
  priority_class       SimpleAggregateFunction(any, Nullable(String)),
  annotations          SimpleAggregateFunction(any, Map(String, String)),
  job_state            SimpleAggregateFunction(anyLast, Nullable(Enum8(
                                               'QUEUED'   = 1,
                                               'LEASED' = 2,
                                               'PENDING' = 3,
                                               'RUNNING' = 4,
                                               'SUCCEEDED' = 5,
                                               'FAILED'   = 6,
                                               'CANCELLED'= 7,
                                               'REJECTED' = 8,
                                               'PREEMPTED'= 9))),
  cancel_ts            SimpleAggregateFunction(anyLast, Nullable(DateTime64(3))),
  cancel_reason        SimpleAggregateFunction(anyLast, Nullable(String)),
  cancel_user          SimpleAggregateFunction(anyLast, Nullable(String)),
  latest_run_id        SimpleAggregateFunction(anyLast, Nullable(String)),
  run_cluster          SimpleAggregateFunction(anyLast, Nullable(String)),
  run_exit_code        SimpleAggregateFunction(anyLast, Nullable(Int32)),
  run_finished_ts      SimpleAggregateFunction(anyLast, Nullable(DateTime64(3))),
  run_state            SimpleAggregateFunction(anyLast, Nullable(String)),
  run_node             SimpleAggregateFunction(anyLast, Nullable(String)),
  run_leased_ts        SimpleAggregateFunction(anyLast, Nullable(DateTime64(3))),
  run_pending_ts       SimpleAggregateFunction(anyLast, Nullable(DateTime64(3))),
  run_started_ts       SimpleAggregateFunction(anyLast, Nullable(DateTime64(3))),
  last_transition_time SimpleAggregateFunction(anyLast, Nullable(DateTime64(3))),
  last_update_ts       SimpleAggregateFunction(anyLast, DateTime64(3)),
  error                SimpleAggregateFunction(anyLast, Nullable(String)),
  merged               SimpleAggregateFunction(any, Nullable(Bool))
)
ENGINE = AggregatingMergeTree()
ORDER BY (job_id)
SETTINGS deduplicate_merge_projection_mode = 'drop';

-- Fast lookup by jobid filtering to merged jobs only
ALTER TABLE jobs
  ADD PROJECTION merge_jobid_lookup (SELECT * ORDER BY (merged, job_id));

-- Fast ordering by last_transition_time filtering to merged jobs only
ALTER TABLE jobs
  ADD PROJECTION merge_last_transition_time_lookup (SELECT * ORDER BY (merged, last_transition_time, job_id));

-- Fast lookup by queue filtering to merged jobs only
ALTER TABLE jobs
  ADD PROJECTION merge_queue_lookup (SELECT * ORDER BY (merged, queue, last_transition_time, job_id));

ALTER TABLE jobs
  MODIFY SETTING
  min_age_to_force_merge_seconds = 2;

ALTER TABLE jobs
  MODIFY SETTING
  max_parts_to_merge_at_once = 0;

/* ======================= *
 * job_runs                *
 * ======================= */
CREATE TABLE IF NOT EXISTS job_runs (
  job_id      FixedString(26),
  run_id      String,
  cluster     SimpleAggregateFunction(any, String),
  exit_code   SimpleAggregateFunction(anyLast, Nullable(Int32)),
  state       SimpleAggregateFunction(anyLast, Nullable(String)),
  node        SimpleAggregateFunction(any, String),
  leased_ts   SimpleAggregateFunction(any, DateTime64(3)),
  pending_ts  SimpleAggregateFunction(anyLast, Nullable(DateTime64(3))),
  started_ts  SimpleAggregateFunction(anyLast, Nullable(DateTime64(3))),
  finished_ts SimpleAggregateFunction(anyLast, Nullable(DateTime64(3))),
  merged      SimpleAggregateFunction(any, Nullable(Bool))
)
ENGINE = AggregatingMergeTree()
ORDER BY (job_id, run_id)
SETTINGS deduplicate_merge_projection_mode = 'drop';

/* ======================= *
 * job_specs               *
 * ======================= */
CREATE TABLE IF NOT EXISTS job_specs (
  job_id   FixedString(26),
  job_spec String
)
ENGINE = ReplacingMergeTree()
ORDER BY (job_id);

/* ======================= *
 * job_run_errors          *
 * ======================= */
CREATE TABLE IF NOT EXISTS job_run_errors (
  run_id        FixedString(26),
  error_message String
)
ENGINE = ReplacingMergeTree()
ORDER BY (run_id);

/* ======================= *
 * job_run_debugs              *
 * ======================= */
CREATE TABLE IF NOT EXISTS job_run_debugs (
  run_id         FixedString(26),
  debug_message  String
)
ENGINE = ReplacingMergeTree()
ORDER BY (run_id);

-- +goose Down
DROP TABLE IF EXISTS job_debugs;
DROP TABLE IF EXISTS job_errors;
DROP TABLE IF EXISTS job_specs;
DROP TABLE IF EXISTS job_runs;
DROP TABLE IF EXISTS jobs;
