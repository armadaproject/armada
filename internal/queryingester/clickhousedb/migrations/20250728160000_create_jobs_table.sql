-- +goose Up
/* ======================= *
 * jobs (summary per job)  *
 * ======================= */
CREATE TABLE jobs_raw
(
  job_id FixedString(26),
  queue             LowCardinality(String),
  namespace         LowCardinality(Nullable(String)),
  job_set           Nullable(String),
  priority_class    LowCardinality(Nullable(String)),
  annotations       Map(String, String),
  cpu               Nullable(Int64),
  memory            Nullable(Int64),
  ephemeral_storage Nullable(Int64),
  gpu               Nullable(Int64),
  priority          Nullable(Int64),
  run_exit_code     Nullable(Int32),
  job_state         LowCardinality(Nullable(String)),
  submit_ts            Nullable(DateTime64(3)),
  cancel_ts            Nullable(DateTime64(3)),
  run_finished_ts      Nullable(DateTime64(3)),
  run_leased_ts        Nullable(DateTime64(3)),
  run_pending_ts       Nullable(DateTime64(3)),
  run_started_ts       Nullable(DateTime64(3)),
  last_transition_time Nullable(DateTime64(3)),
  cancel_reason   Nullable(String),
  cancel_user     LowCardinality(Nullable(String)),
  latest_run_id   Nullable(String),
  run_cluster     LowCardinality(Nullable(String)),
  run_state       LowCardinality(Nullable(String)),
  run_node        LowCardinality(Nullable(String)),
  error           Nullable(String),
  last_update_ts  DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (job_id, last_update_ts);

-- Jobs indexed by jobId
CREATE TABLE jobs
(
  job_id FixedString(26),
  queue             LowCardinality(String),
  namespace         LowCardinality(Nullable(String)),
  job_set           Nullable(String),
  priority_class    LowCardinality(Nullable(String)),
  annotations       Map(String, String),
  cpu               Nullable(Int64),
  memory            Nullable(Int64),
  ephemeral_storage Nullable(Int64),
  gpu               Nullable(Int64),
  priority          Nullable(Int64),
  run_exit_code     Nullable(Int32),
  job_state         LowCardinality(Nullable(String)),
  submit_ts            Nullable(DateTime64(3)),
  cancel_ts            Nullable(DateTime64(3)),
  run_finished_ts      Nullable(DateTime64(3)),
  run_leased_ts        Nullable(DateTime64(3)),
  run_pending_ts       Nullable(DateTime64(3)),
  run_started_ts       Nullable(DateTime64(3)),
  last_transition_time Nullable(DateTime64(3)),
  cancel_reason   LowCardinality(Nullable(String)),
  cancel_user     LowCardinality(Nullable(String)),
  latest_run_id   Nullable(String),
  run_cluster     LowCardinality(Nullable(String)),
  run_state       LowCardinality(Nullable(String)),
  run_node        LowCardinality(Nullable(String)),
  error           Nullable(String),
  last_update_ts  DateTime64(3)
)
ENGINE = CoalescingMergeTree
ORDER BY job_id;


CREATE MATERIALIZED VIEW mv_raw_to_jobs
TO jobs AS
SELECT * FROM jobs_raw;


CREATE TABLE jobs_by_queue
(
  job_id FixedString(26),
  queue  String,
  namespace         LowCardinality(Nullable(String)),
  job_set           Nullable(String),
  priority_class    LowCardinality(Nullable(String)),
  annotations       Map(String, String),
  cpu               Nullable(Int64),
  memory            Nullable(Int64),
  ephemeral_storage Nullable(Int64),
  gpu               Nullable(Int64),
  priority          Nullable(Int64),
  run_exit_code     Nullable(Int32),
  job_state         LowCardinality(Nullable(String)),
  submit_ts            Nullable(DateTime64(3)),
  cancel_ts            Nullable(DateTime64(3)),
  run_finished_ts      Nullable(DateTime64(3)),
  run_leased_ts        Nullable(DateTime64(3)),
  run_pending_ts       Nullable(DateTime64(3)),
  run_started_ts       Nullable(DateTime64(3)),
  last_transition_time Nullable(DateTime64(3)),
  cancel_reason   Nullable(String),
  cancel_user     LowCardinality(Nullable(String)),
  latest_run_id   Nullable(String),
  run_cluster     LowCardinality(Nullable(String)),
  run_state       LowCardinality(Nullable(String)),
  run_node        LowCardinality(Nullable(String)),
  error           Nullable(String),
  last_update_ts  DateTime64(3)
)
  ENGINE = CoalescingMergeTree
ORDER BY (queue, job_id);

ALTER TABLE jobs_by_queue
  ADD INDEX idx_state_set (job_state) TYPE set(256) GRANULARITY 4;

CREATE MATERIALIZED VIEW mv_raw_to_jobs_by_queue
TO jobs_by_queue AS
SELECT * FROM jobs_raw;



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
