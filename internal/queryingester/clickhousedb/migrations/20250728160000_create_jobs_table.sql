-- +goose Up
/* ======================= *
 * jobs (summary per job)  *
 * ======================= */
CREATE TABLE jobs
(
  job_id FixedString(26),
  queue             LowCardinality(String),
  namespace         LowCardinality(Nullable(String)),
  job_set           Nullable(String),
  priority_class    LowCardinality(Nullable(String)),
  annotations       Nullable(JSON),
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


CREATE TABLE jobs_history
(
    job_id FixedString(26),
    queue             LowCardinality(String),
    namespace         LowCardinality(Nullable(String)),
    job_set           Nullable(String),
    priority_class    LowCardinality(Nullable(String)),
    annotations       Nullable(JSON),
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
ENGINE = ReplacingMergeTree()
ORDER BY job_id
SETTINGS deduplicate_merge_projection_mode = 'drop';

-- CREATE OR REPLACE VIEW jobs_all AS
-- SELECT *
-- FROM jobs FINAL
-- UNION ALL
-- SELECT *
-- FROM jobs_history;

ALTER TABLE jobs_history
    ADD PROJECTION p_queue_transition
    (
SELECT *
ORDER BY (queue, last_transition_time, job_id)
    );

ALTER TABLE jobs_history
    ADD PROJECTION p_queue_jobset_transition
    (
SELECT *
ORDER BY (queue, job_set, last_transition_time, job_id)
    );

ALTER TABLE jobs_history
    ADD PROJECTION p_transition_time
    (
SELECT *
ORDER BY (last_transition_time, job_id)
    );

ALTER TABLE jobs_history ADD INDEX ix_queue queue TYPE set(1000) GRANULARITY 1;
ALTER TABLE jobs_history ADD INDEX ix_queue_jobset (queue, job_set) TYPE set(1000) GRANULARITY 1;

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
