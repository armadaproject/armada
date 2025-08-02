-- +goose Up
CREATE TABLE IF NOT EXISTS jobs (
  job_id FixedString(26),
  queue SimpleAggregateFunction(any, LowCardinality(String)),
  namespace SimpleAggregateFunction(any, LowCardinality(String)),
  job_set SimpleAggregateFunction(any, String),
  cpu SimpleAggregateFunction(any, Int64),
  memory SimpleAggregateFunction(any, Int64),
  ephemeral_storage SimpleAggregateFunction(any, Int64),
  gpu SimpleAggregateFunction(any, Int64),
  priority SimpleAggregateFunction(anyLast, Int64),
  submit_ts SimpleAggregateFunction(any, DateTime64(3)),
  priority_class SimpleAggregateFunction(any, LowCardinality(String)),
  annotations SimpleAggregateFunction(any, Map(String, String)),
  job_state SimpleAggregateFunction(anyLast, LowCardinality(String)),
  cancel_ts SimpleAggregateFunction(anyLast, Nullable(DateTime64(3))),
  cancel_reason SimpleAggregateFunction(anyLast, Nullable(String)),
  cancel_user SimpleAggregateFunction(anyLast, Nullable(String)),
  latest_run_id SimpleAggregateFunction(anyLast, Nullable(String)),
  run_cluster SimpleAggregateFunction(anyLast, LowCardinality(Nullable(String))),
  run_exit_code SimpleAggregateFunction(anyLast, Nullable(Int32)),
  run_finished_ts SimpleAggregateFunction(anyLast, Nullable(DateTime64(3))),
  run_state SimpleAggregateFunction(anyLast, LowCardinality(Nullable(String))),
  run_node SimpleAggregateFunction(anyLast, LowCardinality(Nullable(String))),
  run_leased SimpleAggregateFunction(anyLast, Nullable(DateTime64(3))),
  run_pending_ts SimpleAggregateFunction(anyLast, Nullable(DateTime64(3))),
  run_started_ts SimpleAggregateFunction(anyLast, Nullable(DateTime64(3))),
  last_transition_time SimpleAggregateFunction(anyLast, DateTime64(3)),
  last_update_ts SimpleAggregateFunction(anyLast, DateTime64(3)),
  merged SimpleAggregateFunction(any, Bool)
  )
  ENGINE = AggregatingMergeTree()
  ORDER BY (job_id)
  SETTINGS deduplicate_merge_projection_mode = 'drop';

-- +goose Down
DROP TABLE IF EXISTS jobs;
