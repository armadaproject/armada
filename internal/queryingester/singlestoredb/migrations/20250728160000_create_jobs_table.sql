-- +goose Up
-- NOTE: this migration assumes you already selected a database in the DSN (e.g., .../armada)
-- If you need to create/select a DB, do it outside Goose or in a separate migration.

CREATE TABLE jobs_active (
                           job_id CHAR(26) NOT NULL,
                           queue VARCHAR(255) NOT NULL,
                           namespace VARCHAR(255),
                           job_set VARCHAR(255),
                           priority_class VARCHAR(255),

  -- JSON compatibility for older SingleStore: use LONGTEXT + JSON_VALID check
                           annotations LONGTEXT CHECK (JSON_VALID(annotations)),

                           cpu BIGINT,
                           memory BIGINT,
                           ephemeral_storage BIGINT,
                           gpu BIGINT,
                           priority BIGINT,
                           run_exit_code INT,

                           job_state VARCHAR(64),
                           submit_ts DATETIME(6),
                           cancel_ts DATETIME(6),
                           run_finished_ts DATETIME(6),
                           run_leased_ts DATETIME(6),
                           run_pending_ts DATETIME(6),
                           run_started_ts DATETIME(6),
                           last_transition_time DATETIME(6),

                           cancel_reason VARCHAR(255),
                           cancel_user VARCHAR(255),
                           latest_run_id VARCHAR(255),
                           run_cluster VARCHAR(255),
                           run_state VARCHAR(64),
                           run_node VARCHAR(255),
                           error TEXT,

                           last_update_ts DATETIME(6) NOT NULL,

                           PRIMARY KEY (job_id),
                           KEY idx_active_queue (queue),
                           KEY idx_active_job_state (job_state),
                           KEY idx_active_last_transition_time (last_transition_time)
);

CREATE TABLE jobs_history (
                            job_id CHAR(26) NOT NULL,
                            queue VARCHAR(255) NOT NULL,
                            namespace VARCHAR(255),
                            job_set VARCHAR(255),
                            priority_class VARCHAR(255),

  -- JSON compatibility for older SingleStore: use LONGTEXT + JSON_VALID check
                            annotations LONGTEXT CHECK (JSON_VALID(annotations)),

                            cpu BIGINT,
                            memory BIGINT,
                            ephemeral_storage BIGINT,
                            gpu BIGINT,
                            priority BIGINT,
                            run_exit_code INT,

                            job_state VARCHAR(64),
                            submit_ts DATETIME(6),
                            cancel_ts DATETIME(6),
                            run_finished_ts DATETIME(6),
                            run_leased_ts DATETIME(6),
                            run_pending_ts DATETIME(6),
                            run_started_ts DATETIME(6),
                            last_transition_time DATETIME(6),

                            cancel_reason VARCHAR(255),
                            cancel_user VARCHAR(255),
                            latest_run_id VARCHAR(255),
                            run_cluster VARCHAR(255),
                            run_state VARCHAR(64),
                            run_node VARCHAR(255),
                            error TEXT,

                            last_update_ts DATETIME(6) NOT NULL,

  -- Sharding + lookup helpers (safe in 5.x; drop SHARD KEY if your build complains)
                            SHARD KEY (job_id),
                            KEY idx_hist_job_id (job_id),
                            KEY idx_hist_queue (queue),
                            KEY idx_hist_job_state (job_state),
                            KEY idx_hist_last_transition_time (last_transition_time)
);

-- +goose Down
DROP TABLE IF EXISTS jobs_active;
DROP TABLE IF EXISTS jobs_history;
