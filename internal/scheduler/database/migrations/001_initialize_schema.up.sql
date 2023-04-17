CREATE TABLE queues (
  name text PRIMARY KEY,
  weight double precision NOT NULL
);

CREATE TABLE jobs (
    job_id text PRIMARY KEY,
    job_set text NOT NULL,
    queue text NOT NULL,
    user_id text NOT NULL,
    -- timestamp that tells us when the job has been submitted
    submitted bigint NOT NULL,
    groups bytea, -- compressed
    priority bigint NOT NULL,
    -- Indicates if the job is queued
    queued boolean NOT NULL default false,
    -- The current version of the queued column, used to prevent queued state going backwards on event replay
    queued_version integer NOT NULL default 0,
    -- Indicates that the user has requested the job be cancelled
    cancel_requested boolean NOT NULL DEFAULT false,
    -- Indicates if this job has been cancelled
    cancelled boolean NOT NULL DEFAULT false,
    -- Indicates if this job has has been cancelled as part of a cancel jobset op
    cancel_by_jobset_requested boolean NOT NULL DEFAULT false,
    -- Set to true when a JobSucceeded event has been received for this job by the ingester.
    succeeded boolean NOT NULL DEFAULT false,
    -- Set to true when a terminal JobErrors event has been received for this job by the ingester.
    failed boolean NOT NULL DEFAULT false,
    -- SubmitJob message stored as a proto buffer.
    submit_message bytea NOT NULL,
    -- JobSchedulingInfo message stored as a proto buffer.
    scheduling_info bytea NOT NULL,
    -- The current version of the JobSchedulingInfo, used to prevent JobSchedulingInfo state going backwards on even replay
    scheduling_info_version integer NOT NULL default 0,
    serial bigserial NOT NULL,
    last_modified timestamptz NOT NULL
);
CREATE UNIQUE INDEX idx_jobs_serial ON jobs (serial);
ALTER TABLE jobs ALTER COLUMN groups SET STORAGE EXTERNAL;
ALTER TABLE jobs ALTER COLUMN submit_message SET STORAGE EXTERNAL;

CREATE TABLE runs (
    run_id uuid PRIMARY KEY,
    job_id text NOT NULL,
    -- used to order runs
    created bigint NOT NULL,
    -- Needed to efficiently cancel all runs for a particular job set.
    job_set text NOT NULL,
    -- Executor this job run is assigned to.
    executor text NOT NULL,
    -- Node this job run is assigned to.
    node text NOT NULL,
    -- Indicates if this lease has been cancelled.
    cancelled boolean NOT NULL DEFAULT false,
    -- Set to true once a JobRunRunning messages is received for this run.
    -- I.e., is true if the run has ever been started, even if it later failed.
    running boolean NOT NULL DEFAULT false,
    -- Set to true if a JobRunSucceeded message is received for this run.
    succeeded boolean NOT NULL DEFAULT false,
    -- Set to true when a terminal JobRunErrors event has been received for this run by the ingester.
    failed boolean NOT NULL DEFAULT false,
    -- Set to true when the lease is returned by the executor.
    returned boolean NOT NULL DEFAULT false,
    -- Set to true when the returned job run was given a chance to run. i.e unscheduled jobs will be false, image pull failure would be true
    run_attempted boolean NOT NULL DEFAULT false,
    serial bigserial NOT NULL,
    last_modified timestamptz NOT NULL
);
CREATE UNIQUE INDEX idx_runs_serial ON runs (serial);
CREATE INDEX idx_runs_job_id ON runs (job_id);
CREATE INDEX idx_runs_job_set ON runs (job_set);

CREATE TABLE markers (
    group_id uuid NOT NULL,
    partition_id integer NOT NULL,
    created timestamptz NOT NULL default now(),
    PRIMARY KEY (group_id, partition_id)
);

CREATE TABLE executors (
    -- unique identified for an executor
    executor_id text PRIMARY KEY,
    -- the last lease request made by the executor.  Compressed.
    last_request bytea,
    -- the timestamp of last lease request made by the executor
    last_updated timestamptz NOT NULL
);

-- stores terminal job run errors
-- we need this because the job error generated by the scheduler needs to contain the run error
CREATE TABLE job_run_errors (
    run_id uuid PRIMARY KEY,
    job_id text NOT NULL,
    -- Byte array containing a compressed JobRunErrors proto message.
    error bytea NOT NULL
);
CREATE INDEX idx_job_run_errors_job_id ON job_run_errors (job_id);

ALTER TABLE job_run_errors ALTER COLUMN error SET STORAGE EXTERNAL;


-- Automatically increment serial and set last_modified on insert.
-- Because we upsert by inserting from a temporary table, this trigger handles both insert and update.
-- All new/updated rows can be queried by querying for all rows with serial larger than that of the most recent query.
--
-- Source:
-- https://dba.stackexchange.com/questions/294727/how-to-auto-increment-a-serial-column-on-update
CREATE OR REPLACE FUNCTION trg_increment_serial_set_last_modified()
    RETURNS trigger
    LANGUAGE plpgsql AS
$func$
BEGIN
    NEW.serial := nextval(CONCAT(TG_TABLE_SCHEMA, '.', TG_TABLE_NAME, '_serial_seq'));
    NEW.last_modified := NOW();
    RETURN NEW;
END
$func$;

CREATE TRIGGER next_serial_on_insert_jobs
    BEFORE INSERT or UPDATE ON jobs
    FOR EACH ROW
EXECUTE FUNCTION trg_increment_serial_set_last_modified();

CREATE TRIGGER next_serial_on_insert_runs
    BEFORE INSERT or UPDATE ON runs
    FOR EACH ROW
EXECUTE FUNCTION trg_increment_serial_set_last_modified();
