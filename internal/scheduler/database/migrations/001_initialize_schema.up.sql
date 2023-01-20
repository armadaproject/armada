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
    -- Indicates that the user has requested the job be cancelled
    cancel_requested boolean NOT NULL DEFAULT false,
    -- Indicates if this job has been cancelled
    cancelled boolean NOT NULL DEFAULT false,
    -- Set to true when a JobSucceeded event has been received for this job by the ingester.
    succeeded boolean NOT NULL DEFAULT false,
    -- Set to true when a terminal JobErrors event has been received for this job by the ingester.
    failed boolean NOT NULL DEFAULT false,
    -- SubmitJob message stored as a proto buffer.
    submit_message bytea NOT NULL,
    -- JobSchedulingInfo message stored as a proto buffer.
    scheduling_info bytea NOT NULL,
    serial bigserial NOT NULL,
    last_modified timestamptz NOT NULL
);

ALTER TABLE jobs ALTER COLUMN groups SET STORAGE EXTERNAL;
ALTER TABLE jobs ALTER COLUMN submit_message SET STORAGE EXTERNAL;

CREATE TABLE runs (
    run_id uuid PRIMARY KEY,
    job_id text NOT NULL,
    -- Needed to efficiently cancel all runs for a particular job set.
    job_set text NOT NULL,
    -- Executor this job run is assigned to.
    executor text NOT NULL,
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
    serial bigserial NOT NULL,
    last_modified timestamptz NOT NULL
);

CREATE TABLE markers (
    group_id uuid NOT NULL,
    partition_id integer NOT NULL,
    PRIMARY KEY (group_id, partition_id)
);


CREATE TABLE job_run_errors (
    run_id uuid PRIMARY KEY,
    -- Byte array containing a JobRunErrors proto message.
    error bytea NOT NULL,
    -- Indicates if this error is terminal.
    -- The presence of a terminal error indicates this job run has failed.
    serial bigserial NOT NULL,
    last_modified timestamptz NOT NULL
);

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

CREATE TRIGGER next_serial_on_insert_job_run_errors
    BEFORE INSERT or UPDATE ON job_run_errors
    FOR EACH ROW
EXECUTE FUNCTION trg_increment_serial_set_last_modified();
