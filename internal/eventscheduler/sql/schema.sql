CREATE TABLE queues (
    name text PRIMARY KEY,
    weight double precision NOT NULL
);

CREATE TABLE jobs (
    job_id UUID PRIMARY KEY,
    -- TODO: We could store a hash to reduce memory.
    job_set text NOT NULL,
    queue text NOT NULL,
    user_id text NOT NULL,
    groups text[],
    priority bigint NOT NULL,
    -- Indicates if this job has been cancelled by a user.
    cancelled boolean NOT NULL DEFAULT false,
    -- Set to true when a JobSucceeded event has been received for this job by the ingester.
    succeeded boolean NOT NULL DEFAULT false,
    -- Set to true when a terminal JobErrors event has been received for this job by the ingester.
    -- The error itself is written into the job_errors table.
    failed boolean NOT NULL DEFAULT false,
     -- Dict mapping resource type to amount requested.
     -- TODO: We need proto message containing the minimal amount of data the scheduler needs.
    -- claims json NOT NULL,
    -- SubmitJob Pulsar message stored as a proto buffer.
    submit_message bytea NOT NULL,
    serial bigserial NOT NULL,
    last_modified TIMESTAMPTZ NOT NULL
);

CREATE TABLE runs (
    run_id UUID PRIMARY KEY,
    job_id UUID NOT NULL,
    -- Needed to efficiently cancel all runs for a particular job set.
    -- TODO: We could store a hash to reduce memory.
    job_set TEXT NOT NULL,
    -- Executor this job run is assigned to.
    executor text NOT NULL,
    -- TODO: We probably want this to be proto.
    -- assignment json,
    -- True if this run has been sent to the executor already.
    -- Used to control which runs are sent to the executor when it requests jobs.
    sent_to_executor boolean NOT NULL,
    -- Indicates if this lease has been cancelled.
    cancelled boolean NOT NULL,
    -- Set to true once a JobRunRunning messages is received for this run.
    -- I.e., is true if the run has ever been started, even if it later failed.
    running boolean NOT NULL,
    -- Set to true if a JobRunSucceeded message is received for this run.
    succeeded boolean NOT NULL,
    -- Most recently received terminal error.
    -- If not NULL, this job has failed.
    -- There shuld only be at most one terminal error for any given run.
    error bytea,
    serial bigserial NOT NULL,
    last_modified TIMESTAMPTZ NOT NULL
);

-- Info of physical resources assigned to job runs.
-- Populated based on JobRunAssigned Pulsar messages.
-- Job runs with no entry in this table have not yet been assigned resources.
CREATE TABLE job_run_assignments (
    run_id UUID PRIMARY KEY,
    -- Encoded proto message storing the assignment.
    assignment bytea NOT NULL,
    serial bigserial NOT NULL,
    last_modified TIMESTAMPTZ NOT NULL
);

CREATE TABLE job_errors (
    -- To ensure inserts are idempotent, we to asociate with each error a unique id
    -- that can be computed deterministically by the ingester.
    id text PRIMARY KEY,
    job_id UUID NOT NULL,
    -- Byte array containing a JobErrors proto message.
    error bytea NOT NULL,
    -- Indicates if this error is terminal.
    -- The presence of a terminal error indicates this job has failed.
    terminal boolean NOT NULL DEFAULT false,
    serial bigserial NOT NULL,
    last_modified TIMESTAMPTZ NOT NULL
);

CREATE INDEX job_errors_id ON job_errors (job_id);

-- There should ever only be one terminal error for any job.
-- We avoid creating the index as unique to avoid failing inserts on programming bugs.
CREATE INDEX job_errors_id_terminal ON job_errors (job_id, terminal);

CREATE TABLE job_run_errors (
    -- To ensure inserts are idempotent, we to asociate with each error a unique id
    -- that can be computed deterministically by the ingester.    
    id text PRIMARY KEY,
    run_id UUID NOT NULL,
    -- Byte array containing a JobRunErrors proto message.    
    error bytea NOT NULL,
    -- Indicates if this error is terminal.
    -- The presence of a terminal error indicates this job run has failed.
    terminal boolean NOT NULL DEFAULT false,
    serial bigserial NOT NULL,
    last_modified TIMESTAMPTZ NOT NULL
);

-- The combination topic name and partition index must be unique.
CREATE INDEX job_run_errors_id ON job_run_errors (run_id);

-- There should ever only be one terminal error for any run.
-- We avoid creating the index as unique to avoid failing inserts on programming bugs.
CREATE INDEX job_run_errors_id_terminal ON job_run_errors (run_id, terminal);

-- CREATE TABLE executors (
--     id text PRIMARY KEY,
--     -- Map from resource type to total amount available of that resource.
--     -- The following pairs are required: "cpu", "memory", "storage".
--     -- In addition, any accelerators (e.g., A100_16GB) must be included.
--     total_resources json NOT NULL,
--     -- Map from resource type to max amount of that resource available on any node.
--     -- Must contain a pair for each resource type in totalResources.
--     max_resources json NOT NULL
-- );

CREATE TABLE nodeinfo (
    -- Name of the node. Must be unique across all clusters.
    node_name text NOT NULL,
    -- Name of the executor responsible for this node.
    executor text NOT NULL,
    -- Most recently received NodeInfo message for this node.
    message bytea NOT NULL,
    serial bigserial NOT NULL,
    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The combination node name and executor must be unique.
CREATE UNIQUE INDEX node_name_executor ON nodeinfo (node_name, executor);

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
BEFORE INSERT ON jobs
FOR EACH ROW
EXECUTE FUNCTION trg_increment_serial_set_last_modified();

CREATE TRIGGER next_serial_on_insert_runs
BEFORE INSERT ON runs
FOR EACH ROW
EXECUTE FUNCTION trg_increment_serial_set_last_modified();

CREATE TRIGGER next_serial_on_insert_job_run_assignments
BEFORE INSERT ON job_run_assignments
FOR EACH ROW
EXECUTE FUNCTION trg_increment_serial_set_last_modified();

CREATE TRIGGER next_serial_on_insert_job_errors
BEFORE INSERT ON job_errors
FOR EACH ROW
EXECUTE FUNCTION trg_increment_serial_set_last_modified();

CREATE TRIGGER next_serial_on_insert_job_run_errors
BEFORE INSERT ON job_run_errors
FOR EACH ROW
EXECUTE FUNCTION trg_increment_serial_set_last_modified();

CREATE TRIGGER next_serial_on_insert_nodeinfo
BEFORE INSERT ON nodeinfo
FOR EACH ROW
EXECUTE FUNCTION trg_increment_serial_set_last_modified();

-- Used to store Pulsar message ids for idempotency checks.
CREATE TABLE pulsar (
    -- Pulsar topic name. Should not include partition index.
    topic text NOT NULL,
    -- pulsar.MessageID fields.
    ledger_id bigint NOT NULL,
    entry_id bigint NOT NULL,
    batch_idx int not NULL,
    partition_idx int NOT NULL
);

-- The combination topic name and partition index must be unique.
CREATE UNIQUE INDEX topic_partition ON pulsar (topic, partition_idx);