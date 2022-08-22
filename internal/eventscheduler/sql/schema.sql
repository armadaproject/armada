CREATE TABLE queues (
    name text PRIMARY KEY,
    weight double precision NOT NULL
);

CREATE TABLE jobs (
    job_id UUID PRIMARY KEY,
    job_set text NOT NULL,
    queue text NOT NULL,
    user_id text NOT NULL,
    groups text[] NOT NULL DEFAULT array[]::text[],
    priority bigint NOT NULL,
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
    -- Executor this job run is assigned to.
    executor text NOT NULL,
    -- Info of where this job is assigned to run. NULL until assigned to a node.
    assignment json,
    -- True if this run has been sent to the executor already.
    -- Used to control which runs are sent to the executor when it requests jobs.
    sent_to_executor boolean NOT NULL,
    -- Indicates if this lease has been cancelled.
    cancelled boolean NOT NULL,
    serial bigserial NOT NULL,
    last_modified TIMESTAMPTZ NOT NULL
);

CREATE TABLE executors (
    id text PRIMARY KEY,
    -- Map from resource type to total amount available of that resource.
    -- The following pairs are required: "cpu", "memory", "storage".
    -- In addition, any accelerators (e.g., A100_16GB) must be included.
    total_resources json NOT NULL,
    -- Map from resource type to max amount of that resource available on any node.
    -- Must contain a pair for each resource type in totalResources.
    max_resources json NOT NULL
);

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

CREATE TABLE nodeinfo (
    -- Name of the node. Must be unique across all clusters.
    node_name text PRIMARY KEY,
    -- Most recently received NodeInfo message for this node.
    message bytea NOT NULL,
    serial bigserial NOT NULL,
    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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

CREATE TRIGGER next_serial_on_insert_nodeinfo
BEFORE INSERT ON nodeinfo
FOR EACH ROW
EXECUTE FUNCTION trg_increment_serial_set_last_modified();

CREATE TRIGGER next_serial_on_insert_runs
BEFORE INSERT ON runs
FOR EACH ROW
EXECUTE FUNCTION trg_increment_serial_set_last_modified();

CREATE TRIGGER next_serial_on_insert_jobs
BEFORE INSERT ON jobs
FOR EACH ROW
EXECUTE FUNCTION trg_increment_serial_set_last_modified();