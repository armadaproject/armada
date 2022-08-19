CREATE TABLE queues (
    name text PRIMARY KEY,
    weight double precision NOT NULL
);

CREATE TABLE jobs (
    job_id UUID PRIMARY KEY,
    job_set text NOT NULL,
    queue text NOT NULL,
    priority bigint NOT NULL,
     -- Dict mapping resource type to amount requested.
     -- TODO: We may want a proto message containing the minimal amount of data the scheduler needs.
    -- claims json NOT NULL,
    -- SubmitJob Pulsar message stored as a proto buffer.
    message bytea NOT NULL,
    message_index bigint NOT NULL
);

CREATE TABLE runs (
    run_id UUID PRIMARY KEY,
    job_id UUID NOT NULL,
    -- Executor this job run is assigned to.
    executor text NOT NULL,
    -- Info of where this job is assigned to run. NULL until assigned to a node.
    assignment json,
    -- Tombstone value.
    deleted bool NOT NULL,
    last_modified timestamp with time zone NOT NULL
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
    -- Serial auto-incrementing on write and update.
    -- Used to only read rows that were updated since the last write.
    serial bigserial NOT NULL,
    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Auto-increment nodeinfo.serial on insert and update.
-- TODO: This triggers a double increment on update. Not a problem. But a bit wasteful.
--
-- Source:
-- https://dba.stackexchange.com/questions/294727/how-to-auto-increment-a-serial-column-on-update
CREATE OR REPLACE FUNCTION trg_increment_serial_nodeinfo()
  RETURNS trigger
  LANGUAGE plpgsql AS
$func$
BEGIN
  NEW.serial := nextval(pg_get_serial_sequence('public.nodeinfo', 'serial'));
  NEW.last_modified := NOW();
  RETURN NEW;
END
$func$;

CREATE TRIGGER next_serial_on_insert_nodeinfo
BEFORE INSERT ON nodeinfo
FOR EACH ROW
EXECUTE FUNCTION trg_increment_serial_nodeinfo();

CREATE TRIGGER next_serial_on_update_nodeinfo
BEFORE UPDATE ON nodeinfo
FOR EACH ROW
WHEN (OLD.message IS DISTINCT FROM NEW.message)
EXECUTE FUNCTION trg_increment_serial_nodeinfo();

-- -- Automatically set last_modified on update.
-- --
-- -- Source:
-- -- https://stackoverflow.com/questions/52426656/track-last-modification-timestamp-of-a-row-in-postgres
-- CREATE OR REPLACE FUNCTION trg_set_timestamp_nodeinfo()
--   RETURNS TRIGGER
--   LANGUAGE plpgsql AS
-- $func$
-- BEGIN
--   NEW.serial = OLD.serial;
--   NEW.last_modified = OLD.last_modified;
--   RETURN NEW;
-- END
-- $func$;

-- CREATE TRIGGER set_timestamp_on_update_nodeinfo
-- BEFORE UPDATE ON nodeinfo
-- FOR EACH ROW
-- WHEN (OLD.message IS DISTINCT FROM NEW.message)
-- EXECUTE FUNCTION trg_set_timestamp_nodeinfo();