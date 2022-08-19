/*
 * CODE GENERATED AUTOMATICALLY WITH
 *    github.com/wlbr/templify
 * THIS FILE SHOULD NOT BE EDITED BY HAND
 */

package sql

// SchemaTemplate is a generated function returning the template as a string.
// That string should be parsed by the functions of the golang's template package.
func SchemaTemplate() string {
	var tmpl = "CREATE TABLE queues (\n" +
		"    name text PRIMARY KEY,\n" +
		"    weight double precision NOT NULL\n" +
		");\n" +
		"\n" +
		"CREATE TABLE jobs (\n" +
		"    job_id UUID PRIMARY KEY,\n" +
		"    job_set text NOT NULL,\n" +
		"    queue text NOT NULL,\n" +
		"    priority bigint NOT NULL,\n" +
		"     -- Dict mapping resource type to amount requested.\n" +
		"     -- TODO: We may want a proto message containing the minimal amount of data the scheduler needs.\n" +
		"    -- claims json NOT NULL,\n" +
		"    -- SubmitJob Pulsar message stored as a proto buffer.\n" +
		"    message bytea NOT NULL,\n" +
		"    message_index bigint NOT NULL\n" +
		");\n" +
		"\n" +
		"CREATE TABLE runs (\n" +
		"    run_id UUID PRIMARY KEY,\n" +
		"    job_id UUID NOT NULL,\n" +
		"    -- Executor this job run is assigned to.\n" +
		"    executor text NOT NULL,\n" +
		"    -- Info of where this job is assigned to run. NULL until assigned to a node.\n" +
		"    assignment json,\n" +
		"    -- Tombstone value.\n" +
		"    deleted bool NOT NULL,\n" +
		"    last_modified timestamp with time zone NOT NULL\n" +
		");\n" +
		"\n" +
		"CREATE TABLE executors (\n" +
		"    id text PRIMARY KEY,\n" +
		"    -- Map from resource type to total amount available of that resource.\n" +
		"    -- The following pairs are required: \"cpu\", \"memory\", \"storage\".\n" +
		"    -- In addition, any accelerators (e.g., A100_16GB) must be included.\n" +
		"    total_resources json NOT NULL,\n" +
		"    -- Map from resource type to max amount of that resource available on any node.\n" +
		"    -- Must contain a pair for each resource type in totalResources.\n" +
		"    max_resources json NOT NULL\n" +
		");\n" +
		"\n" +
		"CREATE TABLE pulsar (    \n" +
		"    -- Pulsar topic name. Should not include partition index.\n" +
		"    topic text NOT NULL,\n" +
		"    -- pulsar.MessageID fields.\n" +
		"    ledger_id bigint NOT NULL,\n" +
		"    entry_id bigint NOT NULL,\n" +
		"    batch_idx int not NULL,\n" +
		"    partition_idx int NOT NULL\n" +
		");\n" +
		"\n" +
		"-- The combination topic name and partition index must be unique.\n" +
		"CREATE UNIQUE INDEX topic_partition ON pulsar (topic, partition_idx);\n" +
		"\n" +
		"CREATE TABLE nodeinfo (\n" +
		"    -- Name of the node. Must be unique across all clusters.\n" +
		"    node_name text PRIMARY KEY,\n" +
		"    -- Most recently received NodeInfo message for this node.\n" +
		"    message bytea NOT NULL,\n" +
		"    -- Serial auto-incrementing on write and update.\n" +
		"    -- Used to only read rows that were updated since the last write.\n" +
		"    serial bigserial NOT NULL,\n" +
		"    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW()\n" +
		");\n" +
		"\n" +
		"-- Auto-increment nodeinfo.serial on insert and update.\n" +
		"--\n" +
		"-- Source:\n" +
		"-- https://dba.stackexchange.com/questions/294727/how-to-auto-increment-a-serial-column-on-update\n" +
		"CREATE OR REPLACE FUNCTION trg_increment_serial_nodeinfo()\n" +
		"  RETURNS trigger\n" +
		"  LANGUAGE plpgsql AS\n" +
		"$func$\n" +
		"BEGIN\n" +
		"  NEW.serial := nextval(pg_get_serial_sequence('public.nodeinfo', 'serial'));\n" +
		"  NEW.last_modified := NOW();\n" +
		"  RETURN NEW;\n" +
		"END\n" +
		"$func$;\n" +
		"\n" +
		"CREATE TRIGGER next_serial_on_insert_nodeinfo\n" +
		"BEFORE INSERT ON nodeinfo\n" +
		"FOR EACH ROW\n" +
		"EXECUTE FUNCTION trg_increment_serial_nodeinfo();\n" +
		"\n" +
		"CREATE TRIGGER next_serial_on_update_nodeinfo\n" +
		"BEFORE UPDATE ON nodeinfo\n" +
		"FOR EACH ROW\n" +
		"WHEN (OLD.message IS DISTINCT FROM NEW.message)\n" +
		"EXECUTE FUNCTION trg_increment_serial_nodeinfo();\n" +
		"\n" +
		"-- -- Automatically set last_modified on update.\n" +
		"-- --\n" +
		"-- -- Source:\n" +
		"-- -- https://stackoverflow.com/questions/52426656/track-last-modification-timestamp-of-a-row-in-postgres\n" +
		"-- CREATE OR REPLACE FUNCTION trg_set_timestamp_nodeinfo()\n" +
		"--   RETURNS TRIGGER\n" +
		"--   LANGUAGE plpgsql AS\n" +
		"-- $func$\n" +
		"-- BEGIN\n" +
		"--   NEW.serial = OLD.serial;\n" +
		"--   NEW.last_modified = OLD.last_modified;\n" +
		"--   RETURN NEW;\n" +
		"-- END\n" +
		"-- $func$;\n" +
		"\n" +
		"-- CREATE TRIGGER set_timestamp_on_update_nodeinfo\n" +
		"-- BEFORE UPDATE ON nodeinfo\n" +
		"-- FOR EACH ROW\n" +
		"-- WHEN (OLD.message IS DISTINCT FROM NEW.message)\n" +
		"-- EXECUTE FUNCTION trg_set_timestamp_nodeinfo();"
	return tmpl
}
