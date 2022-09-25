/*
 * CODE GENERATED AUTOMATICALLY WITH
 *    github.com/wlbr/templify
 * THIS FILE SHOULD NOT BE EDITED BY HAND
 */

package sql

// SchemaTemplate is a generated function returning the template as a string.
// That string should be parsed by the functions of the golang's template package.
func SchemaTemplate() string {
	tmpl := "\n" +
		"-- TODO: Consistently compress proto messages.\n" +
		"-- TODO: Cleanup may be expensive. Investigate.\n" +
		"-- TODO: Research how postgres ANY works and what its limitations are.\n" +
		"-- TODO: Backup solution inserting one by one.\n" +
		"-- TODO: Look into go generics.\n" +
		"-- TODO: Consider consistently mapping queues and job sets to unique integers. Maybe also node selectors, tolerations, and taints.\n" +
		"-- TODO: Is the serial updated correctly for updates?\n" +
		"\n" +
		"-- TODO: Turns out postgres ANY compares one by one with each element given to it.\n" +
		"-- The solution seems to be to either use a VALUES clause + join or COPY + join.\n" +
		"-- https://stackoverflow.com/questions/34627026/in-vs-any-operator-in-postgresql\n" +
		"-- https://dba.stackexchange.com/questions/91247/optimizing-a-postgres-query-with-a-large-in\n" +
		"-- https://stackoverflow.com/questions/24647503/performance-issue-in-update-query\n" +
		"-- https://stackoverflow.com/questions/17813492/postgres-not-in-performance\n" +
		"\n" +
		"CREATE TABLE queues (\n" +
		"    name text PRIMARY KEY,\n" +
		"    weight double precision NOT NULL\n" +
		");\n" +
		"\n" +
		"CREATE TABLE jobs (\n" +
		"    job_id UUID PRIMARY KEY,\n" +
		"    -- TODO: We could store a hash to reduce memory.\n" +
		"    job_set text NOT NULL,\n" +
		"    queue text NOT NULL,\n" +
		"    user_id text NOT NULL,\n" +
		"    groups text[], -- Consider storing compressed. Could be large. Tell postgres to not compress if we did it ourselves.\n" +
		"    priority bigint NOT NULL,\n" +
		"    -- Indicates if this job has been cancelled by a user.\n" +
		"    -- TODO: Corresponds to should be cancelled.\n" +
		"    cancelled boolean NOT NULL DEFAULT false,\n" +
		"    -- Set to true when a JobSucceeded event has been received for this job by the ingester.\n" +
		"    succeeded boolean NOT NULL DEFAULT false,\n" +
		"    -- Set to true when a terminal JobErrors event has been received for this job by the ingester.\n" +
		"    -- The error itself is written into the job_errors table.\n" +
		"    failed boolean NOT NULL DEFAULT false,\n" +
		"    -- SubmitJob message stored as a proto buffer.\n" +
		"    submit_message bytea NOT NULL,\n" +
		"    -- JobSchedulingInfo message stored as a proto buffer.\n" +
		"    scheduling_info bytea NOT NULL,\n" +
		"    serial bigserial NOT NULL,\n" +
		"    last_modified TIMESTAMPTZ NOT NULL\n" +
		");\n" +
		"\n" +
		"CREATE TABLE runs (\n" +
		"    run_id UUID PRIMARY KEY,\n" +
		"    job_id UUID NOT NULL,\n" +
		"    -- Needed to efficiently cancel all runs for a particular job set.\n" +
		"    -- TODO: We could store a hash to reduce memory.\n" +
		"    -- TODO: We have to use queue-jobset for uniqueness. Maybe hash them together.\n" +
		"    job_set TEXT NOT NULL,\n" +
		"    -- Executor this job run is assigned to.\n" +
		"    executor text NOT NULL,\n" +
		"    -- True if this run has been sent to the executor already.\n" +
		"    -- Used to control which runs are sent to the executor when it requests jobs.\n" +
		"    sent_to_executor boolean NOT NULL DEFAULT false,\n" +
		"    -- Indicates if this lease has been cancelled.\n" +
		"    cancelled boolean NOT NULL DEFAULT false,\n" +
		"    -- Set to true once a JobRunRunning messages is received for this run.\n" +
		"    -- I.e., is true if the run has ever been started, even if it later failed.\n" +
		"    running boolean NOT NULL DEFAULT false,\n" +
		"    -- Set to true if a JobRunSucceeded message is received for this run.\n" +
		"    succeeded boolean NOT NULL DEFAULT false,\n" +
		"    -- Set to true when a terminal JobRunErrors event has been received for this run by the ingester.\n" +
		"    -- The error itself is written into the job_run_errors table.\n" +
		"    failed boolean NOT NULL DEFAULT false,\n" +
		"    serial bigserial NOT NULL,\n" +
		"    last_modified TIMESTAMPTZ NOT NULL\n" +
		");\n" +
		"\n" +
		"-- Info of physical resources assigned to job runs.\n" +
		"-- Populated based on JobRunAssigned Pulsar messages.\n" +
		"-- Job runs with no entry in this table have not yet been assigned resources.\n" +
		"CREATE TABLE job_run_assignments (\n" +
		"    run_id UUID PRIMARY KEY,\n" +
		"    -- Encoded proto message storing the assignment.\n" +
		"    assignment bytea NOT NULL,\n" +
		"    serial bigserial NOT NULL,\n" +
		"    last_modified TIMESTAMPTZ NOT NULL\n" +
		");\n" +
		"\n" +
		"CREATE TABLE job_errors (\n" +
		"    -- To ensure inserts are idempotent, we need to asociate with each error a unique id\n" +
		"    -- that can be computed deterministically by the ingester.\n" +
		"    id integer PRIMARY KEY,\n" +
		"    job_id UUID NOT NULL,\n" +
		"    -- Byte array containing a JobErrors proto message.\n" +
		"    error bytea NOT NULL,\n" +
		"    -- Indicates if this error is terminal.\n" +
		"    -- The presence of a terminal error indicates this job has failed.\n" +
		"    terminal boolean NOT NULL DEFAULT false,\n" +
		"    serial bigserial NOT NULL,\n" +
		"    last_modified TIMESTAMPTZ NOT NULL\n" +
		");\n" +
		"\n" +
		"CREATE INDEX job_errors_id ON job_errors (job_id);\n" +
		"\n" +
		"-- There should ever only be one terminal error for any job.\n" +
		"-- We avoid creating the index as unique to avoid failing inserts on programming bugs.\n" +
		"CREATE INDEX job_errors_id_terminal ON job_errors (job_id, terminal);\n" +
		"\n" +
		"CREATE TABLE job_run_errors (\n" +
		"    -- To ensure inserts are idempotent, we need to asociate with each error a unique id\n" +
		"    -- that can be computed deterministically by the ingester.\n" +
		"    id integer PRIMARY KEY,\n" +
		"    run_id UUID NOT NULL,\n" +
		"    -- Byte array containing a JobRunErrors proto message.    \n" +
		"    error bytea NOT NULL,\n" +
		"    -- Indicates if this error is terminal.\n" +
		"    -- The presence of a terminal error indicates this job run has failed.\n" +
		"    terminal boolean NOT NULL DEFAULT false,\n" +
		"    serial bigserial NOT NULL,\n" +
		"    last_modified TIMESTAMPTZ NOT NULL\n" +
		");\n" +
		"\n" +
		"-- The combination topic name and partition index must be unique.\n" +
		"CREATE INDEX job_run_errors_id ON job_run_errors (run_id);\n" +
		"\n" +
		"-- There should ever only be one terminal error for any run.\n" +
		"-- We avoid creating the index as unique to avoid failing inserts on programming bugs.\n" +
		"CREATE INDEX job_run_errors_id_terminal ON job_run_errors (run_id, terminal);\n" +
		"\n" +
		"-- CREATE TABLE executors (\n" +
		"--     id text PRIMARY KEY,\n" +
		"--     -- Map from resource type to total amount available of that resource.\n" +
		"--     -- The following pairs are required: \"cpu\", \"memory\", \"storage\".\n" +
		"--     -- In addition, any accelerators (e.g., A100_16GB) must be included.\n" +
		"--     total_resources json NOT NULL,\n" +
		"--     -- Map from resource type to max amount of that resource available on any node.\n" +
		"--     -- Must contain a pair for each resource type in totalResources.\n" +
		"--     max_resources json NOT NULL\n" +
		"-- );\n" +
		"\n" +
		"CREATE TABLE nodeinfo (\n" +
		"    -- The concatenation of executor and node name.\n" +
		"    -- TODO: We need a unique primary key for the upsert logic. But we should do something smarter.\n" +
		"    executor_node_name text PRIMARY KEY,\n" +
		"    -- Name of the node. Must be unique across all clusters.\n" +
		"    node_name text NOT NULL,\n" +
		"    -- Name of the executor responsible for this node.\n" +
		"    executor text NOT NULL,\n" +
		"    -- Most recently received NodeInfo message for this node.\n" +
		"    message bytea NOT NULL,\n" +
		"    serial bigserial NOT NULL,\n" +
		"    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW()\n" +
		");\n" +
		"\n" +
		"-- The combination node name and executor must be unique.\n" +
		"CREATE UNIQUE INDEX node_name_executor ON nodeinfo (node_name, executor);\n" +
		"\n" +
		"-- Used for leader election.\n" +
		"-- Each replica regularly updates its last_modified entry.\n" +
		"-- If the last_modified entry of the current leader is older than some threshold,\n" +
		"-- another replica tries to become leader by updating the is_leader field in a transaction.\n" +
		"CREATE TABLE leaderelection (\n" +
		"    id UUID PRIMARY KEY,\n" +
		"    is_leader boolean NOT NULL,\n" +
		"    serial bigserial NOT NULL,\n" +
		"    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW()\n" +
		");\n" +
		"\n" +
		"-- Automatically increment serial and set last_modified on insert.\n" +
		"-- Because we upsert by inserting from a temporary table, this trigger handles both insert and update.\n" +
		"-- All new/updated rows can be queried by querying for all rows with serial larger than that of the most recent query.\n" +
		"--\n" +
		"-- Source:\n" +
		"-- https://dba.stackexchange.com/questions/294727/how-to-auto-increment-a-serial-column-on-update\n" +
		"CREATE OR REPLACE FUNCTION trg_increment_serial_set_last_modified()\n" +
		"  RETURNS trigger\n" +
		"  LANGUAGE plpgsql AS\n" +
		"$func$\n" +
		"BEGIN\n" +
		"  NEW.serial := nextval(CONCAT(TG_TABLE_SCHEMA, '.', TG_TABLE_NAME, '_serial_seq'));\n" +
		"  NEW.last_modified := NOW();\n" +
		"  RETURN NEW;\n" +
		"END\n" +
		"$func$;\n" +
		"\n" +
		"CREATE TRIGGER next_serial_on_insert_jobs\n" +
		"BEFORE INSERT or UPDATE ON jobs\n" +
		"FOR EACH ROW\n" +
		"EXECUTE FUNCTION trg_increment_serial_set_last_modified();\n" +
		"\n" +
		"CREATE TRIGGER next_serial_on_insert_runs\n" +
		"BEFORE INSERT or UPDATE ON runs\n" +
		"FOR EACH ROW\n" +
		"EXECUTE FUNCTION trg_increment_serial_set_last_modified();\n" +
		"\n" +
		"CREATE TRIGGER next_serial_on_insert_job_run_assignments\n" +
		"BEFORE INSERT or UPDATE ON job_run_assignments\n" +
		"FOR EACH ROW\n" +
		"EXECUTE FUNCTION trg_increment_serial_set_last_modified();\n" +
		"\n" +
		"CREATE TRIGGER next_serial_on_insert_job_errors\n" +
		"BEFORE INSERT or UPDATE ON job_errors\n" +
		"FOR EACH ROW\n" +
		"EXECUTE FUNCTION trg_increment_serial_set_last_modified();\n" +
		"\n" +
		"CREATE TRIGGER next_serial_on_insert_job_run_errors\n" +
		"BEFORE INSERT or UPDATE ON job_run_errors\n" +
		"FOR EACH ROW\n" +
		"EXECUTE FUNCTION trg_increment_serial_set_last_modified();\n" +
		"\n" +
		"CREATE TRIGGER next_serial_on_insert_nodeinfo\n" +
		"BEFORE INSERT or UPDATE ON nodeinfo\n" +
		"FOR EACH ROW\n" +
		"EXECUTE FUNCTION trg_increment_serial_set_last_modified();\n" +
		"\n" +
		"CREATE TRIGGER next_serial_on_insert_leaderelection\n" +
		"BEFORE INSERT or UPDATE ON leaderelection\n" +
		"FOR EACH ROW\n" +
		"EXECUTE FUNCTION trg_increment_serial_set_last_modified();\n" +
		"\n" +
		"-- Used to store Pulsar message ids for idempotency checks.\n" +
		"CREATE TABLE pulsar (\n" +
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
		"CREATE UNIQUE INDEX topic_partition ON pulsar (topic, partition_idx);"
	return tmpl
}
