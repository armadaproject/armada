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
		"CREATE TABLE pulsar (\n" +
		"    topic text NOT NULL,\n" +
		"    ledger_id bigint NOT NULL,\n" +
		"    entry_id bigint NOT NULL,\n" +
		"    batch_idx int not NULL,\n" +
		"    partition_idx int NOT NULL\n" +
		");\n" +
		""
	return tmpl
}
