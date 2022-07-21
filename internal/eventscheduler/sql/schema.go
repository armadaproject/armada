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
		"    jobId UUID PRIMARY KEY,\n" +
		"    jobSetHash bit(256) NOT NULL,\n" +
		"    queue text NOT NULL,\n" +
		"    priority double precision NOT NULL,\n" +
		"     -- Dict mapping resource type to amount requested.\n" +
		"    claims json NOT NULL\n" +
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
		"    totalResources json NOT NULL,\n" +
		"    -- Map from resource type to max amount of that resource available on any node.\n" +
		"    -- Must contain a pair for each resource type in totalResources.\n" +
		"    maxResources json NOT NULL\n" +
		");\n" +
		"\n" +
		"CREATE TABLE pulsar (\n" +
		"    topic text PRIMARY KEY,\n" +
		"    ledgerId bigint NOT NULL,\n" +
		"    entryId bigint NOT NULL,\n" +
		"    batchIdx int not NULL,\n" +
		"    partitionIdx int NOT NULL\n" +
		");\n" +
		""
	return tmpl
}
