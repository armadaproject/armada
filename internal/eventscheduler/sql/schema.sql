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
    topic text NOT NULL,
    ledger_id bigint NOT NULL,
    entry_id bigint NOT NULL,
    batch_idx int not NULL,
    partition_idx int NOT NULL
);
