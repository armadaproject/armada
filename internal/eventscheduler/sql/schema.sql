CREATE TABLE queues (
    name text PRIMARY KEY,
    weight double precision NOT NULL
);

CREATE TABLE jobs (
    jobId UUID PRIMARY KEY,
    jobSetHash bit(256) NOT NULL,
    queue text NOT NULL,
    priority double precision NOT NULL,
     -- Dict mapping resource type to amount requested.
    claims json NOT NULL
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
    totalResources json NOT NULL,
    -- Map from resource type to max amount of that resource available on any node.
    -- Must contain a pair for each resource type in totalResources.
    maxResources json NOT NULL
);

CREATE TABLE pulsar (
    topic text NOT NULL,
    ledgerId bigint NOT NULL,
    entryId bigint NOT NULL,
    batchIdx int not NULL,
    partitionIdx int NOT NULL
);
