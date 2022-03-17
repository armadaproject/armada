CREATE TABLE queues (
    name text PRIMARY KEY,
    weight double precision NOT NULL
);

CREATE TABLE jobs (
    id text PRIMARY KEY,
    jobSetHash bit(256) NOT NULL,
    queue text NOT NULL,
    priority double precision NOT NULL,
    atMostOnce boolean NOT NULL,
    claims json NOT NULL -- Dict mapping resource type to requested resources
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

CREATE TABLE leases (
    id text PRIMARY KEY,
    job_id text NOT NULL,
    executor_id text NOT NULL,
    state text NOT NULL
);