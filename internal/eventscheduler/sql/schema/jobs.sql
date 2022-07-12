CREATE TABLE jobs (
    jobId UUID PRIMARY KEY,
    jobSetHash bit(256) NOT NULL,
    queue text NOT NULL,
    priority double precision NOT NULL,
     -- Dict mapping resource type to amount requested.
    claims json NOT NULL
);