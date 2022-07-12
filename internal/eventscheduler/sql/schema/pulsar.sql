CREATE TABLE pulsar (
    topic text PRIMARY KEY,
    ledgerId bigint NOT NULL,
    entryId bigint NOT NULL,
    batchIdx int not NULL,
    partitionIdx int NOT NULL
);