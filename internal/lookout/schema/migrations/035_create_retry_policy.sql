CREATE TABLE IF NOT EXISTS retry_policy
(
  name text NOT NULL PRIMARY KEY,
  definition bytea NOT NULL
);

-- Source of truth for a queue's attachments; the serialized queue definition
-- does not carry them. ordinal is the policy's position in the submitted list.
CREATE TABLE IF NOT EXISTS queue_retry_policy
(
  queue_name text NOT NULL,
  policy_name text NOT NULL,
  ordinal int NOT NULL,
  PRIMARY KEY (queue_name, policy_name),
  CONSTRAINT queue_retry_policy_queue_name_fkey
    FOREIGN KEY (queue_name) REFERENCES queue (name) ON DELETE CASCADE,
  -- Named explicitly because upsertQueue matches on this constraint name to
  -- turn a foreign key violation into ErrUnknownRetryPolicies.
  CONSTRAINT queue_retry_policy_policy_name_fkey
    FOREIGN KEY (policy_name) REFERENCES retry_policy (name) ON DELETE CASCADE
);

-- The primary key indexes policy_name only as a trailing column, so deleting a
-- policy would otherwise scan the table.
CREATE INDEX IF NOT EXISTS idx_queue_retry_policy_policy_name ON queue_retry_policy (policy_name);
