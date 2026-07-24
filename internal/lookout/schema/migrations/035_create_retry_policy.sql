CREATE TABLE IF NOT EXISTS retry_policy
(
  name text NOT NULL PRIMARY KEY,
  definition bytea NOT NULL
)
