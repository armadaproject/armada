-- The first stop request of the run, as a serialized proto message.
ALTER TABLE runs ADD COLUMN IF NOT EXISTS stop_request bytea NULL;
