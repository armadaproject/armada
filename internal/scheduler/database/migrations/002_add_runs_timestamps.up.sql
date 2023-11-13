ALTER TABLE runs
  ADD COLUMN leased_timestamp timestamptz;
ALTER TABLE runs
  ADD COLUMN pending_timestamp timestamptz;
ALTER TABLE runs
  ADD COLUMN running_timestamp timestamptz;
ALTER TABLE runs
  ADD COLUMN terminated_timestamp timestamptz;
