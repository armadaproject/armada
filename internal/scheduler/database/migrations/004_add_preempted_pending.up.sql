ALTER TABLE runs ADD COLUMN preempted boolean NOT NULL DEFAULT false;
-- Set to true once the run is sent to an executor, is assigned to a node, and is waiting to start running.
ALTER TABLE runs ADD COLUMN pending boolean NOT NULL DEFAULT false;
