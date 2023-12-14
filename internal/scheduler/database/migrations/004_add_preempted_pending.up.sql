ALTER TABLE runs ADD COLUMN preempted boolean NOT NULL DEFAULT false;
-- Set to true once the run is assigned to an executor and waiting to start running.
ALTER TABLE runs ADD COLUMN pending boolean NOT NULL DEFAULT false;
