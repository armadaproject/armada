ALTER TABLE job ADD COLUMN reason varchar(64) null;
ALTER TABLE job_run ADD COLUMN preempted timestamp null;
