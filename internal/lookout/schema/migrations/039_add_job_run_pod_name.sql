-- The name of the pod of the run, as the executor reports it
ALTER TABLE job_run ADD COLUMN IF NOT EXISTS pod_name varchar(253) NULL;
