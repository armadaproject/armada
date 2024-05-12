ALTER TABLE runs ADD COLUMN preempt_requested boolean NOT NULL DEFAULT false;
ALTER TABLE runs ADD COLUMN queue text;
UPDATE runs as jr SET queue = j.queue from jobs as j WHERE j.job_id = jr.job_id;
ALTER TABLE runs ALTER COLUMN queue SET NOT NULL;
CREATE INDEX idx_runs_queue_jobset ON runs (queue, job_set);
