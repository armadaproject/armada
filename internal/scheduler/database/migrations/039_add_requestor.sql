-- The (first) user who preempted this job
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS preempt_user varchar(512) NULL;

-- The (last) user who reprioritized this job
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS reprioritize_user varchar(512) NULL;
