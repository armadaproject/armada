-- The (last) user who reprioritised this job
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS reprioritise_user varchar(512) NULL;

-- The user who requested preemption of this run
ALTER TABLE runs ADD COLUMN IF NOT EXISTS preempt_user varchar(512) NULL;
