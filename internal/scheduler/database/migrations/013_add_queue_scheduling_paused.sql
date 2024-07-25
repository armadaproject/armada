ALTER TABLE queues ADD COLUMN scheduling_paused bool DEFAULT FALSE;
ALTER TABLE queues ADD COLUMN labels text[] DEFAULT array[]::text[];
