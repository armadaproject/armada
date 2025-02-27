ALTER TABLE job_run ALTER pending DROP NOT NULL;
ALTER TABLE job_run ADD COLUMN leased timestamp NULL;
