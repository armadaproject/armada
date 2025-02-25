-- The (first) user who cancelled this job
ALTER TABLE jobs ADD COLUMN cancel_user varchar(512) NULL;