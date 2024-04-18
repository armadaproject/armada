ALTER TABLE job ADD CONSTRAINT job_annotations_not_null CHECK (annotations IS NOT NULL) NOT VALID;
ALTER TABLE job VALIDATE CONSTRAINT job_annotations_not_null;
