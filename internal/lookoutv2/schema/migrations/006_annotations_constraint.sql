ALTER TABLE job ADD CONSTRAINT annotations_not_null CHECK (annotations IS NOT NULL) NOT VALID;
ALTER TABLE job VALIDATE CONSTRAINT annotations_not_null;
