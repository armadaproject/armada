-- Add generated column for terminated status (cancelled OR succeeded OR failed)
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS terminated BOOLEAN GENERATED ALWAYS AS (cancelled OR succeeded OR failed) STORED;
