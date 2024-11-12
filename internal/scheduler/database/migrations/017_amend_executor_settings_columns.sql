ALTER TABLE executor_settings
  ALTER COLUMN cordon_reason TYPE text USING (COALESCE(cordon_reason, ''))
  , ALTER COLUMN cordon_reason SET DEFAULT ''
  , ALTER COLUMN cordon_reason SET NOT NULL;

ALTER TABLE executor_settings
  ALTER COLUMN set_by_user TYPE text USING (COALESCE(set_by_user, ''))
  , ALTER COLUMN set_by_user SET DEFAULT ''
  , ALTER COLUMN set_by_user SET NOT NULL;

ALTER TABLE executor_settings
  ALTER COLUMN set_at_time TYPE timestamptz USING (COALESCE(set_at_time, '2024-11-12 09:00:00 UTC'::timestamptz))
  , ALTER COLUMN set_at_time SET DEFAULT NOW()
  , ALTER COLUMN set_at_time SET NOT NULL;
