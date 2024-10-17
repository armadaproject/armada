CREATE TABLE executor_settings (
  executor_id text PRIMARY KEY,
  cordoned boolean NOT NULL default false,
  cordon_reason text
);
