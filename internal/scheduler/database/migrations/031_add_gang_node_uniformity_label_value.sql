-- Add gang_node_uniformity_label_value column to runs table
-- This stores the actual node uniformity label value (e.g., "rack-1") selected by the scheduler
-- during gang scheduling. This value is passed to the executor via job leases.
-- VARCHAR(63) enforces Kubernetes label value length limit (max 63 characters per K8s spec).
ALTER TABLE runs ADD COLUMN IF NOT EXISTS gang_node_uniformity_label_value VARCHAR(63) DEFAULT '';
