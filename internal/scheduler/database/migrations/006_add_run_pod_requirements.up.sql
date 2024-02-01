-- The pod_requirements_overlay column holds a serialized PodRequirements
-- object that can be used to modify the pod requirements of a job; for
-- example, it is used to add additional tolerations to runs that are scheduled
-- as away jobs.
ALTER TABLE runs ADD COLUMN pod_requirements_overlay bytea;
