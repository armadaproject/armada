package dbloadtester

var getJobByID = `
SELECT
	 	 selected_jobs.job_id,
	 	 selected_jobs.queue,
	 	 selected_jobs.owner,
	 	 selected_jobs.namespace,
	 	 selected_jobs.jobset,
	 	 selected_jobs.cpu,
	 	 selected_jobs.memory,
	 	 selected_jobs.ephemeral_storage,
	 	 selected_jobs.gpu,
	 	 selected_jobs.priority,
	 	 selected_jobs.submitted,
	 	 selected_jobs.cancelled,
	 	 selected_jobs.state,
	 	 selected_jobs.last_transition_time,
	 	 selected_jobs.duplicate,
	 	 selected_jobs.priority_class,
	 	 selected_jobs.latest_run_id,
	 	 selected_jobs.cancel_reason,
	 	 selected_jobs.annotations,
	 	 selected_runs.runs
FROM (
	 	 	 	 	SELECT *
	 	 	 	 	FROM job AS j
	 	 	 	 	WHERE j.job_id = $1
	 	 	 	 	ORDER BY j.job_id DESC
	 	 	 	 	OFFSET 0 LIMIT 50
	 	 	) AS selected_jobs
	 	 	 	 	CROSS JOIN LATERAL (
	 	 SELECT
	 	 	 	 COALESCE(
	 	 	 	 	 	 	 	 	 	 	 	 json_agg(
	 	 	 	 	 	 	 	 	 	 	 	 json_strip_nulls(
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 json_build_object(
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'runId', run_id,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'cluster', cluster,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'node', node,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'leased', leased AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'pending', pending AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'started', started AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'finished', finished AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'jobRunState', job_run_state,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'exitCode', exit_code
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 )
	 	 	 	 	 	 	 	 	 	 	 	 )
	 	 	 	 	 	 	 	 	 	 	 	 ORDER BY COALESCE(leased, pending)
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 ) FILTER (WHERE run_id IS NOT NULL),
	 	 	 	 	 	 	 	 	 	 	 	 '[]'
	 	 	 	 ) AS runs
	 	 FROM job_run
	 	 WHERE job_id = selected_jobs.job_id
	 	 ) AS selected_runs
`

var getLookoutFrontPage = `
SELECT
	 	 selected_jobs.job_id,
	 	 selected_jobs.queue,
	 	 selected_jobs.owner,
	 	 selected_jobs.namespace,
	 	 selected_jobs.jobset,
	 	 selected_jobs.cpu,
	 	 selected_jobs.memory,
	 	 selected_jobs.ephemeral_storage,
	 	 selected_jobs.gpu,
	 	 selected_jobs.priority,
	 	 selected_jobs.submitted,
	 	 selected_jobs.cancelled,
	 	 selected_jobs.state,
	 	 selected_jobs.last_transition_time,
	 	 selected_jobs.duplicate,
	 	 selected_jobs.priority_class,
	 	 selected_jobs.latest_run_id,
	 	 selected_jobs.cancel_reason,
	 	 selected_jobs.annotations,
	 	 selected_runs.runs
FROM (
	 	 	 	 	SELECT *
	 	 	 	 	FROM job AS j
	 	 	 	 	ORDER BY j.submitted DESC
	 	 	 	 	OFFSET 0 LIMIT 100
	 	 	) AS selected_jobs
	 	 	 	 	CROSS JOIN LATERAL (
	 	 SELECT
	 	 	 	 COALESCE(
	 	 	 	 	 	 	 	 	 	 	 	 json_agg(
	 	 	 	 	 	 	 	 	 	 	 	 json_strip_nulls(
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 json_build_object(
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'runId', run_id,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'cluster', cluster,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'node', node,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'leased', leased AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'pending', pending AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'started', started AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'finished', finished AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'jobRunState', job_run_state,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'exitCode', exit_code
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 )
	 	 	 	 	 	 	 	 	 	 	 	 )
	 	 	 	 	 	 	 	 	 	 	 	 ORDER BY COALESCE(leased, pending)
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 ) FILTER (WHERE run_id IS NOT NULL),
	 	 	 	 	 	 	 	 	 	 	 	 '[]'
	 	 	 	 ) AS runs
	 	 FROM job_run
	 	 WHERE job_id = selected_jobs.job_id
	 	 ) AS selected_runs;
`

var getQueueActiveJobsets = `
SELECT j.jobset,
	 	 	 	COUNT(*) as count,
	 	 	 	SUM(CASE WHEN j.state = 1 THEN 1 ELSE 0 END) AS state_QUEUED,
	 	 	 	SUM(CASE WHEN j.state = 2 THEN 1 ELSE 0 END) AS state_PENDING,
	 	 	 	SUM(CASE WHEN j.state = 3 THEN 1 ELSE 0 END) AS state_RUNNING,
	 	 	 	SUM(CASE WHEN j.state = 4 THEN 1 ELSE 0 END) AS state_SUCCEEDED,
	 	 	 	SUM(CASE WHEN j.state = 5 THEN 1 ELSE 0 END) AS state_FAILED,
	 	 	 	SUM(CASE WHEN j.state = 6 THEN 1 ELSE 0 END) AS state_CANCELLED,
	 	 	 	SUM(CASE WHEN j.state = 7 THEN 1 ELSE 0 END) AS state_PREEMPTED,
	 	 	 	SUM(CASE WHEN j.state = 8 THEN 1 ELSE 0 END) AS state_LEASED,
	 	 	 	SUM(CASE WHEN j.state = 9 THEN 1 ELSE 0 END) AS state_REJECTED,
	 	 	 	MIN(j.submitted) AS submitted
FROM job AS j
	 	 	 	 	INNER JOIN (SELECT DISTINCT queue, jobset
	 	 	 	 	 	 	 	 	 	 	FROM job
	 	 	 	 	 	 	 	 	 	 	WHERE state IN (1, 2, 3, 8)) AS active_job_sets USING (queue, jobset)
WHERE queue = $1
GROUP BY j.jobset
ORDER BY submitted DESC
OFFSET 0;
`

var getQueueAllJobs = `
SELECT j.jobset,
	 	 	 	COUNT(*) as count,
	 	 	 	SUM(CASE WHEN j.state = 1 THEN 1 ELSE 0 END) AS state_QUEUED,
	 	 	 	SUM(CASE WHEN j.state = 2 THEN 1 ELSE 0 END) AS state_PENDING,
	 	 	 	SUM(CASE WHEN j.state = 3 THEN 1 ELSE 0 END) AS state_RUNNING,
	 	 	 	SUM(CASE WHEN j.state = 4 THEN 1 ELSE 0 END) AS state_SUCCEEDED,
	 	 	 	SUM(CASE WHEN j.state = 5 THEN 1 ELSE 0 END) AS state_FAILED,
	 	 	 	SUM(CASE WHEN j.state = 6 THEN 1 ELSE 0 END) AS state_CANCELLED,
	 	 	 	SUM(CASE WHEN j.state = 7 THEN 1 ELSE 0 END) AS state_PREEMPTED,
	 	 	 	SUM(CASE WHEN j.state = 8 THEN 1 ELSE 0 END) AS state_LEASED,
	 	 	 	SUM(CASE WHEN j.state = 9 THEN 1 ELSE 0 END) AS state_REJECTED,
	 	 	 	MIN(j.submitted) AS submitted
FROM job AS j
WHERE queue = $1
GROUP BY j.jobset
ORDER BY submitted DESC
OFFSET 0;
`
var getJobsetGroupedByState = `
SELECT j.state,
	 	 	 	COUNT(*) as count,
	 	 	 	MIN(j.submitted) AS submitted,
	 	 	 	AVG(j.last_transition_time_seconds) AS last_transition_time_seconds
FROM job AS j
WHERE j.jobset = $1
GROUP BY j.state
ORDER BY count DESC
OFFSET 0;
`

var getJobsRunningInQueue = `
SELECT
	 	 selected_jobs.job_id,
	 	 selected_jobs.queue,
	 	 selected_jobs.owner,
	 	 selected_jobs.namespace,
	 	 selected_jobs.jobset,
	 	 selected_jobs.cpu,
	 	 selected_jobs.memory,
	 	 selected_jobs.ephemeral_storage,
	 	 selected_jobs.gpu,
	 	 selected_jobs.priority,
	 	 selected_jobs.submitted,
	 	 selected_jobs.cancelled,
	 	 selected_jobs.state,
	 	 selected_jobs.last_transition_time,
	 	 selected_jobs.duplicate,
	 	 selected_jobs.priority_class,
	 	 selected_jobs.latest_run_id,
	 	 selected_jobs.cancel_reason,
	 	 selected_jobs.annotations,
	 	 selected_runs.runs
FROM (
	 	 	 	 	SELECT *
	 	 	 	 	FROM job AS j
	 	 	 	 	WHERE j.queue = $1 AND j.state = 3
	 	 	 	 	ORDER BY j.job_id DESC
	 	 	) AS selected_jobs
	 	 	 	 	CROSS JOIN LATERAL (
	 	 SELECT
	 	 	 	 COALESCE(
	 	 	 	 	 	 	 	 	 	 	 	 json_agg(
	 	 	 	 	 	 	 	 	 	 	 	 json_strip_nulls(
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 json_build_object(
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'runId', run_id,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'cluster', cluster,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'node', node,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'leased', leased AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'pending', pending AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'started', started AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'finished', finished AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'jobRunState', job_run_state,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'exitCode', exit_code
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 )
	 	 	 	 	 	 	 	 	 	 	 	 )
	 	 	 	 	 	 	 	 	 	 	 	 ORDER BY COALESCE(leased, pending)
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 ) FILTER (WHERE run_id IS NOT NULL),
	 	 	 	 	 	 	 	 	 	 	 	 '[]'
	 	 	 	 ) AS runs
	 	 FROM job_run
	 	 WHERE job_id = selected_jobs.job_id
	 	 ) AS selected_runs;
`

var getJobsRunningInQueueOrderBySubmitted = `
SELECT
	 	 selected_jobs.job_id,
	 	 selected_jobs.queue,
	 	 selected_jobs.owner,
	 	 selected_jobs.namespace,
	 	 selected_jobs.jobset,
	 	 selected_jobs.cpu,
	 	 selected_jobs.memory,
	 	 selected_jobs.ephemeral_storage,
	 	 selected_jobs.gpu,
	 	 selected_jobs.priority,
	 	 selected_jobs.submitted,
	 	 selected_jobs.cancelled,
	 	 selected_jobs.state,
	 	 selected_jobs.last_transition_time,
	 	 selected_jobs.duplicate,
	 	 selected_jobs.priority_class,
	 	 selected_jobs.latest_run_id,
	 	 selected_jobs.cancel_reason,
	 	 selected_jobs.annotations,
	 	 selected_runs.runs
FROM (
	 	 	 	 	SELECT *
	 	 	 	 	FROM job AS j
	 	 	 	 	WHERE j.queue = $1 AND j.state = 3
	 	 	 		ORDER BY j.submitted DESC LIMIT 100 OFFSET 0
	 	 	) AS selected_jobs
	 	 	 	 	CROSS JOIN LATERAL (
	 	 SELECT
	 	 	 	 COALESCE(
	 	 	 	 	 	 	 	 	 	 	 	 json_agg(
	 	 	 	 	 	 	 	 	 	 	 	 json_strip_nulls(
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 json_build_object(
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'runId', run_id,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'cluster', cluster,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'node', node,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'leased', leased AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'pending', pending AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'started', started AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'finished', finished AT TIME ZONE 'UTC',
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'jobRunState', job_run_state,
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 'exitCode', exit_code
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 )
	 	 	 	 	 	 	 	 	 	 	 	 )
	 	 	 	 	 	 	 	 	 	 	 	 ORDER BY COALESCE(leased, pending)
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 ) FILTER (WHERE run_id IS NOT NULL),
	 	 	 	 	 	 	 	 	 	 	 	 '[]'
	 	 	 	 ) AS runs
	 	 FROM job_run
	 	 WHERE job_id = selected_jobs.job_id
	 	 ) AS selected_runs;
`
