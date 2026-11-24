package constants

const (
	// GangIdAnnotation maps to a unique id of the gang the job is part of; jobs with equal value make up a gang.
	// All jobs in a gang are scheduled at the same time. They are only guaranteed to be scheduled onto the same cluster
	// if the gang sets a node uniformity label whose value identifies the cluster; see GangNodeUniformityLabelAnnotation.
	GangIdAnnotation = "armadaproject.io/gangId"
	// GangCardinalityAnnotation All jobs in a gang must specify the total number of jobs in the gang via this annotation.
	// The cardinality should be expressed as a positive integer, e.g., "3".
	GangCardinalityAnnotation = "armadaproject.io/gangCardinality"
	// The jobs that make up a gang may be constrained to be scheduled across a set of uniform nodes.
	// Specifically, if provided, all gang jobs are scheduled onto nodes for which the value of the provided label is equal.
	// Used to ensure, e.g., that all gang jobs are scheduled onto the same cluster or rack.
	GangNodeUniformityLabelAnnotation = "armadaproject.io/gangNodeUniformityLabel"
	// GangFailurePolicyAnnotation controls what happens to the remaining jobs of a gang once one of them fails or is cancelled.
	// If unset, the remaining jobs are left untouched. All jobs in a gang must specify the same value.
	GangFailurePolicyAnnotation = "armadaproject.io/gangFailurePolicy"
	// GangFailurePolicyCancelGang is the GangFailurePolicyAnnotation value under which the scheduler cancels the
	// remaining jobs of a gang as soon as one of its jobs fails or is cancelled.
	GangFailurePolicyCancelGang = "CancelGang"
	// GangLeaderAnnotation, if set to true, marks the job as the leader of its gang.
	// When the leader succeeds, the scheduler cancels the remaining jobs of the gang.
	// Used for workloads where the other jobs only exist to serve the leader, e.g., the workers of an MPI job.
	// At most one job in a gang may be the leader.
	GangLeaderAnnotation = "armadaproject.io/gangLeader"
	// GangServiceAnnotation, if set to true on all jobs of a gang, requests a headless service shared by the gang.
	// It gives each job of the gang that sets a hostname in its pod spec a DNS name of the form
	// <hostname>.<service name>.<namespace>.svc, so that the jobs can reach each other by names known at submit time.
	// The gang is scheduled onto a single cluster. Must be enabled in the server configuration.
	GangServiceAnnotation = "armadaproject.io/gangService"
	// GangServiceNameLabel is set by the server, on jobs of a gang that requests a gang service, to the name of that
	// service. The service selects the pods of the gang by this label.
	GangServiceNameLabel = "armadaproject.io/gangServiceName"

	// internalEnvVarPrefix is the prefix for all Armada-injected environment variables
	internalEnvVarPrefix = "ARMADA_"

	// Environment variables injected into all jobs
	JobIdEnvVar    = internalEnvVarPrefix + "JOB_ID"
	JobRunIdEnvVar = internalEnvVarPrefix + "JOB_RUN_ID"
	QueueEnvVar    = internalEnvVarPrefix + "QUEUE"
	JobSetIdEnvVar = internalEnvVarPrefix + "JOB_SET_ID"

	// Additional environment variables for gang-scheduled jobs
	GangIdEnvVar                       = internalEnvVarPrefix + "GANG_ID"
	GangCardinalityEnvVar              = internalEnvVarPrefix + "GANG_CARDINALITY"
	GangNodeUniformityLabelNameEnvVar  = internalEnvVarPrefix + "GANG_NODE_UNIFORMITY_LABEL_NAME"
	GangNodeUniformityLabelValueEnvVar = internalEnvVarPrefix + "GANG_NODE_UNIFORMITY_LABEL_VALUE"
	GangServiceNameEnvVar              = internalEnvVarPrefix + "GANG_SERVICE_NAME"

	// GangNumJobsScheduledAnnotation is set by the scheduler and indicates how many gang jobs were scheduled.
	// FailFastAnnotation, if set to true, ensures Armada does not re-schedule jobs that fail to start.
	// Instead, the job the pod is part of fails immediately.
	JobPriceBand        = "armadaproject.io/priceBand"
	FailFastAnnotation  = "armadaproject.io/failFast"
	PoolAnnotation      = "armadaproject.io/pool"
	ReservationTaintKey = "armadaproject.io/reservation"

	// EventStreamPrefix is the Redis stream key prefix for Armada event streams.
	// Event stream keys follow the pattern: "Events:{queue}:{jobSetId}"
	EventStreamPrefix = "Events:"

	// ExternalJobUriAnnotation is the legacy annotation key for setting an external job URI.
	// Prefer the ExternalJobUri proto field on JobSubmitRequestItem / SubmitJob instead.
	ExternalJobUriAnnotation = "armadaproject.io/externalJobUri"
)

var schedulingAnnotations = map[string]bool{
	GangIdAnnotation:                  true,
	GangCardinalityAnnotation:         true,
	GangNodeUniformityLabelAnnotation: true,
	GangFailurePolicyAnnotation:       true,
	GangLeaderAnnotation:              true,
	FailFastAnnotation:                true,
	JobPriceBand:                      true,
}

func IsSchedulingAnnotation(annotation string) bool {
	_, ok := schedulingAnnotations[annotation]
	return ok
}

func SchedulingAnnotationCount() int {
	return len(schedulingAnnotations)
}
