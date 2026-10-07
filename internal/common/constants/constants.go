package constants

import "strings"

const (
	// GangIdAnnotation maps to a unique id of the gang the job is part of; jobs with equal value make up a gang.
	// All jobs in a gang are guaranteed to be scheduled onto the same cluster at the same time.
	GangIdAnnotation = "armadaproject.io/gangId"
	// GangCardinalityAnnotation All jobs in a gang must specify the total number of jobs in the gang via this annotation.
	// The cardinality should be expressed as a positive integer, e.g., "3".
	GangCardinalityAnnotation = "armadaproject.io/gangCardinality"
	// The jobs that make up a gang may be constrained to be scheduled across a set of uniform nodes.
	// Specifically, if provided, all gang jobs are scheduled onto nodes for which the value of the provided label is equal.
	// Used to ensure, e.g., that all gang jobs are scheduled onto the same cluster or rack.
	GangNodeUniformityLabelAnnotation = "armadaproject.io/gangNodeUniformityLabel"

	// internalEnvVarPrefix is the prefix for all Armada-injected environment variables
	internalEnvVarPrefix = "ARMADA_"

	// Environment variables injected into all jobs
	JobIdEnvVar    = internalEnvVarPrefix + "JOB_ID"
	QueueEnvVar    = internalEnvVarPrefix + "QUEUE"
	JobSetIdEnvVar = internalEnvVarPrefix + "JOB_SET_ID"

	// Additional environment variables for gang-scheduled jobs
	GangIdEnvVar                       = internalEnvVarPrefix + "GANG_ID"
	GangCardinalityEnvVar              = internalEnvVarPrefix + "GANG_CARDINALITY"
	GangNodeUniformityLabelNameEnvVar  = internalEnvVarPrefix + "GANG_NODE_UNIFORMITY_LABEL_NAME"
	GangNodeUniformityLabelValueEnvVar = internalEnvVarPrefix + "GANG_NODE_UNIFORMITY_LABEL_VALUE"

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

	// RetryPoliciesAnnotation lists the retry policies of a job in order, separated by commas. For the job, the list
	// replaces the retry policies of its queue. Each policy must exist, but it does not need to be attached to the queue.
	RetryPoliciesAnnotation = "armadaproject.io/retryPolicies"
)

var schedulingAnnotations = map[string]bool{
	GangIdAnnotation:                  true,
	GangCardinalityAnnotation:         true,
	GangNodeUniformityLabelAnnotation: true,
	FailFastAnnotation:                true,
	JobPriceBand:                      true,
	RetryPoliciesAnnotation:           true,
}

func IsSchedulingAnnotation(annotation string) bool {
	_, ok := schedulingAnnotations[annotation]
	return ok
}

func SchedulingAnnotationCount() int {
	return len(schedulingAnnotations)
}

// RetryPolicyNames returns the policy names in the retry policies annotation, in order and without spaces around them.
// It returns nil when the annotation is not set.
func RetryPolicyNames(annotations map[string]string) []string {
	value, ok := annotations[RetryPoliciesAnnotation]
	if !ok {
		return nil
	}
	parts := strings.Split(value, ",")
	names := make([]string, len(parts))
	for i, part := range parts {
		names[i] = strings.TrimSpace(part)
	}
	return names
}
