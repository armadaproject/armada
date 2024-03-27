package configuration

const (
	// GangIdAnnotation maps to a unique id of the gang the job is part of; jobs with equal value make up a gang.
	// All jobs in a gang are guaranteed to be scheduled onto the same cluster at the same time.
	GangIdAnnotation = "armadaproject.io/gangId"
	// GangCardinalityAnnotation All jobs in a gang must specify the total number of jobs in the gang via this annotation.
	// The cardinality should be expressed as a positive integer, e.g., "3".
	GangCardinalityAnnotation = "armadaproject.io/gangCardinality"
	// GangMinimumCardinalityAnnotation All jobs in a gang must specify the minimum size for the gang to be schedulable via this annotation.
	// The cardinality should be expressed as a positive integer, e.g., "3".
	GangMinimumCardinalityAnnotation = "armadaproject.io/gangMinimumCardinality"
	// The jobs that make up a gang may be constrained to be scheduled across a set of uniform nodes.
	// Specifically, if provided, all gang jobs are scheduled onto nodes for which the value of the provided label is equal.
	// Used to ensure, e.g., that all gang jobs are scheduled onto the same cluster or rack.
	GangNodeUniformityLabelAnnotation = "armadaproject.io/gangNodeUniformityLabel"
	// GangNumJobsScheduledAnnotation is set by the scheduler and indicates how many gang jobs were scheduled.
	// For example, a gang composed of 4 jobs may only have a subset be scheduled if  GangMinimumCardinalityAnnotation < 4.
	GangNumJobsScheduledAnnotation = "armadaproject.io/numGangJobsScheduled"
	// FailFastAnnotation, if set to true, ensures Armada does not re-schedule jobs that fail to start.
	// Instead, the job the pod is part of fails immediately.
	FailFastAnnotation = "armadaproject.io/failFast"
)

var schedulingAnnotations = map[string]bool{
	GangIdAnnotation:                  true,
	GangCardinalityAnnotation:         true,
	GangMinimumCardinalityAnnotation:  true,
	GangNodeUniformityLabelAnnotation: true,
	GangNumJobsScheduledAnnotation:    true,
	FailFastAnnotation:                true,
}

func IsSchedulingAnnotation(annotation string) bool {
	_, ok := schedulingAnnotations[annotation]
	return ok
}
