package configuration

const (
	// GangIdAnnotation is set by the user and is used to group gang jobs together,
	// such that jobs with equal value for this annotation make up a gang.
	// Jobs in a gang are scheduled and preempted atomically.
	GangIdAnnotation = "armadaproject.io/gangId"
	// GangUuidAnnotation is set for gang jobs by Armada on job submission.
	// The value of this annotation is used in place of GangIdAnnotation to group gang jobs together internally.
	// This ensures that the internal grouping is correct even if users re-use the same value for GangIdAnnotation.
	// It's an error to submit a job that has GangUuidAnnotation already set.
	GangUuidAnnotation = "armadaproject.io/gangUuid"
	// GangCardinalityAnnotation All jobs in a gang must specify the total number of jobs in the gang via this annotation.
	// The cardinality should be expressed as an integer, e.g., "3".
	GangCardinalityAnnotation = "armadaproject.io/gangCardinality"
	// Armada normally tries to re-schedule jobs for which a pod fails to start.
	// Pods for which this annotation has value "true" are not retried.
	// Instead, the job the pod is part of fails immediately.
	FailFastAnnotation = "armadaproject.io/failFast"
)

var ArmadaManagedAnnotations = []string{
	GangIdAnnotation,
	GangUuidAnnotation,
	GangCardinalityAnnotation,
	FailFastAnnotation,
}

var ArmadaInternalAnnotations = []string{
	GangUuidAnnotation,
}

var ReturnLeaseRequestTrackedAnnotations = map[string]struct{}{
	FailFastAnnotation: {},
}
