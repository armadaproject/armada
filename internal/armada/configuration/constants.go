package configuration

const (
	// GangIdAnnotation Jobs with equal value for this annotation make up a gang.
	// All jobs in a gang are guaranteed to be scheduled onto the same cluster at the same time.
	GangIdAnnotation = "armadaproject.io/gangId"
	// GangCardinalityAnnotation All jobs in a gang must specify the total number of jobs in the gang via this annotation.
	// The cardinality should be expressed as an integer, e.g., "3".
	GangCardinalityAnnotation = "armadaproject.io/gangCardinality"
	// Armada normally tries to re-schedule jobs for which a pod fails to start.
	// Pods for which this annotation has value true are not retried.
	// Instead, the job the pod is part of fails immediately.
	FailFastAnnotation = "armadaproject.io/failFast"
)

var ArmadaManagedAnnotations = []string{
	GangIdAnnotation,
	GangCardinalityAnnotation,
	FailFastAnnotation,
}
