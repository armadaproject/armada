package configuration

// GangIdAnnotation Jobs with equal value for this annotation make up a gang.
// All jobs in a gang are guaranteed to be scheduled onto the same cluster at the same time.
const GangIdAnnotation = "armadaproject.io/gangId"

// GangCardinalityAnnotation All jobs in a gang must specify the total number of jobs in the gang via this annotation.
// The cardinality should be expressed as an integer, e.g., "3".
const GangCardinalityAnnotation = "armadaproject.io/gangCardinality"

var ArmadaManagedAnnotations = []string {
	GangIdAnnotation,
	GangCardinalityAnnotation,
}