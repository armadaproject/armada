package jobspec

import "fmt"

const (
	PriorityValues       = 2000
	ClusterValues        = 40
	NodeValuesPerCluster = 80
)

var (
	NamespaceOptions = []string{
		"default",
		"dev",
		"staging",
		"research-red",
		"research-blue",
		"research-green",
		"research-yellow",
	}
	priorityClassOptions = []string{
		"armada-default",
		"armada-resilient",
		"armada-high",
		"armada-custom",
	}
	cpuOptions    = []int64{1, 2, 4, 8, 16, 32}
	memoryOptions = []int64{
		1024 * 1024 * 1024,
		4 * 1024 * 1024 * 1024,
		64 * 1024 * 1024 * 1024,
		1024 * 1024 * 1024 * 1024,
	}
	ephemeralStorageOptions = []int64{
		512 * 1024 * 1024,
		2 * 1024 * 1024 * 1024,
		10 * 1024 * 1024 * 1024,
		1024 * 1024 * 1024 * 1024,
		30 * 1024 * 1024 * 1024 * 1024,
	}
	gpuOptions  = []int64{0, 0, 0, 1, 0, 0, 0, 8}
	poolOptions = []string{
		"general-purpose",
		"high-memory",
		"high-cpu",
		"gpu",
	}
)

func CreateClusterName(nameIndex int) string {
	return fmt.Sprintf("broadside-cluster-%d", nameIndex)
}

func CreateNodeName(clusterNameIndex, nodeNameIndex int) string {
	return fmt.Sprintf("broadside-cluster-%d-node-%d", clusterNameIndex, nodeNameIndex)
}

func GetClusterNodeForJobNumber(jobNumber int) (string, string) {
	clusterIndex := jobNumber % ClusterValues
	nodeIndex := jobNumber % NodeValuesPerCluster
	return CreateClusterName(clusterIndex + 1), CreateNodeName(clusterIndex+1, nodeIndex+1)
}

type AnnotationConfig struct {
	Key             string
	MaxUniqueValues int
}

var AnnotationConfigs = []AnnotationConfig{
	{
		Key:             "example.com/broadside-alpha",
		MaxUniqueValues: 10,
	},
	{
		Key:             "example.com/broadside-beta",
		MaxUniqueValues: 100,
	},
	{
		Key:             "example.com/broadside-charlie",
		MaxUniqueValues: 1000,
	},
	{
		Key:             "example.com/broadside-delta",
		MaxUniqueValues: 10000,
	},
	{
		Key:             "example.com/broadside-echo",
		MaxUniqueValues: 100000,
	},
}

func CreateAnnotationValue(valueIndex int) string {
	return fmt.Sprintf("value-%d", valueIndex)
}

func GenerateAnnotationsForJob(jobNumber int) map[string]string {
	annotations := make(map[string]string, len(AnnotationConfigs))
	for _, annotationConfig := range AnnotationConfigs {
		annotations[annotationConfig.Key] = CreateAnnotationValue(jobNumber % annotationConfig.MaxUniqueValues)
	}
	return annotations
}

func GetNamespace(jobNumber int) string {
	return NamespaceOptions[jobNumber%len(NamespaceOptions)]
}

func GetPriorityClass(jobNumber int) string {
	return priorityClassOptions[jobNumber%len(priorityClassOptions)]
}

func GetCpu(jobNumber int) int64 {
	return cpuOptions[jobNumber%len(cpuOptions)]
}

func GetMemory(jobNumber int) int64 {
	return memoryOptions[jobNumber%len(memoryOptions)]
}

func GetEphemeralStorage(jobNumber int) int64 {
	return ephemeralStorageOptions[jobNumber%len(ephemeralStorageOptions)]
}

func GetGpu(jobNumber int) int64 {
	return gpuOptions[jobNumber%len(gpuOptions)]
}

func GetPool(jobNumber int) string {
	return poolOptions[jobNumber%len(poolOptions)]
}
