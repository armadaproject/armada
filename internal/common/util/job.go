package util

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
)

const objectSeparator = "--"
const tolerationFieldSeparator = "+"
const sectionSeparator = "||"
const keySeparator = "##"
const prioritySeparator = "++"
const noValueString = "NIL"

func GetRequirementKey(job *api.Job) string {
	existingKeys := map[string]int{}

	if job.PodSpec != nil {
		specKey := getSpecKey(job.PodSpec)
		existingKeys[specKey]++
	}

	for _, spec := range job.PodSpecs {
		specKey := getSpecKey(spec)
		existingKeys[specKey]++
	}

	requestKeyValues := make([]string, 0, len(existingKeys))
	for key, value := range existingKeys {
		requestKeyValues = append(requestKeyValues, fmt.Sprintf("%s%s%s", key, sectionSeparator, strconv.Itoa(value)))
	}
	sort.Strings(requestKeyValues)
	// TODO Add quotes and escaping if required - for redis compatibility
	return strconv.FormatFloat(job.Priority, 'f', -1, 64) + prioritySeparator + strings.Join(requestKeyValues, keySeparator)
}

func getSpecKey(podSpec *v1.PodSpec) string {
	requirementsKey := getRequirementsKey(podSpec)
	nodeSelectorKey := getNodeSelectorKey(podSpec)
	tolerationsKey := getTolerationsKey(podSpec.Tolerations)
	affinityKey := getAffinityKey(podSpec)
	return requirementsKey + sectionSeparator + nodeSelectorKey + sectionSeparator + tolerationsKey + sectionSeparator + affinityKey
}

func getRequirementsKey(podSpec *v1.PodSpec) string {
	totalResourceRequest := common.TotalPodResourceRequest(podSpec)

	requestAsStringsMap := map[string]string{}
	for resourceName, quantity := range totalResourceRequest {
		requestAsStringsMap[resourceName] = removeDecimalZeros(quantity.AsDec().String())
	}

	return convertStringMapToKey(requestAsStringsMap)
}

func removeDecimalZeros(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	if strings.HasSuffix(s, ".") {
		s = s[0 : len(s)-1]
	}
	return s
}

func getRequirementsFromKey(key string) (v1.ResourceRequirements, error) {
	resourceRequirementsMap, err := convertStringToStringMap(key)
	if err != nil {
		return v1.ResourceRequirements{}, fmt.Errorf("unable to process resource requirements key because %s", err)
	}

	resourceList := v1.ResourceList{}

	for resourceName, quantityString := range resourceRequirementsMap {
		quantity, err := resource.ParseQuantity(quantityString)
		if err != nil {
			return v1.ResourceRequirements{}, fmt.Errorf("unable to process resource value %s because %s", quantityString, err)
		}
		resourceList[v1.ResourceName(resourceName)] = quantity
	}

	return v1.ResourceRequirements{
		Limits:   resourceList,
		Requests: resourceList,
	}, nil
}

func getTolerationsKey(tolerations []v1.Toleration) string {
	if len(tolerations) <= 0 {
		return noValueString
	}
	sort.Slice(tolerations, func(i, j int) bool {
		return tolerations[i].Key < tolerations[j].Key
	})

	tolerationKeys := make([]string, 0, len(tolerations))
	for _, toleration := range tolerations {
		tolerationKeys = append(tolerationKeys, getTolerationKey(toleration))
	}
	return strings.Join(tolerationKeys, objectSeparator)
}

func getTolerationKey(toleration v1.Toleration) string {
	return fmt.Sprintf("%s%s%s%s%s%s%s",
		toleration.Key, tolerationFieldSeparator,
		toleration.Value, tolerationFieldSeparator,
		toleration.Effect, tolerationFieldSeparator,
		toleration.Operator)
}

func getTolerationsFromKey(key string) ([]v1.Toleration, error) {
	if key == noValueString {
		return []v1.Toleration{}, nil
	}

	tolerationKeys := strings.Split(key, objectSeparator)
	tolerations := make([]v1.Toleration, 0, len(tolerationKeys))
	for _, tolerationKey := range tolerationKeys {
		toleration, err := getTolerationFromKey(tolerationKey)
		if err != nil {
			return []v1.Toleration{}, err
		}
		tolerations = append(tolerations, toleration)
	}
	return tolerations, nil
}

func getTolerationFromKey(key string) (v1.Toleration, error) {
	tolerationFields := strings.Split(key, tolerationFieldSeparator)
	if len(tolerationFields) != 4 {
		return v1.Toleration{}, fmt.Errorf("unable to convert key %s to toleration, due to it being in an unexpected format", key)
	}
	return v1.Toleration{
		Key:      tolerationFields[0],
		Value:    tolerationFields[1],
		Effect:   v1.TaintEffect(tolerationFields[2]),
		Operator: v1.TolerationOperator(tolerationFields[3]),
	}, nil

}

func getNodeSelectorKey(spec *v1.PodSpec) string {
	if len(spec.NodeSelector) <= 0 {
		return noValueString
	}
	return convertStringMapToKey(spec.NodeSelector)
}

func getNodeSelectorFromKey(key string) (map[string]string, error) {
	if key == noValueString {
		return map[string]string{}, nil
	}

	return convertStringToStringMap(key)
}

// TODO We could do some custom encoding of these object to make them more compact
func getAffinityKey(spec *v1.PodSpec) string {
	if spec.Affinity == nil {
		return noValueString
	}

	b, _ := proto.Marshal(spec.Affinity)
	return string(b)
}

func getAffinityFromKey(key string) (*v1.Affinity, error) {
	if key == noValueString {
		return nil, nil
	}

	var affinity v1.Affinity
	keyBytes := []byte(key)
	err := proto.Unmarshal(keyBytes, &affinity)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal node affinity %s because %s", key, err)
	}
	return &affinity, err
}

func GenerateJobRequirementsFromKey(key string) (*api.Job, error) {
	job := &api.Job{}
	sections := strings.Split(key, prioritySeparator)

	strconv.FormatFloat(job.Priority, 'f', -1, 64)
	if len(sections) != 2 {
		return nil, fmt.Errorf("Expected sections not found. Key should be formatted as <priority>%s<podSpecKeys>, instead found %s", prioritySeparator, key)
	}

	priorityString := sections[0]
	priority, err := strconv.ParseFloat(priorityString, 64)
	if err != nil {
		return nil, fmt.Errorf("Failed to extract priority from key. It should be a decimal instead found %s", priorityString)
	}
	job.Priority = priority

	podSpeckeys := sections[1]
	specKeys := strings.Split(podSpeckeys, keySeparator)
	for _, specKey := range specKeys {
		spec, count, err := generatePodSpecFromKey(specKey)
		if err != nil {
			return nil, err
		}
		for i := 0; i < count; i++ {
			job.PodSpecs = append(job.PodSpecs, spec)
		}
	}
	return job, nil
}

func generatePodSpecFromKey(key string) (*v1.PodSpec, int, error) {
	sections := strings.Split(key, sectionSeparator)
	if len(sections) != 5 {
		return nil, 0, fmt.Errorf("Unexpected number of key sections found, found %d, expected  %d", len(sections), 5)
	}

	spec := &v1.PodSpec{}

	requirementsKey := sections[0]
	requirements, err := getRequirementsFromKey(requirementsKey)
	if err != nil {
		return nil, 0, err
	}
	spec.Containers = []v1.Container{
		{
			Resources: requirements,
		},
	}

	nodeSelectorKey := sections[1]
	nodeSelector, err := getNodeSelectorFromKey(nodeSelectorKey)
	if err != nil {
		return nil, 0, err
	}
	spec.NodeSelector = nodeSelector

	tolerationsKey := sections[2]
	tolerations, err := getTolerationsFromKey(tolerationsKey)
	if err != nil {
		return nil, 0, err
	}
	spec.Tolerations = tolerations

	affinityKey := sections[3]
	affinity, err := getAffinityFromKey(affinityKey)
	if err != nil {
		return nil, 0, err
	}
	if affinity != nil {
		spec.Affinity = affinity
	}

	count, err := strconv.Atoi(sections[4])
	if err != nil {
		return nil, 0, fmt.Errorf("count section contained %s, expected a number", sections[4])
	}

	return spec, count, nil
}

func convertStringMapToKey(m map[string]string) string {
	keys := GetKeys(m)
	sort.Strings(keys)
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, fmt.Sprintf("%s=%s", key, m[key]))
	}

	return strings.Join(values, objectSeparator)
}

func convertStringToStringMap(key string) (map[string]string, error) {
	keyValueStrings := strings.Split(key, objectSeparator)

	result := make(map[string]string, len(keyValueStrings))

	for _, keyValueString := range keyValueStrings {
		r := strings.Split(keyValueString, "=")
		if len(r) != 2 {
			return map[string]string{}, fmt.Errorf("unable to process keyvalue string %s as it is in an unexpected format", keyValueString)
		}
		k, value := r[0], r[1]
		result[k] = value
	}

	return result, nil
}
