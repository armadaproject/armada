package scheduling

import v1 "k8s.io/api/core/v1"

func isPriorityJob(spec *v1.PodSpec) bool {
	return spec.PriorityClassName != ""
}
