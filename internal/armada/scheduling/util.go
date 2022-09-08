package scheduling

import v1 "k8s.io/api/core/v1"

func hasPriorityClass(spec *v1.PodSpec) bool {
	return spec.PriorityClassName != ""
}
