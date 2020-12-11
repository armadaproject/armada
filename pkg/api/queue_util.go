package api

import v1 "k8s.io/api/core/v1"

func (m *Job) GetAllPodSpecs() []*v1.PodSpec {
	if m != nil {
		return m.PodSpecs
	}
	return []*v1.PodSpec{m.PodSpec}
}

func (m *JobSubmitRequestItem) GetAllPodSpecs() []*v1.PodSpec {
	if m != nil {
		return m.PodSpecs
	}
	return []*v1.PodSpec{m.PodSpec}
}
