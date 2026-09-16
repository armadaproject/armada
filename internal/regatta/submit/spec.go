// Package submit implements regatta's own minimal job-submission spec and runner. It
// deliberately does not reuse pkg/client/domain.LoadTestSpecification or
// client.ArmadaLoadTester.RunSubmissionTest - those belong to armada-load-tester's ramped,
// delayed, multi-submission-round load-testing model. Regatta only needs to submit a fixed
// number of jobs from a template and wait for them to reach a terminal state.
package submit

import (
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/pkg/client/util"
)

// Spec is regatta's submission spec: a queue, a job count, and a single pod template repeated
// Count times.
type Spec struct {
	Queue     string      `json:"queue"`
	JobSetId  string      `json:"jobSetId,omitempty"`
	Namespace string      `json:"namespace,omitempty"`
	Count     int         `json:"count"`
	Spec      *v1.PodSpec `json:"spec"`
}

// Load reads a Spec from a YAML/JSON file at path.
func Load(path string) (*Spec, error) {
	spec := &Spec{}
	if err := util.BindJsonOrYaml(path, spec); err != nil {
		return nil, err
	}
	return spec, nil
}
