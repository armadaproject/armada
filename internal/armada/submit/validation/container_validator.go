package validation

import (
	"fmt"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/pkg/api"
)

type containerValidator struct {
	minJobResources v1.ResourceList
}

func (c containerValidator) Validate(j *api.JobSubmitRequestItem) error {
	spec := j.GetMainPodSpec()
	if spec == nil {
		return nil
	}

	if len(spec.Containers) == 0 {
		return fmt.Errorf("pod spec has no containers")
	}

	for _, container := range spec.Containers {

		if len(container.Resources.Requests) == 0 {
			return fmt.Errorf("container %v has no resource requests specified", container.Name)
		}

		if !resourceListEquals(container.Resources.Requests, container.Resources.Limits) {
			return fmt.Errorf("container %v does not have resource request and limit equal (this is currently not supported)", container.Name)
		}

		for rc, containerRsc := range container.Resources.Requests {
			serverRsc, nonEmpty := c.minJobResources[rc]
			if nonEmpty && containerRsc.Value() < serverRsc.Value() {
				return fmt.Errorf(
					"container %q %s requests (%s) below server minimum (%s)",
					container.Name,
					rc,
					&containerRsc,
					&serverRsc,
				)
			}
		}
	}
	return nil
}

func resourceListEquals(a v1.ResourceList, b v1.ResourceList) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if v != b[k] {
			return false
		}
	}
	return true
}
