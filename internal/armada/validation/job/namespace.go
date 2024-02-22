package job

import (
	"fmt"
	"github.com/armadaproject/armada/pkg/api"
)

type namespaceValidator struct{}

func (p namespaceValidator) Validate(j *api.JobSubmitRequestItem) error {
	if len(j.Namespace) == 0 {
		return fmt.Errorf("namespace is a required field")
	}
	return nil
}
