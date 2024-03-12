package validation

import (
	"fmt"

	"github.com/armadaproject/armada/pkg/api"
)

type queueValidator struct{}

func (p queueValidator) Validate(r *api.JobSubmitRequest) error {
	if len(r.Queue) == 0 {
		return fmt.Errorf("queue is a required field")
	}
	return nil
}
