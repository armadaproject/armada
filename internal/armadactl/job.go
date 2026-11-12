package armadactl

import (
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/pkg/client"
)

// GetJob prints the full details of a job, including its job spec and its runs. Like the other get commands,
// it prints yaml unless json is asked for.
func (a *App) GetJob(jobId string) error {
	j, err := a.Params.JobAPI.Get(jobId)
	if err != nil {
		return errors.Errorf("error getting job %s: %s", jobId, err)
	}
	if err := a.emitDocument(client.ResourceKindJob, j); err != nil {
		return errors.Errorf("error writing job %s: %s", jobId, err)
	}
	return nil
}
