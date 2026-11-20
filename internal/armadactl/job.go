package armadactl

import (
	"fmt"
	"os"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/job"
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

// ListJobs prints the jobs that match args as a document with a jobs list holding the details of each job,
// without the job specs: use GetJob for that. Like the other get commands, it prints yaml unless json is asked for.
func (a *App) ListJobs(args job.ListArgs) error {
	jobs, err := a.Params.JobAPI.List(args)
	if err != nil {
		return errors.Errorf("error listing jobs: %s", err)
	}

	list := make([]*job.Job, len(jobs))
	for i, j := range jobs {
		list[i] = job.NewJob(j)
	}
	if err := a.emitDocument(client.ResourceKindJob, jobListBody{Jobs: list}); err != nil {
		return errors.Errorf("error writing jobs: %s", err)
	}

	// This notice is not part of the output, so it stays off stdout, which may be parsed.
	if len(jobs) == args.Take {
		fmt.Fprintf(os.Stderr, "Showing %d jobs, there may be more: use --skip and --take to see others\n", len(jobs))
	}
	return nil
}

// jobListBody is the body of a job list document. A list body is a mapping so that it follows the mapping
// header the document opens with.
type jobListBody struct {
	Jobs []*job.Job `json:"jobs"`
}
