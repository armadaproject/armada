package armadactl

import (
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/job"
)

// GetJobs prints the full details of one or more jobs, in the order they are given, including the job specs
// and the runs. Like the other get commands, it prints yaml unless json is asked for.
//
// Like kubectl, a single job is printed as a document of its own and several jobs are printed as a document
// with a jobs list. With several jobs, those Lookout does not know about are left out and reported in the
// error returned after the rest has been printed.
func (a *App) GetJobs(jobIds []string) error {
	jobIds = unique(jobIds)

	jobs, err := a.Params.JobAPI.Get(jobIds, true)
	if err != nil {
		return errors.Errorf("error getting jobs: %s", err)
	}
	byId := make(map[string]*job.Job, len(jobs))
	for _, j := range jobs {
		byId[j.JobID] = j
	}
	var missing []string
	for _, id := range jobIds {
		if _, ok := byId[id]; !ok {
			missing = append(missing, id)
		}
	}

	if len(jobIds) == 1 {
		if len(missing) > 0 {
			return errors.Errorf("job %s not found", missing[0])
		}
		if err := a.emitDocument(client.ResourceKindJob, jobs[0]); err != nil {
			return errors.Errorf("error writing job %s: %s", jobIds[0], err)
		}
		return nil
	}

	if err := a.emitDocument(client.ResourceKindJob, jobListBody{Jobs: jobs}); err != nil {
		return errors.Errorf("error writing jobs: %s", err)
	}
	if len(missing) > 0 {
		return errors.Errorf("jobs not found: %s", strings.Join(missing, ", "))
	}
	return nil
}

// ListJobs prints the jobs that match args as a document with a jobs list holding the details of each job,
// without the job specs: use GetJobs for those. Like the other get commands, it prints yaml unless json is asked for.
func (a *App) ListJobs(args job.ListArgs) error {
	jobs, err := a.Params.JobAPI.List(args)
	if err != nil {
		return errors.Errorf("error listing jobs: %s", err)
	}

	list := make([]*job.Job, len(jobs))
	for i, j := range jobs {
		list[i] = &job.Job{Job: j}
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

// unique returns ids without repeats, in the order they first appear.
func unique(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}
