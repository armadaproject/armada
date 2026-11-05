package job

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/lookout/gen/models"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/httpapi"
)

// Job is a job as Lookout reports it: its state, queue, resources, runs and so on, with its job spec and, if it did not
// succeed, why. Make one with NewJob: the runs are replaced by ones that can carry what is recorded about them, so a
// Job made without it has no runs when it is written.
type Job struct {
	*models.Job
	// Runs replaces the runs of the job, which this has the same JSON key as, to add to each what Lookout has recorded.
	Runs    []*Run          `json:"runs"`
	JobSpec json.RawMessage `json:"jobSpec,omitempty"`
	// Error is why the job did not run, when Lookout has recorded one for the job itself. A job that ran has its
	// reasons on its runs.
	Error string `json:"error,omitempty"`
}

// Run is a run of a job as Lookout reports it, with what Lookout has recorded about why it did not succeed.
type Run struct {
	*models.Run
	RunDetails *RunDetails `json:"runDetails,omitempty"`
}

// NewJob makes a Job of a job Lookout reported.
func NewJob(job *models.Job) *Job {
	runs := make([]*Run, len(job.Runs))
	for i, run := range job.Runs {
		runs[i] = &Run{Run: run}
	}
	return &Job{Job: job, Runs: runs}
}

// GetAPI returns the job with the given id from Lookout, with its job spec and, if it did not succeed, why. It is an
// error if Lookout does not know the job.
type GetAPI func(jobId string) (*Job, error)

func Get(getConnectionDetails client.ConnectionDetails) GetAPI {
	return func(jobId string) (*Job, error) {
		connectionDetails, err := getConnectionDetails()
		if err != nil {
			return nil, fmt.Errorf("failed to obtain api connection details: %s", err)
		}
		api, err := newClient(connectionDetails)
		if err != nil {
			return nil, err
		}

		job, err := getJob(api, jobId)
		if err != nil {
			return nil, err
		}
		if job == nil {
			return nil, errors.New("not found")
		}
		if job.JobSpec, err = getJobSpec(api, jobId); err != nil {
			return nil, err
		}
		if err := addDiagnostics(api, job); err != nil {
			return nil, err
		}
		return job, nil
	}
}

// newClient makes a client for the Lookout API of the connection.
func newClient(connectionDetails *client.ApiConnectionDetails) (*httpapi.Client, error) {
	if connectionDetails.LookoutUrl == "" {
		return nil, errors.New("no Lookout url configured: set lookoutUrl in the context or pass --lookoutUrl")
	}
	return httpapi.New(connectionDetails, connectionDetails.LookoutUrl)
}

// getJob returns the job with the given id, or nil if Lookout does not know it.
func getJob(api *httpapi.Client, jobId string) (*Job, error) {
	// Lookout can only filter on a job id by an exact match, so jobs are fetched one at a time.
	jobs, err := getJobs(api, ListArgs{
		Filters: []*models.Filter{{Field: "jobId", Value: jobId, Match: models.FilterMatchExact}},
		Order:   models.Order{Field: "jobId", Direction: models.OrderDirectionASC},
		Take:    1,
	})
	if err != nil {
		return nil, fmt.Errorf("get job %s request failed: %s", jobId, err)
	}
	if len(jobs) == 0 {
		return nil, nil
	}
	return NewJob(jobs[0]), nil
}

func getJobSpec(api *httpapi.Client, jobId string) (json.RawMessage, error) {
	var response struct {
		Job json.RawMessage `json:"job"`
	}
	if err := api.Post("/api/v1/jobSpec", map[string]any{"jobId": jobId}, &response); err != nil {
		return nil, fmt.Errorf("get job spec %s request failed: %s", jobId, err)
	}
	return response.Job, nil
}
