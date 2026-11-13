package job

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/lookout/gen/models"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/httpapi"
)

// Job is a job as Lookout reports it: its state, queue, resources, runs and so on, with its job spec.
type Job struct {
	*models.Job
	JobSpec json.RawMessage `json:"jobSpec,omitempty"`
}

// NewJob makes a Job of a job Lookout reported.
func NewJob(job *models.Job) *Job {
	return &Job{Job: job}
}

// GetAPI returns the job with the given id from Lookout, with its job spec. It is an error if Lookout does not know the
// job.
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
	jobs, err := getJobs(api, []*models.Filter{{Field: "jobId", Value: jobId, Match: models.FilterMatchExact}}, 1)
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

// getJobs returns the first take jobs that match the filters, by job id. The result is never nil.
func getJobs(api *httpapi.Client, filters []*models.Filter, take int) ([]*models.Job, error) {
	request := map[string]any{
		// Lookout treats null filters as matching nothing, so this must be an empty list when there are none.
		"filters": append([]*models.Filter{}, filters...),
		"order":   models.Order{Field: "jobId", Direction: models.OrderDirectionASC},
		"skip":    0,
		"take":    take,
	}
	var response struct {
		Jobs []*models.Job `json:"jobs"`
	}
	if err := api.Post("/api/v1/jobs", request, &response); err != nil {
		return nil, err
	}
	if response.Jobs == nil {
		// Lookout sends null when nothing matches.
		response.Jobs = []*models.Job{}
	}
	return response.Jobs, nil
}
