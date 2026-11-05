package job

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/lookout/gen/models"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/httpapi"
)

// Job is a job as Lookout reports it: its state, queue, resources, runs and so on, optionally with its job spec.
type Job struct {
	*models.Job
	JobSpec json.RawMessage `json:"jobSpec,omitempty"`
}

// GetAPI returns the given jobs from Lookout, in the order they were asked for. Jobs Lookout does not know about
// are left out. With withSpec the job spec of each job is fetched as well, which costs a request per job.
type GetAPI func(jobIds []string, withSpec bool) ([]*Job, error)

func Get(getConnectionDetails client.ConnectionDetails) GetAPI {
	return func(jobIds []string, withSpec bool) ([]*Job, error) {
		connectionDetails, err := getConnectionDetails()
		if err != nil {
			return nil, fmt.Errorf("failed to obtain api connection details: %s", err)
		}
		api, err := newClient(connectionDetails)
		if err != nil {
			return nil, err
		}

		jobs := make([]*Job, 0, len(jobIds))
		for _, id := range jobIds {
			job, err := getJob(api, id)
			if err != nil {
				return nil, err
			}
			if job == nil {
				continue
			}
			if withSpec {
				if job.JobSpec, err = getJobSpec(api, id); err != nil {
					return nil, err
				}
			}
			jobs = append(jobs, job)
		}
		return jobs, nil
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
	return &Job{Job: jobs[0]}, nil
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
