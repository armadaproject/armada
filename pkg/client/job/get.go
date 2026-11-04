package job

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/lookout/gen/models"
	"github.com/armadaproject/armada/pkg/client"
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
		if connectionDetails.LookoutUrl == "" {
			return nil, errors.New("no Lookout url configured: set lookoutUrl in the context or pass --lookoutUrl")
		}
		l := &lookout{baseUrl: baseUrl(connectionDetails.LookoutUrl), connectionDetails: connectionDetails}

		jobs := make([]*Job, 0, len(jobIds))
		for _, id := range jobIds {
			job, err := l.getJob(id)
			if err != nil {
				return nil, err
			}
			if job == nil {
				continue
			}
			if withSpec {
				if job.JobSpec, err = l.getJobSpec(id); err != nil {
					return nil, err
				}
			}
			jobs = append(jobs, job)
		}
		return jobs, nil
	}
}

// baseUrl adds a scheme to a Lookout url that has none: http for a local Lookout, https otherwise.
func baseUrl(url string) string {
	url = strings.TrimRight(url, "/")
	if strings.Contains(url, "://") {
		return url
	}
	if client.IsLocalAddress(url) {
		return "http://" + url
	}
	return "https://" + url
}

type lookout struct {
	baseUrl           string
	connectionDetails *client.ApiConnectionDetails
}

// getJob returns the job with the given id, or nil if Lookout does not know it.
func (l *lookout) getJob(jobId string) (*Job, error) {
	// Lookout can only filter on a job id by an exact match, so jobs are fetched one at a time.
	jobs, err := l.getJobs(ListArgs{
		Filters: []Filter{{Field: "jobId", Value: jobId, Match: "exact"}},
		Order:   Order{Field: "jobId", Direction: "ASC"},
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

// getJobs returns the jobs that match the args. The result is never nil.
func (l *lookout) getJobs(args ListArgs) ([]*models.Job, error) {
	request := map[string]any{
		// Lookout treats null filters as matching nothing, so this must be an empty list when there are none.
		"filters":       append([]Filter{}, args.Filters...),
		"order":         args.Order,
		"skip":          args.Skip,
		"take":          args.Take,
		"activeJobSets": args.ActiveJobSets,
	}
	var response struct {
		Jobs []*models.Job `json:"jobs"`
	}
	if err := l.post("/api/v1/jobs", request, &response); err != nil {
		return nil, err
	}
	if response.Jobs == nil {
		// Lookout sends null when nothing matches.
		response.Jobs = []*models.Job{}
	}
	return response.Jobs, nil
}

func (l *lookout) getJobSpec(jobId string) (json.RawMessage, error) {
	var response struct {
		Job json.RawMessage `json:"job"`
	}
	if err := l.post("/api/v1/jobSpec", map[string]any{"jobId": jobId}, &response); err != nil {
		return nil, fmt.Errorf("get job spec %s request failed: %s", jobId, err)
	}
	return response.Job, nil
}

// post sends body as JSON to path, authenticated with the credentials of the connection, and decodes the JSON
// response into out.
func (l *lookout) post(path string, body, out any) error {
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()

	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	url := l.baseUrl + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if err := l.authenticate(ctx, req); err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("%s returned %s: %s", path, resp.Status, strings.TrimSpace(string(b)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (l *lookout) authenticate(ctx context.Context, req *http.Request) error {
	headers, err := l.connectionDetails.AuthHeaders(ctx, req.URL.String())
	if err != nil {
		return err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return nil
}

// Filter is a Lookout job filter. Which matches a field supports is decided by Lookout, which rejects the rest.
type Filter struct {
	// Field is a job field such as queue, state or cpu, or the name of an annotation when IsAnnotation is set.
	Field string `json:"field"`
	// Value is what to match. It is a list for the anyOf match, and is left out for the exists match.
	Value any `json:"value,omitempty"`
	// Match is one of exact, anyOf, startsWith, contains, greaterThan, lessThan, greaterThanOrEqualTo,
	// lessThanOrEqualTo and exists.
	Match        string `json:"match"`
	IsAnnotation bool   `json:"isAnnotation,omitempty"`
}

// Order is how Lookout orders jobs: by a field, in direction ASC or DESC.
type Order struct {
	Field     string `json:"field"`
	Direction string `json:"direction"`
}

// ListArgs is a request for Lookout's job listing. A job must satisfy every filter.
type ListArgs struct {
	Filters []Filter
	Order   Order
	// ActiveJobSets restricts the jobs to those in job sets that are still active.
	ActiveJobSets bool
	// Skip jobs before returning up to Take of them.
	Skip int
	Take int
}

// ListAPI returns the jobs that match the args, in order.
type ListAPI func(args ListArgs) ([]*models.Job, error)

func List(getConnectionDetails client.ConnectionDetails) ListAPI {
	return func(args ListArgs) ([]*models.Job, error) {
		connectionDetails, err := getConnectionDetails()
		if err != nil {
			return nil, fmt.Errorf("failed to obtain api connection details: %s", err)
		}
		if connectionDetails.LookoutUrl == "" {
			return nil, errors.New("no Lookout url configured: set lookoutUrl in the context or pass --lookoutUrl")
		}
		l := &lookout{baseUrl: baseUrl(connectionDetails.LookoutUrl), connectionDetails: connectionDetails}

		jobs, err := l.getJobs(args)
		if err != nil {
			return nil, fmt.Errorf("list jobs request failed: %s", err)
		}
		return jobs, nil
	}
}
