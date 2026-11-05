package job

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	openapierrors "github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
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
		l, err := newLookout(connectionDetails, connectionDetails.AuthHeaders)
		if err != nil {
			return nil, err
		}

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
	baseUrl    string
	httpClient *http.Client
	// authHeaders authenticate every request. They are resolved once, when the client is made, because resolving
	// them can log in, which may be interactive, and a command makes several requests.
	authHeaders map[string]string
}

// newLookout makes a client for the Lookout of the connection. resolveAuthHeaders returns the headers that
// authenticate a request to a url, as ApiConnectionDetails.AuthHeaders does.
func newLookout(
	connectionDetails *client.ApiConnectionDetails,
	resolveAuthHeaders func(ctx context.Context, url string) (map[string]string, error),
) (*lookout, error) {
	if connectionDetails.LookoutUrl == "" {
		return nil, errors.New("no Lookout url configured: set lookoutUrl in the context or pass --lookoutUrl")
	}
	base := baseUrl(connectionDetails.LookoutUrl)

	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	headers, err := resolveAuthHeaders(ctx, base)
	if err != nil {
		return nil, err
	}
	return &lookout{
		baseUrl:     base,
		authHeaders: headers,
		httpClient:  &http.Client{CheckRedirect: checkRedirect(connectionDetails, len(headers) > 0)},
	}, nil
}

// checkRedirect stops a request that carries credentials from following a redirect to somewhere they must not go.
// The credentials were only checked against the url the request started at, and a redirect to the same host
// keeps them, so a Lookout that redirects from https to http would otherwise get them in clear text.
func checkRedirect(connectionDetails *client.ApiConnectionDetails, authenticated bool) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		if authenticated && !connectionDetails.AllowsCredentialsOver(req.URL.String()) {
			return fmt.Errorf("refusing to follow a redirect to %s: the credentials would be sent over plain http", req.URL.Redacted())
		}
		return nil
	}
}

// getJob returns the job with the given id, or nil if Lookout does not know it.
func (l *lookout) getJob(jobId string) (*Job, error) {
	// Lookout can only filter on a job id by an exact match, so jobs are fetched one at a time.
	jobs, err := l.getJobs(ListArgs{
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

// getJobs returns the jobs that match the args. The result is never nil.
func (l *lookout) getJobs(args ListArgs) ([]*models.Job, error) {
	request := map[string]any{
		"filters":       requestFilters(args.Filters),
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
	for k, v := range l.authHeaders {
		req.Header.Set(k, v)
	}

	resp, err := l.httpClient.Do(req)
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

// ListArgs is a request for Lookout's job listing. A job must satisfy every filter. The filters and the order are
// the types of Lookout's own API, so they have the shape and the validation it has.
type ListArgs struct {
	Filters []*models.Filter
	Order   models.Order
	// ActiveJobSets restricts the jobs to those in job sets that are still active.
	ActiveJobSets bool
	// Skip jobs before returning up to Take of them.
	Skip int
	Take int
}

// Validate checks the filters and the order against the rules Lookout checks a request against, so a mistake is
// reported before anything is sent, and before any login, which may be interactive.
func (a ListArgs) Validate() error {
	if err := ValidateFilters(a.Filters); err != nil {
		return err
	}
	return ValidateOrder(a.Order)
}

// ValidateFilters checks filters as Lookout does. The exists match only looks at the field, so it may leave the
// value out, as it is when the request is sent.
func ValidateFilters(filters []*models.Filter) error {
	for i, f := range requestFilters(filters) {
		if f == nil {
			return fmt.Errorf("filter %d is null", i+1)
		}
		if err := f.Validate(strfmt.Default); err != nil {
			return fmt.Errorf("filter %d: %s", i+1, validationMessage(err))
		}
	}
	return nil
}

// ValidateOrder checks an order as Lookout does.
func ValidateOrder(order models.Order) error {
	if err := order.Validate(strfmt.Default); err != nil {
		return errors.New(validationMessage(err))
	}
	return nil
}

// validationMessage is the message of a validation error on one line, with the reasons it lists.
func validationMessage(err error) string {
	var composite *openapierrors.CompositeError
	if !errors.As(err, &composite) || len(composite.Errors) == 0 {
		return err.Error()
	}
	reasons := make([]string, len(composite.Errors))
	for i, e := range composite.Errors {
		reasons[i] = e.Error()
	}
	return strings.Join(reasons, "; ")
}

// requestFilters returns the filters as they are sent: a copy, and never nil, because Lookout treats null filters
// as matching nothing. Lookout's request check wants every filter to have a value, though it ignores the value
// of an exists filter, which only looks at the field, so an empty one is given to an exists filter without one.
func requestFilters(filters []*models.Filter) []*models.Filter {
	out := make([]*models.Filter, len(filters))
	for i, f := range filters {
		if f != nil && f.Match == models.FilterMatchExists && f.Value == nil {
			withValue := *f
			withValue.Value = ""
			f = &withValue
		}
		out[i] = f
	}
	return out
}

// ListAPI returns the jobs that match the args, in order.
type ListAPI func(args ListArgs) ([]*models.Job, error)

func List(getConnectionDetails client.ConnectionDetails) ListAPI {
	return func(args ListArgs) ([]*models.Job, error) {
		if err := args.Validate(); err != nil {
			return nil, err
		}
		connectionDetails, err := getConnectionDetails()
		if err != nil {
			return nil, fmt.Errorf("failed to obtain api connection details: %s", err)
		}
		l, err := newLookout(connectionDetails, connectionDetails.AuthHeaders)
		if err != nil {
			return nil, err
		}

		jobs, err := l.getJobs(args)
		if err != nil {
			return nil, fmt.Errorf("list jobs request failed: %s", err)
		}
		return jobs, nil
	}
}
