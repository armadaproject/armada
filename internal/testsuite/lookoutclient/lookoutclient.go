package lookoutclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

type failureInfo struct {
	Categories []string `json:"categories,omitempty"`
}

type jobRun struct {
	RunID       string       `json:"runId"`
	FailureInfo *failureInfo `json:"failureInfo,omitempty"`
}

type job struct {
	JobID string   `json:"jobId"`
	Runs  []jobRun `json:"runs"`
}

type getJobsResponse struct {
	Jobs []job `json:"jobs"`
}

type getJobsRequest struct {
	Filters []filter `json:"filters"`
	Order   order    `json:"order"`
	Skip    int      `json:"skip"`
	Take    int      `json:"take"`
}

type filter struct {
	Field string `json:"field"`
	Value string `json:"value"`
	Match string `json:"match"`
}

type order struct {
	Field     string `json:"field"`
	Direction string `json:"direction"`
}

// FetchLastRunCategories queries the Lookout API for the given jobId and returns
// the error categories on its last run. It retries with backoff until the run
// has failureInfo or ctx is cancelled.
func FetchLastRunCategories(ctx context.Context, lookoutURL, jobID string) ([]string, error) {
	reqBody, err := json.Marshal(getJobsRequest{
		Filters: []filter{{Field: "jobId", Value: jobID, Match: "exact"}},
		Order:   order{Field: "submitted", Direction: "DESC"},
		Skip:    0,
		Take:    1,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/jobs", lookoutURL)
	interval := 500 * time.Millisecond
	maxInterval := 5 * time.Second

	for {
		categories, done, err := fetchCategories(ctx, url, reqBody)
		if err != nil {
			return nil, err
		}
		if done {
			return categories, nil
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("timed out waiting for failureInfo on job %s", jobID)
		case <-timer.C:
		}

		interval *= 2
		if interval > maxInterval {
			interval = maxInterval
		}
	}
}

// fetchCategories returns (categories, done, error).
// done=false means the data isn't available yet and the caller should retry.
func fetchCategories(ctx context.Context, url string, body []byte) ([]string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, false, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, false, err
		}
		// Treat transport-level errors (connection refused, etc.) as transient.
		return nil, false, nil
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 500 {
		// Treat server errors as transient; caller will retry with backoff.
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("lookout returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result getJobsResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, false, fmt.Errorf("failed to decode response: %w", err)
	}

	if len(result.Jobs) == 0 {
		// Job not yet visible in Lookout, retry.
		return nil, false, nil
	}

	runs := result.Jobs[0].Runs
	if len(runs) == 0 {
		// No runs yet, retry.
		return nil, false, nil
	}

	// Lookout returns runs ordered ascending by COALESCE(leased, pending),
	// so the last element is the most recently started run.
	lastRun := runs[len(runs)-1]
	if lastRun.FailureInfo == nil {
		// No failureInfo yet, retry.
		return nil, false, nil
	}

	return lastRun.FailureInfo.Categories, true, nil
}
