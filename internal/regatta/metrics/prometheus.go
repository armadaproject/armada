// Package metrics collects a post-run performance report from Prometheus, porting the query set
// and windowing model already used in CI against a live Armada deployment
// (armada-cd-dev/applications/hpcx-dev-wl-01/performance-suite/scripts/collect-perf-metrics.sh) so
// a local `regatta run` produces the same kind of report. This is a single end-of-run snapshot,
// not a continuous poller - regatta is a load generator, not a job-completion tracker (see
// internal/regatta/submit/run.go), and that principle extends to metrics collection too.
package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type promResponse struct {
	Data struct {
		Result []struct {
			Value [2]interface{} `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// query runs a PromQL instant query against baseURL at time at, returning nil if the query
// returned no result or a non-finite value (NaN/+Inf/-Inf) - Prometheus returns these for
// legitimate reasons (e.g. no samples in range yet), and a missing/unavailable metric should
// show up as an omitted field in the report, not fail the whole collection.
func query(ctx context.Context, baseURL, expr string, at time.Time) (*float64, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parsing prometheus url %q: %w", baseURL, err)
	}
	u.Path = u.Path + "/api/v1/query"
	q := u.Query()
	q.Set("query", expr)
	q.Set("time", strconv.FormatInt(at.Unix(), 10))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("building request for %q: %w", expr, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("querying %q: %w", expr, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response for %q: %w", expr, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("querying %q: prometheus returned %s: %s", expr, resp.Status, body)
	}

	var parsed promResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("decoding response for %q: %w", expr, err)
	}
	if len(parsed.Data.Result) == 0 {
		return nil, nil
	}

	raw, ok := parsed.Data.Result[0].Value[1].(string)
	if !ok {
		return nil, fmt.Errorf("querying %q: unexpected value type %T", expr, parsed.Data.Result[0].Value[1])
	}
	val, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(val) || math.IsInf(val, 0) {
		return nil, nil
	}
	return &val, nil
}
