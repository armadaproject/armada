// Package metrics collects a post-run performance report from Prometheus. It queries once per report
// window, not continuously: regatta is a load generator, not a job-completion tracker (see
// internal/regatta/submit/run.go), and that extends to metrics collection too.
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
	"strings"
	"time"
)

// maxGetQueryLength is the longest expression sent as a GET query string; longer ones are POSTed.
const maxGetQueryLength = 2048

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
	params := url.Values{}
	params.Set("query", expr)
	params.Set("time", strconv.FormatInt(at.Unix(), 10))

	// A regex naming hundreds of queues would blow past the URL length limits of proxies and servers, so
	// long queries go as a form POST, which Prometheus's query endpoint accepts in the same shape.
	var req *http.Request
	if len(expr) > maxGetQueryLength {
		req, err = http.NewRequestWithContext(ctx, http.MethodPost, u.String(), strings.NewReader(params.Encode()))
		if err == nil {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	} else {
		u.RawQuery = params.Encode()
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	}
	if err != nil {
		return nil, fmt.Errorf("building request for %q: %w", truncateExpr(expr), err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("querying %q: %w", truncateExpr(expr), err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response for %q: %w", truncateExpr(expr), err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("querying %q: prometheus returned %s: %s", truncateExpr(expr), resp.Status, body)
	}

	var parsed promResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("decoding response for %q: %w", truncateExpr(expr), err)
	}
	if len(parsed.Data.Result) == 0 {
		return nil, nil
	}

	raw, ok := parsed.Data.Result[0].Value[1].(string)
	if !ok {
		return nil, fmt.Errorf("querying %q: unexpected value type %T", truncateExpr(expr), parsed.Data.Result[0].Value[1])
	}
	val, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(val) || math.IsInf(val, 0) {
		return nil, nil
	}
	return &val, nil
}
