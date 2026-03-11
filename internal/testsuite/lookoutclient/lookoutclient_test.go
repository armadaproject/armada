package lookoutclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchLastRunCategories(t *testing.T) {
	tests := map[string]struct {
		responses   []getJobsResponse
		expected    []string
		expectErr   bool
		errContains string
	}{
		"returns categories from last run": {
			responses: []getJobsResponse{{
				Jobs: []job{{
					JobID: "job-1",
					Runs: []jobRun{{
						RunID:       "run-1",
						FailureInfo: &failureInfo{Categories: []string{"oom", "infra_error"}},
					}},
				}},
			}},
			expected: []string{"oom", "infra_error"},
		},
		"retries when job has no runs then succeeds": {
			responses: []getJobsResponse{
				{Jobs: []job{{JobID: "job-1", Runs: nil}}},
				{Jobs: []job{{JobID: "job-1", Runs: []jobRun{{
					RunID:       "run-1",
					FailureInfo: &failureInfo{Categories: []string{"oom"}},
				}}}}},
			},
			expected: []string{"oom"},
		},
		"retries when no jobs then succeeds": {
			responses: []getJobsResponse{
				{Jobs: nil},
				{Jobs: []job{{JobID: "job-1", Runs: []jobRun{{
					RunID:       "run-1",
					FailureInfo: &failureInfo{Categories: []string{"user_error"}},
				}}}}},
			},
			expected: []string{"user_error"},
		},
		"returns nil categories when failureInfo has no categories": {
			responses: []getJobsResponse{{
				Jobs: []job{{
					JobID: "job-1",
					Runs: []jobRun{{
						RunID:       "run-1",
						FailureInfo: &failureInfo{},
					}},
				}},
			}},
			expected: nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			callCount := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				idx := callCount
				if idx >= len(tc.responses) {
					idx = len(tc.responses) - 1
				}
				callCount++
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(tc.responses[idx])
			}))
			defer server.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			categories, err := FetchLastRunCategories(ctx, server.URL, "job-1")
			if tc.expectErr {
				require.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, categories)
			}
		})
	}
}

func TestFetchLastRunCategories_5xxRetriesUntilTimeout(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := FetchLastRunCategories(ctx, server.URL, "job-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
	assert.Greater(t, callCount, 1, "should have retried on 5xx")
}

func TestFetchLastRunCategories_4xxFailsImmediately(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad request"))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := FetchLastRunCategories(ctx, server.URL, "job-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "400")
}

func TestFetchLastRunCategories_ConnectionRefusedRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Use a URL that will always refuse connections.
	_, err := FetchLastRunCategories(ctx, "http://localhost:1", "job-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}

func TestAssertCategories(t *testing.T) {
	tests := map[string]struct {
		actual             []string
		expectedCategories []string
		lookoutURL         string
		expectErr          bool
		errContains        string
	}{
		"all expected present": {
			actual:             []string{"oom", "infra_error"},
			expectedCategories: []string{"oom"},
			lookoutURL:         "set",
		},
		"missing category": {
			actual:             []string{"oom"},
			expectedCategories: []string{"oom", "cuda_error"},
			lookoutURL:         "set",
			expectErr:          true,
			errContains:        "cuda_error",
		},
		"empty expected is no-op": {
			expectedCategories: nil,
		},
		"no lookoutURL with expected categories": {
			expectedCategories: []string{"oom"},
			lookoutURL:         "",
			expectErr:          true,
			errContains:        "lookoutUrl is not configured",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var serverURL string
			if tc.lookoutURL == "set" {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					resp := getJobsResponse{
						Jobs: []job{{
							JobID: "job-1",
							Runs: []jobRun{{
								RunID:       "run-1",
								FailureInfo: &failureInfo{Categories: tc.actual},
							}},
						}},
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(resp)
				}))
				defer server.Close()
				serverURL = server.URL
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err := AssertCategories(ctx, serverURL, []string{"job-1"}, tc.expectedCategories)
			if tc.expectErr {
				require.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}
