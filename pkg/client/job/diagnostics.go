package job

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/pkg/client/httpapi"
)

// RunDetails is what Lookout has recorded about a run beyond its state: why it failed, what the executor reported
// about it, and why the scheduler ended it. What Lookout has nothing for is left out.
type RunDetails struct {
	Error                      string `json:"error,omitempty"`
	DebugMessage               string `json:"debugMessage,omitempty"`
	SchedulerTerminationReason string `json:"schedulerTerminationReason,omitempty"`
}

func (d RunDetails) empty() bool {
	return d == RunDetails{}
}

// addDiagnostics adds to the job why it did not succeed. Lookout records an error for the job itself only when the job
// was rejected (see handleJobErrors in lookoutingester): any other failure is recorded on a run. So a rejected job is
// asked for its error, and each run that did not succeed is asked about.
func addDiagnostics(api *httpapi.Client, job *Job) error {
	if job.State == "REJECTED" {
		message, err := optionalString(api, "/api/v1/jobError", map[string]any{"jobId": job.JobID}, "errorString")
		if err != nil {
			return err
		}
		job.Error = message
	}

	for _, run := range job.Runs {
		if run == nil || run.Run == nil || !runDidNotSucceed(run.JobRunState) {
			continue
		}
		request := map[string]any{"runId": run.RunID}
		var details RunDetails
		var err error
		if details.Error, err = optionalString(api, "/api/v1/jobRunError", request, "errorString"); err != nil {
			return err
		}
		// Lookout names the key of the debug message errorString as well.
		if details.DebugMessage, err = optionalString(api, "/api/v1/jobRunDebugMessage", request, "errorString"); err != nil {
			return err
		}
		if details.SchedulerTerminationReason, err = optionalString(api, "/api/v1/jobRunSchedulerTerminationReason", request, "schedulerTerminationReason"); err != nil {
			return err
		}
		if !details.empty() {
			run.RunDetails = &details
		}
	}
	return nil
}

// runDidNotSucceed reports whether a run is over without having succeeded, so that there may be something to say about
// why. A run that is on its way, or that succeeded, has nothing.
func runDidNotSucceed(state string) bool {
	switch state {
	case "RUN_SUCCEEDED", "RUN_PENDING", "RUN_RUNNING", "RUN_LEASED":
		return false
	}
	return true
}

// optionalString posts request to path and returns the string at key of the response. Lookout answers HTTP 400
// with a message ending in "not found" when it has nothing recorded, which is not a failure here: the result is "".
func optionalString(api *httpapi.Client, path string, request map[string]any, key string) (string, error) {
	var response map[string]string
	err := api.Post(path, request, &response)
	var status *httpapi.StatusError
	if errors.As(err, &status) && status.Code == http.StatusBadRequest && strings.Contains(status.Body, "not found") {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get %s request failed: %s", strings.TrimPrefix(path, "/api/v1/"), err)
	}
	return response[key], nil
}
