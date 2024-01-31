package armadactl

import (
	"fmt"
	"strings"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// Kube prints kubectl commands for querying the pods associated with a particular job identified by
// the given jobId, queueName, jobSetId, and podNumber.
func (a *App) Kube(jobId string, queueName string, jobSetId string, podNumber int, args []string) error {
	verb := strings.Join(args, " ")
	return client.WithEventClient(a.Params.ApiConnectionDetails, func(c api.EventClient) error {
		state := client.GetJobSetState(c, queueName, jobSetId, armadacontext.Background(), true, false, false)
		jobInfo := state.GetJobInfo(jobId)

		if jobInfo == nil {
			fmt.Fprintf(a.Out, "Could not find job %s.\n", jobId)
			return nil
		}

		if jobInfo.ClusterId == "" {
			fmt.Fprintf(a.Out, "Job %s has not been assigned to a cluster yet.\n", jobId)
			return nil
		}

		cmd := client.GetKubectlCommand(jobInfo.ClusterId, jobInfo.Job.Namespace, jobId, podNumber, verb)
		fmt.Fprintf(a.Out, "%s\n", cmd)
		return nil
	})
	return nil
}
