package armadactl

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

// Kube calls kubectl with the verb strings.Join(args, " ") for the pod identified by (jobId, queueName, jobSetId)
func (a *App) Kube(jobId string, queueName string, jobSetId string, podNumber int, args []string) error {
	verb := strings.Join(args, " ")
	client.WithConnection(a.Params.ApiConnectionDetails, func(conn *grpc.ClientConn) {
		eventsClient := api.NewEventClient(conn)
		state := client.GetJobSetState(eventsClient, queueName, jobSetId, context.Background())
		jobInfo := state.GetJobInfo(jobId)

		if jobInfo == nil {
			fmt.Fprintf(a.Out, "Could not find job %s.\n", jobId)
			return
		}

		if jobInfo.ClusterId == "" {
			fmt.Fprintf(a.Out, "Job %s has not been assigned to a cluster yet.\n", jobId)
			return
		}

		cmd := client.GetKubectlCommand(jobInfo.ClusterId, jobInfo.Job.Namespace, jobId, podNumber, verb)
		fmt.Fprintf(a.Out, "%s\n", cmd)
	})
	return nil
}
