package armadactl

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// Preempt a job.
func (a *App) Preempt(queue string, jobSetId string, jobId string, reason string) (outerErr error) {
	apiConnectionDetails := a.Params.ApiConnectionDetails

	fmt.Fprintf(a.Out, "Requesting preemption of job matching queue: %s, job set: %s, and job Id: %s\n", queue, jobSetId, jobId)
	return client.WithSubmitClient(apiConnectionDetails, func(c api.SubmitClient) error {
		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()

		_, err := c.PreemptJobs(ctx, &api.JobPreemptRequest{
			JobIds:   []string{jobId},
			JobSetId: jobSetId,
			Queue:    queue,
			Reason:   reason,
		})
		if err != nil {
			return errors.Wrapf(err, "error preempting job matching queue: %s, job set: %s, and job id: %s", queue, jobSetId, jobId)
		}

		fmt.Fprintf(a.Out, "Requested preemption for job %s\n", jobId)
		return nil
	})
}

func (a *App) PreemptOnExecutor(executor string, queues []string, priorityClasses []string) error {
	queueMsg := strings.Join(queues, ",")
	priorityClassesMsg := strings.Join(priorityClasses, ",")
	// If the provided slice of queues is empty, jobs on all queues will be cancelled
	if len(queues) == 0 {
		apiQueues, err := a.getAllQueuesAsAPIQueue(&QueueQueryArgs{})
		if err != nil {
			return fmt.Errorf("error preempting jobs on executor %s: %s", executor, err)
		}
		queues = armadaslices.Map(apiQueues, func(q *api.Queue) string { return q.Name })
		queueMsg = "all"
	}

	fmt.Fprintf(a.Out, "Requesting preemption of jobs matching executor: %s, queues: %s, priority-classes: %s\n", executor, queueMsg, priorityClassesMsg)
	if err := a.Params.ExecutorAPI.PreemptOnExecutor(executor, queues, priorityClasses); err != nil {
		return fmt.Errorf("error preempting jobs on executor %s: %s", executor, err)
	}
	return nil
}
