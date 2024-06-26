package armadactl

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

func (a *App) pauseScheduling(q *api.Queue) error {
	q.SchedulingPaused = true
	newQueue, err := queue.NewQueue(q)
	if err := a.Params.QueueAPI.Update(newQueue); err != nil {
		return fmt.Errorf("error updating queue %s: %s", q.Name, err)
	}
	return err
}

func (a *App) PauseScheduling(matchQueues []string, matchLabels []string, dryRun bool, inverse bool) error {
	selectedQueues, err := a.getAllQueuesAsAPIQueue(matchQueues, matchLabels, inverse)
	if err != nil {
		return fmt.Errorf("error retrieving queues: %s", err)
	}

	if dryRun {
		fmt.Printf("Pausing scheduling for the following queues: (DRY RUN)")
		slices.Apply(selectedQueues, func(q *api.Queue) { fmt.Println(q.Name) })
	} else {
		fmt.Println("Pausing scheduling for the following queues:")
		for _, q := range selectedQueues {
			err = a.pauseScheduling(q)
			if err != nil {
				return fmt.Errorf("Could not pause scheduling on queue %s: %s", q.Name, err)
			} else {
				fmt.Printf("%s paused\n", q.Name)
			}
		}
	}
	return nil
}

func (a *App) resumeScheduling(q *api.Queue) error {
	q.SchedulingPaused = false
	newQueue, err := queue.NewQueue(q)
	if err != nil {
		return fmt.Errorf("error creating new queue type for %s: %s", q.Name, err)
	}
	if err = a.Params.QueueAPI.Update(newQueue); err != nil {
		return fmt.Errorf("error updating queue %s: %s", q.Name, err)
	}
	return err
}

func (a *App) ResumeScheduling(matchQueues []string, matchLabels []string, dryRun bool, inverse bool) error {
	selectedQueues, err := a.getAllQueuesAsAPIQueue(matchQueues, matchLabels, inverse)
	if err != nil {
		return fmt.Errorf("error retrieving queues: %s", err)
	}

	if dryRun {
		fmt.Println("Resuming scheduling for the following queues: (DRY RUN)")
		slices.Apply(selectedQueues, func(q *api.Queue) { fmt.Println(q.Name) })
	} else {
		fmt.Println("Resuming scheduling for the following queues:")
		for _, q := range selectedQueues {
			err = a.resumeScheduling(q)
			if err != nil {
				return fmt.Errorf("Could not resume scheduling on queue %s: %s", q.Name, err)
			} else {
				fmt.Printf("%s resumed\n", q.Name)
			}
		}
	}

	return nil
}
