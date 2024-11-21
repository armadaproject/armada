package armadactl

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/pkg/api"
)

func (a *App) cordonQueue(queueName string) error {
	if err := a.Params.QueueAPI.Cordon(queueName); err != nil {
		return fmt.Errorf("error updating queue %s: %s", queueName, err)
	}
	return nil
}

func (a *App) CordonQueues(queryArgs *QueueQueryArgs, dryRun bool) error {
	selectedQueues, err := a.getAllQueuesAsAPIQueue(queryArgs)
	if err != nil {
		return fmt.Errorf("error retrieving queues: %s", err)
	}

	if dryRun {
		fmt.Println("Cordoning the following queues: (DRY RUN)")
		slices.Apply(selectedQueues, func(q *api.Queue) { fmt.Println(q.Name) })
	} else {
		fmt.Println("Cordoning the following queues:")
		for _, q := range selectedQueues {
			err = a.cordonQueue(q.Name)
			if err != nil {
				return fmt.Errorf("Could not cordon queue %s: %s", q.Name, err)
			} else {
				fmt.Printf("%s cordoned\n", q.Name)
			}
		}
	}
	return nil
}

func (a *App) uncordonQueue(queueName string) error {
	if err := a.Params.QueueAPI.Uncordon(queueName); err != nil {
		return fmt.Errorf("error updating queue %s: %s", queueName, err)
	}
	return nil
}

func (a *App) UncordonQueues(queryArgs *QueueQueryArgs, dryRun bool) error {
	selectedQueues, err := a.getAllQueuesAsAPIQueue(queryArgs)
	if err != nil {
		return fmt.Errorf("error retrieving queues: %s", err)
	}

	if dryRun {
		fmt.Println("Uncordoning the following queues: (DRY RUN)")
		slices.Apply(selectedQueues, func(q *api.Queue) { fmt.Println(q.Name) })
	} else {
		fmt.Println("Uncordoning the following queues:")
		for _, q := range selectedQueues {
			err = a.uncordonQueue(q.Name)
			if err != nil {
				return fmt.Errorf("Could not uncordon queue %s: %s", q.Name, err)
			} else {
				fmt.Printf("%s uncordoned\n", q.Name)
			}
		}
	}

	return nil
}

func (a *App) UncordonExecutor(executor string) error {
	fmt.Println("Uncordoning the following executors:")
	if err := a.Params.ExecutorAPI.Uncordon(executor); err != nil {
		return fmt.Errorf("error uncordoning executor %s: %s", executor, err)
	} else {
		fmt.Printf("%s uncordoned\n", executor)
	}
	return nil
}

func (a *App) CordonExecutor(executor string, cordonReason string) error {
	fmt.Println("Cordoning the following executors:")
	if err := a.Params.ExecutorAPI.Cordon(executor, cordonReason); err != nil {
		return fmt.Errorf("error cordoning executor %s: %s", executor, err)
	} else {
		fmt.Printf("%s cordoned\n", executor)
	}
	return nil
}
