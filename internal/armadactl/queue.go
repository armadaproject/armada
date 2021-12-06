package armadactl

import (
	"fmt"
	"sort"

	"github.com/G-Research/armada/pkg/api"
)

// CreateQueue calls app.QueueAPI.Create with the provided parameters.
func (a *App) CreateQueue(name string, priorityFactor float64, owners []string, groups []string, resourceLimits map[string]float64) error {
	queue := api.Queue{
		Name:           name,
		PriorityFactor: priorityFactor,
		UserOwners:     owners,
		GroupOwners:    groups,
		ResourceLimits: resourceLimits,
	}
	if !a.Params.DryRun {
		if err := a.Params.QueueAPI.Create(queue); err != nil {
			return fmt.Errorf("[armadactl.CreateQueue] error creating queue %s: %s", name, err)
		}
	}
	fmt.Fprintf(a.Out, "Created queue %s\n", name)
	return nil
}

// DeleteQueue calls app.QueueAPI.Delete with the provided parameters.
func (a *App) DeleteQueue(name string) error {
	if !a.Params.DryRun {
		if err := a.Params.QueueAPI.Delete(name); err != nil {
			return fmt.Errorf("[armadactl.DeleteQueue] error deleting queue %s: %s", name, err)
		}
	}
	fmt.Fprintf(a.Out, "Deleted queue %s (or it did not exist)\n", name)
	return nil
}

// DescribeQueue calls app.QueueAPI.Describe with the provided parameters.
func (a *App) DescribeQueue(name string) error {
	fmt.Fprintf(a.Out, "Queue: %s\n", name)
	queueInfo, err := a.Params.QueueAPI.GetInfo(name)
	if err != nil {
		return fmt.Errorf("[armadactl.DescribeQueue] error describing queue %s: %s", name, err)
	}

	jobSets := queueInfo.ActiveJobSets
	sort.SliceStable(jobSets, func(i, j int) bool {
		return jobSets[i].Name < jobSets[j].Name
	})

	if len(jobSets) == 0 {
		fmt.Fprintf(a.Out, "No queued or running jobs\n")
	} else {
		for _, jobSet := range jobSets {
			fmt.Fprintf(a.Out, "[job set: %s] Running: %d, Queued: %d\n", jobSet.Name, jobSet.LeasedJobs, jobSet.QueuedJobs)
		}
	}

	return nil
}

// UpdateQueue calls app.QueueAPI.Update with the provided parameters.
func (a *App) UpdateQueue(name string, priorityFactor float64, owners []string, groups []string, resourceLimits map[string]float64) error {
	queue := api.Queue{
		Name:           name,
		PriorityFactor: priorityFactor,
		UserOwners:     owners,
		GroupOwners:    groups,
		ResourceLimits: resourceLimits,
	}
	if !a.Params.DryRun {
		if err := a.Params.QueueAPI.Update(queue); err != nil {
			return fmt.Errorf("error updating queue %s: %s", name, err)
		}
	}

	fmt.Fprintf(a.Out, "Updated queue %s\n", name)
	return nil
}
