package actions

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/armadaproject/armada/internal/broadside/configuration"
	"github.com/armadaproject/armada/internal/broadside/db"
	"github.com/armadaproject/armada/internal/broadside/jobspec"
	"github.com/armadaproject/armada/internal/broadside/metrics"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/lookout/model"
)

// Actor executes scheduled actions against the database to simulate bulk
// operations performed by users via the Lookout UI.
type Actor struct {
	config       configuration.ActionsConfig
	queueConfigs []configuration.QueueConfig
	database     db.Database
	metrics      *metrics.ActorMetrics
	testStart    time.Time
}

// scheduledAction represents an action scheduled to execute at a specific time.
type scheduledAction struct {
	scheduledTime time.Duration
	actionType    string // "reprioritise" or "cancel"
	queue         string
	jobSet        string
}

// NewActor creates a new Actor with the given configuration.
func NewActor(
	config configuration.ActionsConfig,
	queueConfigs []configuration.QueueConfig,
	database db.Database,
	metrics *metrics.ActorMetrics,
	testStart time.Time,
) *Actor {
	return &Actor{
		config:       config,
		queueConfigs: queueConfigs,
		database:     database,
		metrics:      metrics,
		testStart:    testStart,
	}
}

// Run starts the actor and executes scheduled actions. It blocks until the
// context is cancelled or all actions have been executed.
func (a *Actor) Run(ctx context.Context) {
	scheduledActions := a.buildActionSchedule()

	if len(scheduledActions) == 0 {
		logging.Info("No actions configured, actor will not run")
		return
	}

	logging.Infof("Actor scheduled %d actions", len(scheduledActions))

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	actionIndex := 0

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			now := time.Since(a.testStart)

			// Execute all actions that are due
			for actionIndex < len(scheduledActions) {
				action := scheduledActions[actionIndex]
				if action.scheduledTime > now {
					break
				}

				a.executeAction(ctx, action)
				actionIndex++
			}

			// If all actions complete, exit
			if actionIndex >= len(scheduledActions) {
				logging.Info("All actions completed")
				return
			}
		}
	}
}

// buildActionSchedule combines and sorts all actions by scheduled time.
func (a *Actor) buildActionSchedule() []scheduledAction {
	var actions []scheduledAction

	for _, reprioritisation := range a.config.JobSetReprioritisations {
		actions = append(actions, scheduledAction{
			scheduledTime: reprioritisation.PerformAfterTestStartTime,
			actionType:    "reprioritise",
			queue:         reprioritisation.Queue,
			jobSet:        reprioritisation.JobSet,
		})
	}

	for _, cancellation := range a.config.JobSetCancellations {
		actions = append(actions, scheduledAction{
			scheduledTime: cancellation.PerformAfterTestStartTime,
			actionType:    "cancel",
			queue:         cancellation.Queue,
			jobSet:        cancellation.JobSet,
		})
	}

	// Sort by scheduled time
	sort.Slice(actions, func(i, j int) bool {
		return actions[i].scheduledTime < actions[j].scheduledTime
	})

	return actions
}

// executeAction executes a single action.
func (a *Actor) executeAction(ctx context.Context, action scheduledAction) {
	logging.WithFields(map[string]any{
		"action": action.actionType,
		"queue":  action.queue,
		"jobSet": action.jobSet,
	}).Info("Executing action")

	switch action.actionType {
	case "reprioritise":
		a.executeReprioritisation(ctx, action.queue, action.jobSet)
	case "cancel":
		a.executeCancellation(ctx, action.queue, action.jobSet)
	default:
		logging.Warnf("Unknown action type: %s", action.actionType)
	}
}

// executeReprioritisation reprioritises all active jobs in the specified queue and job set.
func (a *Actor) executeReprioritisation(ctx context.Context, queue string, jobSet string) {
	start := time.Now()

	// Query for active jobs
	jobs, err := a.getActiveJobs(ctx, queue, jobSet)
	if err != nil {
		logging.WithError(err).WithFields(map[string]any{
			"queue":  queue,
			"jobSet": jobSet,
		}).Warn("Failed to query jobs for reprioritisation")
		a.metrics.RecordReprioritisation(time.Since(start), 0, err)
		return
	}

	if len(jobs) == 0 {
		logging.WithFields(map[string]any{
			"queue":  queue,
			"jobSet": jobSet,
		}).Warn("Action skipped: No active jobs found for reprioritisation")
		a.metrics.RecordReprioritisation(time.Since(start), 0, nil)
		return
	}

	// Generate reprioritisation queries
	queries := make([]db.IngestionQuery, 0, len(jobs))
	for _, job := range jobs {
		queries = append(queries, db.UpdateJobPriority{
			JobID:    job.JobID,
			Priority: jobspec.PriorityValues,
		})
	}

	// Execute batch
	err = a.database.ExecuteIngestionQueryBatch(ctx, queries)
	duration := time.Since(start)

	if err != nil {
		logging.WithError(err).WithFields(map[string]any{
			"queue":    queue,
			"jobSet":   jobSet,
			"jobCount": len(jobs),
		}).Warn("Failed to execute reprioritisation batch")
	} else {
		logging.WithFields(map[string]any{
			"queue":    queue,
			"jobSet":   jobSet,
			"jobCount": len(jobs),
			"duration": duration,
		}).Info("Reprioritisation completed")
	}

	a.metrics.RecordReprioritisation(duration, len(jobs), err)
}

// executeCancellation cancels all active jobs in the specified queue and job set.
func (a *Actor) executeCancellation(ctx context.Context, queue string, jobSet string) {
	start := time.Now()

	// Query for active jobs
	jobs, err := a.getActiveJobs(ctx, queue, jobSet)
	if err != nil {
		logging.WithError(err).WithFields(map[string]any{
			"queue":  queue,
			"jobSet": jobSet,
		}).Warn("Failed to query jobs for cancellation")
		a.metrics.RecordCancellation(time.Since(start), 0, err)
		return
	}

	if len(jobs) == 0 {
		logging.WithFields(map[string]any{
			"queue":  queue,
			"jobSet": jobSet,
		}).Warn("Action skipped: No active jobs found for cancellation")
		a.metrics.RecordCancellation(time.Since(start), 0, nil)
		return
	}

	// Generate cancellation queries
	now := time.Now()
	queries := make([]db.IngestionQuery, 0, len(jobs))
	for _, job := range jobs {
		queries = append(queries, db.SetJobCancelled{
			JobID:        job.JobID,
			Time:         now,
			CancelReason: "Bulk cancellation via Broadside test",
			CancelUser:   "broadside-test",
		})
	}

	// Execute batch
	err = a.database.ExecuteIngestionQueryBatch(ctx, queries)
	duration := time.Since(start)

	if err != nil {
		logging.WithError(err).WithFields(map[string]any{
			"queue":    queue,
			"jobSet":   jobSet,
			"jobCount": len(jobs),
		}).Warn("Failed to execute cancellation batch")
	} else {
		logging.WithFields(map[string]any{
			"queue":    queue,
			"jobSet":   jobSet,
			"jobCount": len(jobs),
			"duration": duration,
		}).Info("Cancellation completed")
	}

	a.metrics.RecordCancellation(duration, len(jobs), err)
}

// jobInfo holds minimal information about a job needed for actions.
type jobInfo struct {
	JobID string
	State string
}

// getActiveJobs queries the database for active jobs in the specified queue and job set.
// Active jobs are those not in terminal states (succeeded, failed, cancelled, preempted, rejected).
func (a *Actor) getActiveJobs(ctx context.Context, queue string, jobSet string) ([]jobInfo, error) {
	// Use GetJobs to query for jobs matching the queue and job set
	filters := []*model.Filter{
		{
			Field:        "queue",
			Match:        model.MatchAnyOf,
			Value:        []string{queue},
			IsAnnotation: false,
		},
		{
			Field:        "jobSet",
			Match:        model.MatchExact,
			Value:        jobSet,
			IsAnnotation: false,
		},
		{
			Field: "state",
			Match: model.MatchAnyOf,
			Value: []string{
				"QUEUED",
				"LEASED",
				"PENDING",
				"RUNNING",
			},
			IsAnnotation: false,
		},
	}

	// Query with large page size to get all matching jobs
	// Using 100000 as a reasonable upper bound for a single job set
	jobs, err := a.database.GetJobs(&ctx, filters, false, nil, 0, 100000)
	if err != nil {
		return nil, fmt.Errorf("querying jobs: %w", err)
	}

	// Extract job IDs
	result := make([]jobInfo, 0, len(jobs))
	for _, job := range jobs {
		result = append(result, jobInfo{
			JobID: job.JobId,
			State: job.State,
		})
	}

	return result, nil
}
