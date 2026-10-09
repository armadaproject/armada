package submit

import (
	"context"
	"fmt"
	"math"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

const (
	// ensureConcurrency is how many queues are created or checked at once.
	ensureConcurrency = 16
	// ensureCallTimeout bounds each call to the server.
	ensureCallTimeout = 30 * time.Second
	// driftLinesToLog is how many "exists with a different priorityFactor" lines are logged
	// individually; the rest are counted, so hundreds of queues do not produce hundreds of lines.
	driftLinesToLog = 5
)

// QueueToEnsure is a queue regatta needs and the priorityFactor to create it with.
type QueueToEnsure struct {
	Name           string
	PriorityFactor float64
}

// EnsureQueues creates each queue that does not exist yet and leaves every existing queue exactly as it
// is: regatta runs against shared instances, where changing another team's priorityFactor would be a
// side effect nobody asked for. A queue that exists with a different priorityFactor than the scenario
// declares is logged as a warning, never updated.
func EnsureQueues(ctx context.Context, apiConnectionDetails *client.ApiConnectionDetails, queues []QueueToEnsure) error {
	return client.WithSubmitClient(apiConnectionDetails, func(submitClient api.SubmitClient) error {
		return ensureQueues(ctx, submitClient, queues)
	})
}

// ensureOutcome is what happened to one queue.
type ensureOutcome struct {
	created bool
	// drift is set when the queue already existed with a different priorityFactor.
	drift string
}

func ensureQueues(ctx context.Context, submitClient api.SubmitClient, queues []QueueToEnsure) error {
	outcomes := make([]ensureOutcome, len(queues))

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(ensureConcurrency)
	for i, q := range queues {
		i, q := i, q
		group.Go(func() error {
			outcome, err := ensureQueue(groupCtx, submitClient, q)
			if err != nil {
				return fmt.Errorf("queue %q: %w", q.Name, err)
			}
			outcomes[i] = outcome
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}

	for _, line := range summariseEnsure(outcomes) {
		log.Info(line)
	}
	return nil
}

// ensureQueue creates the queue, or if it already exists reads it to compare priorityFactor.
func ensureQueue(ctx context.Context, submitClient api.SubmitClient, q QueueToEnsure) (ensureOutcome, error) {
	createCtx, cancel := context.WithTimeout(ctx, ensureCallTimeout)
	defer cancel()
	_, err := submitClient.CreateQueue(createCtx, &api.Queue{Name: q.Name, PriorityFactor: q.PriorityFactor})
	if err == nil {
		return ensureOutcome{created: true}, nil
	}
	if status.Code(err) != codes.AlreadyExists {
		return ensureOutcome{}, fmt.Errorf("creating: %w", err)
	}

	getCtx, cancelGet := context.WithTimeout(ctx, ensureCallTimeout)
	defer cancelGet()
	existing, err := submitClient.GetQueue(getCtx, &api.QueueGetRequest{Name: q.Name})
	if err != nil {
		return ensureOutcome{}, fmt.Errorf("reading the existing queue: %w", err)
	}
	if math.Abs(existing.PriorityFactor-q.PriorityFactor) > 1e-9 {
		return ensureOutcome{drift: fmt.Sprintf("queue %q exists with priorityFactor %g, scenario declares %g (left unchanged)",
			q.Name, existing.PriorityFactor, q.PriorityFactor)}, nil
	}
	return ensureOutcome{}, nil
}

// summariseEnsure turns the per-queue outcomes into the log lines: one summary, then the first few
// priorityFactor drifts and a count of the rest.
func summariseEnsure(outcomes []ensureOutcome) []string {
	created, existed := 0, 0
	var drifts []string
	for _, o := range outcomes {
		switch {
		case o.created:
			created++
		default:
			existed++
			if o.drift != "" {
				drifts = append(drifts, o.drift)
			}
		}
	}
	lines := []string{fmt.Sprintf("queues: %d created, %d already existed (left unchanged)", created, existed)}
	for i, d := range drifts {
		if i == driftLinesToLog {
			lines = append(lines, fmt.Sprintf("queues: %d more existing queues have a different priorityFactor than declared", len(drifts)-driftLinesToLog))
			break
		}
		lines = append(lines, d)
	}
	return lines
}
