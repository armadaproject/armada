package queue

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/status"
	"github.com/pkg/errors"
	"github.com/renstrom/shortuuid"
	"google.golang.org/grpc/codes"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// RunSetup creates the queue(s) if configured and returns list of created queue names
func RunSetup(ctx context.Context, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, out io.Writer) ([]string, error) {
	setup := testSpec.GetQueueConfig().GetSetup()
	if setup == nil {
		return nil, nil
	}
	baseName := testSpec.Queue
	batchSize := max(1, setup.GetBatchSize())
	numBatches := max(1, setup.GetNumBatches())
	interval := protoutil.ToStdDuration(setup.GetInterval())
	queueSpec := setup.GetQueueSpec()

	var queueNames []string
	err := client.WithQueueServiceClient(conn, func(qsc api.QueueServiceClient) error {
		// Create a closed ticker channel; receiving on tickerCh returns immediately.
		C := make(chan time.Time)
		close(C)
		tickerCh := (<-chan time.Time)(C)

		// If an interval is provided, replace tickerCh with one that generates ticks periodically.
		if interval != 0 {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			tickerCh = ticker.C
		}

		var numBatchesCreated uint32
		for numBatchesCreated < numBatches {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tickerCh:
				batchQueues := queuesForBatch(baseName, queueSpec, batchSize)
				var err error
				if len(batchQueues) == 1 {
					_, err = qsc.CreateQueue(ctx, batchQueues[0])
				} else {
					_, err = qsc.CreateQueues(ctx, &api.QueueList{Queues: batchQueues})
				}
				if err != nil {
					return err
				}
				fmt.Fprintf(out, "created %d queue(s)\n", len(batchQueues))
				for _, queue := range batchQueues {
					queueNames = append(queueNames, queue.Name)
				}
				numBatchesCreated++
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// The assertions query TestSpec.Queue, so point it at a queue that actually exists.
	if len(queueNames) > 0 {
		testSpec.Queue = queueNames[0]
	}
	return queueNames, nil
}

// queuesForBatch builds the queues to create for a single batch
func queuesForBatch(baseName string, queueSpec *api.Queue, batchSize uint32) []*api.Queue {
	queues := make([]*api.Queue, 0, batchSize)
	for i := uint32(0); i < batchSize; i++ {
		if queueSpec == nil {
			queues = append(queues, &api.Queue{
				Name:           baseName + "-" + shortuuid.New(),
				PriorityFactor: 1.0,
			})
			continue
		}
		queues = append(queues, queueFromSpec(baseName, queueSpec))
	}
	return queues
}

// queueFromSpec clones spec to create a queue, name from TestSpec.Queue will override the name in spec, and if PriorityFactor is 0 it will be set to 1.0.
func queueFromSpec(baseName string, spec *api.Queue) *api.Queue {
	clone := proto.Clone(spec).(*api.Queue)
	if clone.PriorityFactor == 0 {
		clone.PriorityFactor = 1.0
	}
	clone.Name = baseName + "-" + shortuuid.New()
	return clone
}

// RunUpdate applies the configured update to the queue(s), if configured.
func RunUpdate(ctx context.Context, queueNames []string, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, out io.Writer) error {
	update := testSpec.GetQueueConfig().GetUpdate()
	if update == nil {
		return nil
	}
	if len(queueNames) == 0 {
		queueNames = []string{testSpec.Queue}
	}
	return client.WithQueueServiceClient(conn, func(qsc api.QueueServiceClient) error {
		queues := make([]*api.Queue, len(queueNames))
		for i, queueName := range queueNames {
			queue := proto.Clone(update).(*api.Queue)
			queue.Name = queueName
			if queue.PriorityFactor == 0 {
				queue.PriorityFactor = 1.0
			}
			queues[i] = queue
		}
		if len(queues) == 1 {
			updateCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, err := qsc.UpdateQueue(updateCtx, queues[0])
			cancel()
			if err != nil {
				return errors.Wrapf(err, "failed to update queue %s", queues[0].Name)
			}
		} else {
			// Use the batched UpdateQueues endpoint (PUT /v1/batched/update_queues) to exercise it.
			updateCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			resp, err := qsc.UpdateQueues(updateCtx, &api.QueueList{Queues: queues})
			cancel()
			if err != nil {
				return errors.Wrap(err, "UpdateQueues (batched) failed")
			}
			if failed := resp.GetFailedQueues(); len(failed) > 0 {
				return fmt.Errorf("failed to update queue %s: %s", failed[0].GetQueue().GetName(), failed[0].GetError())
			}
		}
		fmt.Fprintf(out, "updated %d queue(s)\n", len(queueNames))
		return nil
	})
}

// RunTeardown deletes the queue(s).
func RunTeardown(queueNames []string, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, out io.Writer) error {
	if testSpec.GetQueueConfig() == nil {
		return nil
	}
	if len(queueNames) == 0 {
		queueNames = []string{testSpec.Queue}
	}
	return client.WithQueueServiceClient(conn, func(qsc api.QueueServiceClient) error {
		var deleted []string
		for _, queueName := range queueNames {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := qsc.DeleteQueue(ctx, &api.QueueDeleteRequest{Name: queueName})
			cancel()
			if err != nil {
				if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
					fmt.Fprintf(out, "queue %s already deleted, skipping teardown\n", queueName)
					continue // already gone; that's fine
				}
				return errors.Wrapf(err, "failed to delete queue %s", queueName)
			}
			deleted = append(deleted, queueName)
		}
		if len(deleted) > 0 {
			fmt.Fprintf(out, "deleted %d queue(s)\n", len(deleted))
		}
		if assertDeleted(testSpec) {
			for _, queueName := range deleted {
				getCtx, getCancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, err := qsc.GetQueue(getCtx, &api.QueueGetRequest{Name: queueName})
				getCancel()
				if err == nil {
					return fmt.Errorf("expected queue %s to be deleted, but GetQueue succeeded", queueName)
				}
				if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
					return errors.Wrapf(err, "expected NOT_FOUND asserting queue %s was deleted, got", queueName)
				}
			}
			fmt.Fprintf(out, "asserted %d queue(s) were deleted\n", len(deleted))
		}
		return nil
	})
}

// assertDeleted returns true if any configured assertion requests that deleted queue(s)
// be verified as NOT_FOUND via GetQueue.
func assertDeleted(testSpec *api.TestSpec) bool {
	for _, assertion := range testSpec.GetQueueConfig().GetAssertions() {
		if assertion.GetDeleted() {
			return true
		}
	}
	return false
}

// RunAssertions checks queue state assertions. activeInPool is checked against testSpec.Queue
// (the queue jobs are submitted to); appearsInStream and matches are checked against every queue
// in queueNames, since a batched setup creates and updates many queues that must all be valid.
func RunAssertions(ctx context.Context, queueNames []string, testSpec *api.TestSpec, conn *client.ApiConnectionDetails, out io.Writer) error {
	assertions := testSpec.GetQueueConfig().GetAssertions()
	if len(assertions) == 0 {
		return nil
	}
	if len(queueNames) == 0 {
		queueNames = []string{testSpec.Queue}
	}
	for _, assertion := range assertions {
		if pool := assertion.ActiveInPool; pool != "" {
			if err := client.WithJobsClient(conn, func(jc api.JobsClient) error {
				reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				defer cancel()
				resp, err := jc.GetActiveQueues(reqCtx, &api.GetActiveQueuesRequest{})
				if err != nil {
					return errors.Wrap(err, "GetActiveQueues failed")
				}
				activeQueues, ok := resp.ActiveQueuesByPool[pool]
				if !ok {
					return fmt.Errorf("pool %q not found in GetActiveQueues response", pool)
				}
				for _, q := range activeQueues.Queues {
					if q == testSpec.Queue {
						fmt.Fprintf(out, "asserted queue %s is active in pool %s\n", testSpec.Queue, pool)
						return nil
					}
				}
				return fmt.Errorf("queue %q not found in active queues for pool %q", testSpec.Queue, pool)
			}); err != nil {
				return err
			}
		}
		if assertion.AppearsInStream {
			if err := client.WithQueueServiceClient(conn, func(qsc api.QueueServiceClient) error {
				streamCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				defer cancel()
				stream, err := qsc.GetQueues(streamCtx, &api.StreamingQueueGetRequest{})
				if err != nil {
					return errors.Wrap(err, "GetQueues stream failed to open")
				}
				// Collect the stream once, then verify every created queue is present.
				seen := make(map[string]bool)
				for {
					msg, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return errors.Wrap(err, "GetQueues stream recv error")
					}
					if q := msg.GetQueue(); q != nil {
						seen[q.Name] = true
					}
				}
				for _, queueName := range queueNames {
					if !seen[queueName] {
						return fmt.Errorf("queue %q not found in GetQueues stream", queueName)
					}
				}
				fmt.Fprintf(out, "asserted %d queue(s) appear in GetQueues stream\n", len(queueNames))
				return nil
			}); err != nil {
				return err
			}
		}
		if expected := assertion.Matches; expected != nil {
			if err := client.WithQueueServiceClient(conn, func(qsc api.QueueServiceClient) error {
				for _, queueName := range queueNames {
					getCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
					actual, err := qsc.GetQueue(getCtx, &api.QueueGetRequest{Name: queueName})
					cancel()
					if err != nil {
						return errors.Wrap(err, "GetQueue failed")
					}
					want := proto.Clone(expected).(*api.Queue)
					want.Name = actual.Name
					if !proto.Equal(want, actual) {
						return fmt.Errorf("queue %q properties did not match: got %+v, want %+v", queueName, actual, want)
					}
				}
				fmt.Fprintf(out, "asserted %d queue(s) properties match expected\n", len(queueNames))
				return nil
			}); err != nil {
				return err
			}
		}
	}
	return nil
}
