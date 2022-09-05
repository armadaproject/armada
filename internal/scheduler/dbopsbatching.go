package scheduler

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"

	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/logging"
)

// DbOperationsBatcher is a service that consumes event sequences
// and produces optimised sequences of database operations.
//
// TODO: Move into dbopsfromevents.go
type DbOperationsBatcher struct {
	In  chan *eventutil.EventSequenceWithMessageIds
	Out chan *DbOperationsWithMessageIds
	// Max number of Pulsar messages to include with each batch.
	MaxMessages int
	// Max time interval between batches.
	MaxInterval time.Duration
}

func NewDbOperationsBatcher(in chan *eventutil.EventSequenceWithMessageIds) *DbOperationsBatcher {
	return &DbOperationsBatcher{
		In:          in,
		Out:         make(chan *DbOperationsWithMessageIds),
		MaxMessages: 1000,
		MaxInterval: 5 * time.Second,
	}
}

// DbOperationsWithMessageIds bundles a sequence of db ops with the ids of all Pulsar
// messages that were consumed to produce it.
type DbOperationsWithMessageIds struct {
	Ops        []DbOperation
	MessageIds []pulsar.MessageID
}

func (srv *DbOperationsBatcher) Run(ctx context.Context) error {
	log := ctxlogrus.Extract(ctx)
	ticker := time.NewTicker(srv.MaxInterval)
	var batch *DbOperationsWithMessageIds
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if batch == nil {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case srv.Out <- batch:
			}
			batch = nil
		case sequenceWithIds := <-srv.In:
			if sequenceWithIds == nil || sequenceWithIds.Sequence == nil {
				continue
			}
			if batch == nil {
				batch = &DbOperationsWithMessageIds{}
			}

			for i := range sequenceWithIds.Sequence.GetEvents() {
				ops, err := DbOpsFromEventInSequence(sequenceWithIds.Sequence, i)
				if err != nil {
					logging.WithStacktrace(log, err).Error("failed to convert event to db op")
					continue
				} else if ops == nil {
					continue // No op corresponding to this event
				}
				for _, op := range ops {
					batch.Ops = AppendDbOperation(batch.Ops, op)
				}
				batch.MessageIds = append(batch.MessageIds, sequenceWithIds.MessageIds...)
			}
			if len(batch.MessageIds) > srv.MaxMessages {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case srv.Out <- batch:
				}
				batch = nil
			}
		}
	}
}
