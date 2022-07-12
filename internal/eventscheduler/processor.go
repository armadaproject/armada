package eventscheduler

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/internal/common/requestid"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/internal/pulsarutils/pulsarrequestid"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// I'm only interested in having the current state of each job
// So I need to take messages out of Pulsar and update the internal state
// Let's here assume messages always arrive in correct order
// This all comes down to what data the scheduler stores

// Job runs table
// job id, executor name, node name

// I think we can start there
// I have an idea of the sql schema
// For now, let's just write to stdout

// I want to write stuff into postgres in bulk
// For that, I need to have in-memory a set of records to write
// For that, I need to know how these things are written
// I need to remember how the pgx copy protocol write thing works
// Let's get it from internally

// I think I figured out that I want to deal with the native sql objects
// Because I can wrangle those in and out of postgres easily
// Let's move the scheduler code away for now

// Now I have the code to handle records in a very general way.
// I don't even need to keep things in separate files anymore. Nice!
// Now, let's write some tests that do bulk upsert for the runs type.
// The goal for now is to build the fast ingester for the scheduler.
// It'll upsert stuff maybe once per second.

// Looks like it might be possible to insert only if the new value is not null.
// That's a bit tricky with go datatypes though.
// It may be better to keep everything in-memory.
// There's a tricky one around memory leaks.
// It's solveable by making a query once per second, applying updates, and then writing back though.
// And the amount of stuff will be relatively small I think.
// For now, let's write the logic that listens to messages and creates runs stored in-memory.

// Let's add the insert tests
// I have inserts working
// Now I think it's time to clean it up a bit
// Then I can try to make it work with if is not null
// It's not crucial to have though, since I can fetch/cache from postgres and then apply the changed values
// I'm getting to the point where I have everything for the ingester to work
// So it can continously write to the db
// Once I have this part working, I need

type SchedulerProcessor struct {
	Consumer pulsar.Consumer
	// Logger from which the loggers used by this service are derived
	// (e.g., using srv.Logger.WithField), or nil, in which case the global logrus logger is used.
	Logger *logrus.Entry
}

// Maintain the view of the scheduler state by reading from the log and writing the postgres and in-memory storage.
func (srv *SchedulerProcessor) Run(ctx context.Context) error {

	// Get the configured logger, or the standard logger if none is provided.
	var log *logrus.Entry
	if srv.Logger != nil {
		log = srv.Logger.WithField("service", "SchedulerProcessor")
	} else {
		log = logrus.StandardLogger().WithField("service", "SchedulerProcessor")
	}
	log.Info("service started")

	// Receive Pulsar messages asynchronously.
	g, ctx := errgroup.WithContext(ctx)
	pulsarToChannel := pulsarutils.NewPulsarToChannel(srv.Consumer)
	g.Go(func() error { return pulsarToChannel.Run(ctx) })

	// Let's have it regularly print the status of all active jobs in the system
	runsFromJobId := make(map[string]map[string]string)

	// Run until ctx is cancelled.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-pulsarToChannel.C:

			// Incoming gRPC requests are annotated with a unique id,
			// which is included with the corresponding Pulsar message.
			requestId := pulsarrequestid.FromMessageOrMissing(msg)

			// Put the requestId into a message-specific context and logger,
			// which are passed on to sub-functions.
			messageCtx, ok := requestid.AddToIncomingContext(ctx, requestId)
			if !ok {
				messageCtx = ctx
			}
			messageLogger := log.WithFields(logrus.Fields{"messageId": msg.ID(), requestid.MetadataKey: requestId})
			ctxWithLogger := ctxlogrus.ToContext(messageCtx, messageLogger)

			// Unmarshal and validate the message.
			sequence, err := eventutil.UnmarshalEventSequence(ctxWithLogger, msg.Payload())
			srv.Consumer.Ack(msg)
			if err != nil {
				logging.WithStacktrace(messageLogger, err).Warnf("processing message failed; ignoring")
				break
			}
			for _, event := range sequence.Events {
				jobId, err := armadaevents.JobIdFromEvent(event)
				if err != nil {
					logging.WithStacktrace(messageLogger, err).Warnf("getting jobId failed; ignoring")
					break
				}

				jobIdString, err := armadaevents.UlidStringFromProtoUuid(jobId)
				if err != nil {
					logging.WithStacktrace(messageLogger, err).Warnf("converting job id to string failed; ignoring")
					break
				}

				runFromRunId, ok := runsFromJobId[jobIdString]
				if !ok {
					runs := make(map[string]string)
					runsFromJobId[jobIdString] = runs
					fmt.Println(runs)
				}

				fmt.Println(jobIdString, ", ", runFromRunId)
			}
		}
	}
}
