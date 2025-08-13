package dbloadtester

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/queryingester/clickhousedb"
	"github.com/armadaproject/armada/internal/queryingester/configuration"
	"github.com/armadaproject/armada/internal/queryingester/instructions"
	"github.com/armadaproject/armada/pkg/armadaevents"
	clientUtil "github.com/armadaproject/armada/pkg/client/util"
)

type LoadTester struct {
	totalJobs            int
	queueNames           []string
	totalConcurrentJobs  int
	useExistingQueues    bool
	jobTemplate          *v1.PodSpec
	batchSize            int
	batchDuration        time.Duration
	queueSubmitBatchSize int

	db        *clickhousedb.ClickhouseDb
	converter *instructions.Converter
}

type Config struct {
	TotalJobs            int
	QueueNames           []string
	TotalConcurrentJobs  int
	JobTemplateFile      string
	QueueSubmitBatchSize int
}

type Results struct {
	TotalTestDuration        time.Duration
	TotalDBInsertionDuration time.Duration
	TotalEventsProcessed     int
}

func Setup(config configuration.QueryIngesterConfig, testConfig Config) *LoadTester {
	ctx := armadacontext.Background()
	ctx.Info("opening connection to clickhouse")
	options, err := config.ClickHouse.BuildOptions()
	if err != nil {
		panic(err)
	}
	db, err := clickhousedb.OpenClickhouse(ctx, options)
	if err != nil {
		panic(errors.WithMessage(err, "Error opening connection to clickhouse"))
	}

	err = clickhousedb.MigrateDB(ctx, fmt.Sprintf("clickhouse://clickhouse:psw@localhost:9000/%s", "default"))
	if err != nil {
		panic(errors.WithMessage(err, "error migrating database"))
	}

	ch := clickhousedb.New(db)

	converter := instructions.NewConverter(config.UserAnnotationPrefix)

	submitJobTemplate := &v1.PodSpec{}
	err = clientUtil.BindJsonOrYaml(testConfig.JobTemplateFile, submitJobTemplate)
	if err != nil {
		panic(errors.WithMessage(err, "Error reading job template yaml"))
	}

	if len(testConfig.QueueNames) > testConfig.TotalConcurrentJobs {
		panic("Performance simulator currently requires a minimum of one concurrent job per queue")
	}

	return &LoadTester{
		testConfig.TotalJobs,
		testConfig.QueueNames,
		testConfig.TotalConcurrentJobs,
		false,
		submitJobTemplate,
		config.BatchSize,
		config.BatchDuration,
		testConfig.QueueSubmitBatchSize,
		ch,
		converter,
	}
}

// Run performs the load test with the configuration and database provided by the loadtester
func (l *LoadTester) Run(ctx *armadacontext.Context) (*Results, error) {
	loadTestStart := time.Now()

	// generates events, simulated to have a realistic pattern
	simulatedEvents := make(chan *utils.EventsWithIds[*armadaevents.EventSequence])
	go func() {
		l.GenerateEvents(simulatedEvents)
		close(simulatedEvents)
	}()

	// set up batching, to match expected batching behaviour in non-test code
	batchedEventSequences := make(chan []*utils.EventsWithIds[*armadaevents.EventSequence])
	eventCounterFunc := func(seq *utils.EventsWithIds[*armadaevents.EventSequence]) int {
		totalEvents := 0
		for _, sequence := range seq.Events {
			totalEvents += len(sequence.Events)
		}
		return totalEvents
	}

	// batch the generated events
	batcher := ingest.NewBatcher[*utils.EventsWithIds[*armadaevents.EventSequence]](simulatedEvents, l.batchSize, l.batchDuration, eventCounterFunc, batchedEventSequences)
	go func() {
		batcher.Run(ctx)
		close(batchedEventSequences)
	}()

	// Merge intermediate event batches
	mergedEventBatches := make(chan *utils.EventsWithIds[*armadaevents.EventSequence])
	go func() {
		for batch := range batchedEventSequences {
			allEvents := &utils.EventsWithIds[*armadaevents.EventSequence]{}
			for _, eventsWithIds := range batch {
				allEvents.Events = append(allEvents.Events, eventsWithIds.Events...)
				allEvents.MessageIds = append(allEvents.MessageIds, eventsWithIds.MessageIds...)
			}
			mergedEventBatches <- allEvents
		}
		close(mergedEventBatches)
	}()

	// convert the events into the instructionSet taken by the db
	instructionSets := make(chan *instructions.Instructions)
	go func() {
		for msg := range mergedEventBatches {
			start := time.Now()
			converted := l.converter.Convert(ctx, msg)
			taken := time.Now().Sub(start)
			log.Infof("Processed %d pulsar messages in %dms", len(msg.MessageIds), taken.Milliseconds())
			instructionSets <- converted
		}
		close(instructionSets)
	}()

	// benchmark the insertion into the db
	var totalDBTime time.Duration
	var totalMessages int
	for msg := range instructionSets {
		start := time.Now()
		err := l.db.Store(ctx, msg)
		totalDBTime += time.Now().Sub(start)
		if err != nil {
			log.WithError(err).Warn("Error inserting messages")
			log.Panic("db err")
		} else {
			log.Infof("Inserted %d pulsar messages in %dms", len(msg.GetMessageIDs()), totalDBTime.Milliseconds())
			totalMessages += len(msg.GetMessageIDs())
		}
		if errors.Is(err, context.DeadlineExceeded) {
			// This occurs when we're shutting down- it's a signal to stop processing immediately
			break
		}
	}
	loadTestDuration := time.Now().Sub(loadTestStart)

	return &Results{
		TotalTestDuration:        loadTestDuration,
		TotalDBInsertionDuration: totalDBTime,
		TotalEventsProcessed:     totalMessages,
	}, nil
}

// GenerateEvents generates EventSequencesWithIds consisting of job and job run events onto the given channel.
func (l *LoadTester) GenerateEvents(eventsCh chan<- *utils.EventsWithIds[*armadaevents.EventSequence]) {
	totalPerQueue := l.totalJobs / len(l.queueNames)
	additionalJobs := l.totalJobs % len(l.queueNames)

	totalConcurrentPerQueue := l.totalConcurrentJobs / len(l.queueNames)
	additionalConcurrentJobs := l.totalConcurrentJobs % len(l.queueNames)

	// create queues
	queues := make([]*QueueEventGenerator, len(l.queueNames))
	for i, queueName := range l.queueNames {
		if i == len(l.queueNames)-1 {
			totalPerQueue += additionalJobs
			totalConcurrentPerQueue += additionalConcurrentJobs
		}
		queue := NewQueueEventGenerator(
			queueName,
			queueName,
			totalPerQueue,
			totalConcurrentPerQueue,
			l.queueSubmitBatchSize,
			1,
			l.jobTemplate,
		)
		queues[i] = queue
	}

	wg := sync.WaitGroup{}
	wg.Add(len(queues))
	for _, queue := range queues {
		go func() {
			defer wg.Done()
			queue.Generate(eventsCh)
		}()
	}
	wg.Wait()

	return
}
