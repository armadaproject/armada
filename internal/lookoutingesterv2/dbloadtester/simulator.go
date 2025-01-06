package dbloadtester

import (
	"context"
	"math"
	"regexp"
	"sync"
	"time"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/configuration"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/instructions"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/metrics"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/model"
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

	db        *lookoutdb.LookoutDb
	converter *instructions.InstructionConverter
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

func Setup(lookoutIngesterConfig configuration.LookoutIngesterV2Configuration, testConfig Config) *LoadTester {
	m := metrics.Get()

	db, err := database.OpenPgxPool(lookoutIngesterConfig.Postgres)
	if err != nil {
		panic(errors.WithMessage(err, "Error opening connection to postgres"))
	}

	fatalRegexes := make([]*regexp.Regexp, len(lookoutIngesterConfig.FatalInsertionErrors))
	for i, str := range lookoutIngesterConfig.FatalInsertionErrors {
		rgx, err := regexp.Compile(str)
		if err != nil {
			log.Errorf("Error compiling regex %s", str)
			panic(err)
		}
		fatalRegexes[i] = rgx
	}

	lookoutDb := lookoutdb.NewLookoutDb(db, fatalRegexes, m, lookoutIngesterConfig.MaxBackoff)

	// To avoid load testing the compression algorithm, the compressor is configured not to compress.
	compressor, err := compress.NewZlibCompressor(math.MaxInt)
	if err != nil {
		panic(errors.WithMessage(err, "Error creating compressor"))
	}

	converter := instructions.NewInstructionConverter(m.Metrics, lookoutIngesterConfig.UserAnnotationPrefix, compressor)

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
		lookoutIngesterConfig.BatchSize,
		lookoutIngesterConfig.BatchDuration,
		testConfig.QueueSubmitBatchSize,
		lookoutDb,
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
	instructionSets := make(chan *model.InstructionSet)
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
