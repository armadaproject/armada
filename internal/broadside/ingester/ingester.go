package ingester

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/logging"

	"github.com/armadaproject/armada/internal/broadside/configuration"
	"github.com/armadaproject/armada/internal/broadside/db"
	"github.com/armadaproject/armada/internal/broadside/jobspec"
	"github.com/armadaproject/armada/internal/broadside/metrics"
	"github.com/armadaproject/armada/pkg/api"
)

var (
	simulatedError    = []byte("simulated error")
	simulatedDebugMsg = []byte("simulated debug message: pod failed to start due to image pull error")
	preemptionError   = []byte("preempted by higher priority job")
	defaultJobSpec    = generateDefaultJobSpec()
)

func generateDefaultJobSpec() string {
	job := &api.Job{
		PodSpecs: []*v1.PodSpec{
			{
				Containers: []v1.Container{
					{
						Name:  "test",
						Image: "alpine:latest",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU:              resource.MustParse("100m"),
								v1.ResourceMemory:           resource.MustParse("128Mi"),
								v1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
							},
							Limits: v1.ResourceList{
								v1.ResourceCPU:              resource.MustParse("100m"),
								v1.ResourceMemory:           resource.MustParse("128Mi"),
								v1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
							},
						},
						Command: []string{"sleep", "10"},
					},
				},
			},
		},
	}

	bytes, err := proto.Marshal(job)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal default job spec: %v", err))
	}
	return string(bytes)
}

type timestampedQuery struct {
	query      db.IngestionQuery
	enqueuedAt time.Time
}

type queryRouter struct {
	channels []chan timestampedQuery
}

func (r *queryRouter) send(q timestampedQuery, strategy string, maxBacklog int, ctx context.Context) error {
	jobID := db.JobIDFromQuery(q.query)
	workerIdx := hashJobID(jobID) % uint64(len(r.channels))
	ch := r.channels[workerIdx]

	// If no max backlog configured, use blocking send with context
	if maxBacklog <= 0 {
		select {
		case ch <- q:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	// Check backlog size
	if len(ch) >= maxBacklog {
		switch strategy {
		case "drop":
			// Silently drop the query
			return nil
		case "error":
			return fmt.Errorf("backlog full: %d queries pending", len(ch))
		default: // "block" or empty
			select {
			case ch <- q:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}
	}

	// Normal send
	select {
	case ch <- q:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func hashJobID(jobID string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(jobID); i++ {
		h ^= uint64(jobID[i])
		h *= 1099511628211
	}
	return h
}

type Ingester struct {
	config       configuration.IngestionConfig
	queueConfigs []configuration.QueueConfig
	database     db.Database
	metrics      *metrics.IngesterMetrics
	selector     *jobspec.QueueJobSetSelector
}

// routerSend wraps the router send with config-aware backlog handling
func (i *Ingester) routerSend(router *queryRouter, q timestampedQuery, ctx context.Context) {
	err := router.send(q, i.config.BacklogDropStrategy, i.config.MaxBacklogSize, ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		logging.WithError(err).Warn("Failed to send query to worker")
	}
}

func NewIngester(
	config configuration.IngestionConfig,
	queueConfigs []configuration.QueueConfig,
	database db.Database,
	metrics *metrics.IngesterMetrics,
) (*Ingester, error) {
	selector, err := jobspec.NewQueueJobSetSelector(queueConfigs)
	if err != nil {
		return nil, fmt.Errorf("creating new queue job set selector failed: %w", err)
	}

	return &Ingester{
		config:       config,
		queueConfigs: queueConfigs,
		database:     database,
		metrics:      metrics,
		selector:     selector,
	}, nil
}

func (i *Ingester) Setup(ctx context.Context) error {
	logging.Info("Populating database with historical jobs")

	for qIdx, queueCfg := range i.queueConfigs {
		for jsIdx, jobSetCfg := range queueCfg.JobSetConfig {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			historicalCfg := jobSetCfg.HistoricalJobsConfig
			if historicalCfg.NumberOfJobs == 0 {
				continue
			}

			succeededThreshold := int(historicalCfg.ProportionSucceeded * 1000)
			erroredThreshold := succeededThreshold + int(historicalCfg.ProportionErrored*1000)
			cancelledThreshold := erroredThreshold + int(historicalCfg.ProportionCancelled*1000)

			params := db.HistoricalJobsParams{
				QueueIdx:           qIdx,
				JobSetIdx:          jsIdx,
				QueueName:          queueCfg.Name,
				JobSetName:         jobSetCfg.Name,
				NJobs:              historicalCfg.NumberOfJobs,
				SucceededThreshold: succeededThreshold,
				ErroredThreshold:   erroredThreshold,
				CancelledThreshold: cancelledThreshold,
				JobSpecBytes:       []byte(defaultJobSpec),
				ErrorBytes:         simulatedError,
				DebugBytes:         simulatedDebugMsg,
				PreemptionBytes:    preemptionError,
			}

			if err := i.database.PopulateHistoricalJobs(ctx, params); err != nil {
				return fmt.Errorf("populating historical jobs for queue %s jobset %s: %w",
					queueCfg.Name, jobSetCfg.Name, err)
			}
		}
	}

	logging.Info("Finished populating database with historical jobs")
	return nil
}

func (i *Ingester) Run(ctx context.Context) {
	numWorkers := i.config.NumWorkers
	if numWorkers <= 0 {
		numWorkers = 1
	}

	// Calculate buffer size based on configuration
	bufferMultiplier := i.config.ChannelBufferSizeMultiplier
	if bufferMultiplier <= 0 {
		bufferMultiplier = 10 // Default to 10x batch size
	}
	bufferSize := bufferMultiplier * i.config.BatchSize

	workerChans := make([]chan timestampedQuery, numWorkers)
	for w := range numWorkers {
		workerChans[w] = make(chan timestampedQuery, bufferSize)
	}

	executorWg := &sync.WaitGroup{}
	for w := range numWorkers {
		executorWg.Go(func() { i.runBatchExecutor(ctx, workerChans[w]) })
	}

	router := &queryRouter{channels: workerChans}

	submissionsPerSecond := float64(i.config.SubmissionsPerHour) / 3600.0
	accumulatedSubmissions := 0.0

	secondTicker := time.NewTicker(time.Second)
	defer secondTicker.Stop()

	channelsClosed := false
	defer func() {
		if !channelsClosed {
			for _, ch := range workerChans {
				close(ch)
			}
		}
	}()

	transitions := &jobspec.TransitionHeap{}
	heap.Init(transitions)

	jobCounter := 0

	for {
		select {
		case <-ctx.Done():
			for _, ch := range workerChans {
				close(ch)
			}
			channelsClosed = true
			executorWg.Wait()
			return

		case <-secondTicker.C:
			accumulatedSubmissions += submissionsPerSecond
			toSubmit := int(accumulatedSubmissions)
			accumulatedSubmissions -= float64(toSubmit)

			for range toSubmit {
				i.submitJob(ctx, router, transitions, jobCounter)
				jobCounter++
			}

			now := time.Now()
			for transitions.Len() > 0 && !(*transitions)[0].Time().After(now) {
				trans := heap.Pop(transitions).(jobspec.ScheduledTransition)
				i.processTransition(ctx, router, transitions, trans)
			}
		}
	}
}

func (i *Ingester) Metrics() *metrics.IngesterMetrics {
	return i.metrics
}

func (i *Ingester) runBatchExecutor(
	ctx context.Context,
	queryChan <-chan timestampedQuery,
) {
	batch := make([]db.IngestionQuery, 0, i.config.BatchSize)
	flushTimer := time.NewTimer(time.Second)
	flushTimer.Stop()

	stopAndDrainTimer := func() {
		if !flushTimer.Stop() {
			select {
			case <-flushTimer.C:
			default:
			}
		}
	}

	for {
		select {
		case tsQuery, ok := <-queryChan:
			if !ok {
				stopAndDrainTimer()
				if len(batch) > 0 {
					i.executeBatch(ctx, batch)
				}
				return
			}

			i.metrics.RecordBacklogWaitTime(time.Since(tsQuery.enqueuedAt))
			batch = append(batch, tsQuery.query)

			if len(batch) == 1 {
				flushTimer.Reset(time.Second)
			}

			i.metrics.RecordBacklogSize(len(queryChan), len(batch))

			if len(batch) >= i.config.BatchSize {
				stopAndDrainTimer()
				i.executeBatch(ctx, batch)
				batch = batch[:0]
			}

		case <-flushTimer.C:
			if len(batch) > 0 {
				i.executeBatch(ctx, batch)
				batch = batch[:0]
			}
		}
	}
}

func (i *Ingester) executeBatch(ctx context.Context, batch []db.IngestionQuery) {
	// Create a detached context with timeout for this batch operation
	// This allows the batch to complete even if the parent context is cancelled,
	// preventing partial writes and ensuring clean shutdown
	batchCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	err := i.database.ExecuteIngestionQueryBatch(batchCtx, batch)
	duration := time.Since(start)

	i.metrics.RecordBatchExecution(len(batch), duration, err)

	if err != nil && !errors.Is(err, context.Canceled) {
		logging.WithError(err).Warn("Failed to execute batch")
	}
}

func (i *Ingester) createNewJob(jobNumber, queueIdx, jobSetIdx int, submissionTime time.Time) *db.NewJob {
	queue := i.queueConfigs[queueIdx]
	jobSet := queue.JobSetConfig[jobSetIdx]

	jobID := jobspec.EncodeJobID(submissionTime, queueIdx, jobSetIdx, jobNumber)
	return &db.NewJob{
		JobID:            jobID,
		Queue:            queue.Name,
		JobSet:           jobSet.Name,
		Owner:            queue.Name,
		Namespace:        jobspec.GetNamespace(jobNumber),
		Priority:         int64((jobNumber % jobspec.PriorityValues) + 1),
		PriorityClass:    jobspec.GetPriorityClass(jobNumber),
		Submitted:        submissionTime,
		Cpu:              jobspec.GetCpu(jobNumber),
		Memory:           jobspec.GetMemory(jobNumber),
		EphemeralStorage: jobspec.GetEphemeralStorage(jobNumber),
		Gpu:              jobspec.GetGpu(jobNumber),
		Annotations:      jobspec.GenerateAnnotationsForJob(jobNumber),
	}
}

func (i *Ingester) submitJob(
	ctx context.Context,
	router *queryRouter,
	transitions *jobspec.TransitionHeap,
	jobNumber int,
) {
	now := time.Now()
	queueIdx, jobSetIdx := i.selector.SelectQueueAndJobSet(jobNumber)
	newJob := i.createNewJob(jobNumber, queueIdx, jobSetIdx, now)

	i.routerSend(router, timestampedQuery{
		query: db.InsertJob{
			Job: newJob,
		},
		enqueuedAt: now,
	}, ctx)

	i.routerSend(router, timestampedQuery{
		query: db.InsertJobSpec{
			JobID:   newJob.JobID,
			JobSpec: defaultJobSpec,
		},
		enqueuedAt: now,
	}, ctx)

	heap.Push(transitions, jobspec.NewScheduledTransition(
		now.Add(i.config.JobStateTransitionConfig.QueueingDuration),
		newJob.JobID,
		"",
		jobspec.StateLeased,
	))
}

func (i *Ingester) processTransition(
	ctx context.Context,
	router *queryRouter,
	transitions *jobspec.TransitionHeap,
	trans jobspec.ScheduledTransition,
) {
	now := time.Now()
	cfg := i.config.JobStateTransitionConfig

	jobNumber := jobspec.ExtractJobNumber(trans.JobID())
	cluster, node := jobspec.GetClusterNodeForJobNumber(jobNumber)

	switch trans.ToState() {
	case jobspec.StateLeased:
		runID := jobspec.EncodeRunID(trans.JobID(), 0)
		i.routerSend(router, timestampedQuery{
			query: db.SetJobLeased{
				JobID: trans.JobID(),
				Time:  now,
				RunID: runID,
			},
			enqueuedAt: now,
		}, ctx)
		i.routerSend(router, timestampedQuery{
			query: db.InsertJobRun{
				JobRunID: runID,
				JobID:    trans.JobID(),
				Cluster:  cluster,
				Node:     node,
				Pool:     jobspec.GetPool(jobNumber),
				Time:     now,
			},
			enqueuedAt: now,
		}, ctx)
		heap.Push(transitions, jobspec.NewScheduledTransition(
			now.Add(cfg.LeasedDuration),
			trans.JobID(),
			runID,
			jobspec.StatePending,
		))

	case jobspec.StatePending:
		i.routerSend(router, timestampedQuery{
			query: db.SetJobPending{
				JobID: trans.JobID(),
				Time:  now,
				RunID: trans.RunID(),
			},
			enqueuedAt: now,
		}, ctx)
		i.routerSend(router, timestampedQuery{
			query: db.SetJobRunPending{
				JobRunID: trans.RunID(),
				Time:     now,
			},
			enqueuedAt: now,
		}, ctx)
		heap.Push(transitions, jobspec.NewScheduledTransition(
			now.Add(cfg.PendingDuration),
			trans.JobID(),
			trans.RunID(),
			jobspec.StateRunning,
		))

	case jobspec.StateRunning:
		i.routerSend(router, timestampedQuery{
			query: db.SetJobRunning{
				JobID:       trans.JobID(),
				Time:        now,
				LatestRunID: trans.RunID(),
			},
			enqueuedAt: now,
		}, ctx)
		i.routerSend(router, timestampedQuery{
			query: db.SetJobRunStarted{
				JobRunID: trans.RunID(),
				Time:     now,
				Node:     node,
			},
			enqueuedAt: now,
		}, ctx)
		jobNumber := jobspec.ExtractJobNumber(trans.JobID())
		if jobspec.ShouldSucceed(jobNumber, cfg) {
			heap.Push(transitions, jobspec.NewScheduledTransition(
				now.Add(cfg.RunningToSuccessDuration),
				trans.JobID(),
				trans.RunID(),
				jobspec.StateSucceeded,
			))
		} else {
			heap.Push(transitions, jobspec.NewScheduledTransition(
				now.Add(cfg.RunningToFailureDuration),
				trans.JobID(),
				trans.RunID(),
				jobspec.StateErrored,
			))
		}

	case jobspec.StateSucceeded:
		i.routerSend(router, timestampedQuery{
			query: db.SetJobSucceeded{
				JobID: trans.JobID(),
				Time:  now,
			},
			enqueuedAt: now,
		}, ctx)
		i.routerSend(router, timestampedQuery{
			query: db.SetJobRunSucceeded{
				JobRunID: trans.RunID(),
				Time:     now,
			},
			enqueuedAt: now,
		}, ctx)

	case jobspec.StateErrored:
		i.routerSend(router, timestampedQuery{
			query: db.SetJobErrored{
				JobID: trans.JobID(),
				Time:  now,
			},
			enqueuedAt: now,
		}, ctx)
		i.routerSend(router, timestampedQuery{
			query: db.SetJobRunFailed{
				JobRunID: trans.RunID(),
				Time:     now,
				Error:    simulatedError,
				Debug:    simulatedDebugMsg,
			},
			enqueuedAt: now,
		}, ctx)
		i.routerSend(router, timestampedQuery{
			query: db.InsertJobError{
				JobID: trans.JobID(),
				Error: simulatedError,
			},
			enqueuedAt: now,
		}, ctx)
	}
}
