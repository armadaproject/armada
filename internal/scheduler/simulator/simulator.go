package simulator

import (
	"container/heap"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/logging"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/fairness"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	schedulerobjects "github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduleringester"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Simulator captures the parameters and state of the Armada simulator.
type Simulator struct {
	ClusterSpec      *ClusterSpec
	WorkloadSpec     *WorkloadSpec
	schedulingConfig configuration.SchedulingConfig
	// Map from jobId to the jobTemplate from which the job was created.
	jobTemplateByJobId map[string]*JobTemplate
	// Map from job template ids to slices of templates depending on those ids.
	jobTemplatesByDependencyIds map[string]map[string]*JobTemplate
	// Map from job template id to jobTemplate for templates for which all jobs have not yet succeeded.
	activeJobTemplatesById map[string]*JobTemplate
	// The JobDb stores all jobs that have yet to terminate.
	jobDb *jobdb.JobDb
	// Map from node id to the pool to which the node belongs.
	poolByNodeId map[string]string
	// Separate nodeDb per pool and executorGroup.
	nodeDbByPoolAndExecutorGroup map[string][]*nodedb.NodeDb
	// Map from executor name to the nodeDb to which it belongs.
	nodeDbByExecutorName map[string]*nodedb.NodeDb
	// Allocation by pool for each queue and priority class.
	// Stored across invocations of the scheduler.
	allocationByPoolAndQueueAndPriorityClass map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string]
	// Total resources across all executorGroups for each pool.
	totalResourcesByPool map[string]schedulerobjects.ResourceList
	// Indicates whether a job has been submitted or terminated since the last scheduling round.
	shouldSchedule bool
	// Current simulated time.
	time time.Time
	// Sequence number of the next event to be published.
	sequenceNumber int
	// Events stored in a priority queue ordered first by timestamp and second by sequence number.
	eventLog EventLog
	// Simulated events are emitted on these output channels.
	// Create a channel by calling s.Output() before running the simulator.
	outputs []chan *armadaevents.EventSequence
	// Global job scheduling rate-limiter.
	limiter *rate.Limiter
	// Per-queue job scheduling rate-limiters.
	limiterByQueue map[string]*rate.Limiter
	// Used to generate random numbers from a chosen seed.
	rand *rand.Rand
	// Used to ensure each job is given a unique time stamp.
	logicalJobCreatedTimestamp atomic.Int64
	// If true, scheduler logs are omitted.
	// This since the logs are very verbose when scheduling large numbers of jobs.
	SuppressSchedulerLogs bool
}

func NewSimulator(clusterSpec *ClusterSpec, workloadSpec *WorkloadSpec, schedulingConfig configuration.SchedulingConfig) (*Simulator, error) {
	// TODO: Move clone to caller?
	// Copy specs to avoid concurrent mutation.
	clusterSpec = proto.Clone(clusterSpec).(*ClusterSpec)
	workloadSpec = proto.Clone(workloadSpec).(*WorkloadSpec)
	initialiseClusterSpec(clusterSpec)
	initialiseWorkloadSpec(workloadSpec)
	if err := validateClusterSpec(clusterSpec); err != nil {
		return nil, err
	}
	if err := validateWorkloadSpec(workloadSpec); err != nil {
		return nil, err
	}
	jobDb := jobdb.NewJobDb(
		schedulingConfig.Preemption.PriorityClasses,
		schedulingConfig.Preemption.DefaultPriorityClass,
	)
	s := &Simulator{
		ClusterSpec:                              clusterSpec,
		WorkloadSpec:                             workloadSpec,
		schedulingConfig:                         schedulingConfig,
		jobTemplateByJobId:                       make(map[string]*JobTemplate),
		jobTemplatesByDependencyIds:              make(map[string]map[string]*JobTemplate),
		activeJobTemplatesById:                   make(map[string]*JobTemplate),
		jobDb:                                    jobDb,
		nodeDbByPoolAndExecutorGroup:             make(map[string][]*nodedb.NodeDb),
		poolByNodeId:                             make(map[string]string),
		nodeDbByExecutorName:                     make(map[string]*nodedb.NodeDb),
		allocationByPoolAndQueueAndPriorityClass: make(map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string]),
		totalResourcesByPool:                     make(map[string]schedulerobjects.ResourceList),
		limiter: rate.NewLimiter(
			rate.Limit(schedulingConfig.MaximumSchedulingRate),
			schedulingConfig.MaximumSchedulingBurst,
		),
		limiterByQueue: make(map[string]*rate.Limiter),
		rand:           rand.New(rand.NewSource(workloadSpec.RandomSeed)),
	}
	s.limiter.SetBurstAt(s.time, schedulingConfig.MaximumSchedulingBurst)
	if err := s.setupClusters(); err != nil {
		return nil, err
	}
	if err := s.bootstrapWorkload(); err != nil {
		return nil, err
	}
	return s, nil
}

// Run runs the scheduler until all jobs have finished successfully.
func (s *Simulator) Run(ctx *armadacontext.Context) error {
	defer func() {
		for _, c := range s.outputs {
			close(c)
		}
	}()
	// Bootstrap the simulator by pushing an event that triggers a scheduler run.
	s.pushScheduleEvent(s.time)
	// Then run the scheduler until all jobs have completed.
	for s.eventLog.Len() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			event := heap.Pop(&s.eventLog).(Event)
			if err := s.handleSimulatorEvent(ctx, event); err != nil {
				return err
			}
		}
	}
	return nil
}

// Output returns a channel on which all simulated events are sent.
// This function must be called before *Simulator.Run.
func (s *Simulator) Output() <-chan *armadaevents.EventSequence {
	c := make(chan *armadaevents.EventSequence, 128)
	s.outputs = append(s.outputs, c)
	return c
}

func validateClusterSpec(clusterSpec *ClusterSpec) error {
	poolNames := util.Map(clusterSpec.Pools, func(pool *Pool) string { return pool.Name })
	if !slices.Equal(poolNames, armadaslices.Unique(poolNames)) {
		return errors.Errorf("duplicate pool name: %v", poolNames)
	}

	executorNames := make([]string, 0)
	for _, pool := range clusterSpec.Pools {
		for _, executorGroup := range pool.ClusterGroups {
			for _, executor := range executorGroup.Clusters {
				executorNames = append(executorNames, executor.Name)
			}
		}
	}
	if !slices.Equal(executorNames, armadaslices.Unique(executorNames)) {
		return errors.Errorf("duplicate executor name: %v", executorNames)
	}
	return nil
}

func validateWorkloadSpec(workloadSpec *WorkloadSpec) error {
	queueNames := util.Map(workloadSpec.Queues, func(queue *Queue) string { return queue.Name })
	if !slices.Equal(queueNames, armadaslices.Unique(queueNames)) {
		return errors.Errorf("duplicate queue name: %v", queueNames)
	}
	jobTemplateIdSlices := util.Map(workloadSpec.Queues, func(queue *Queue) []string {
		return util.Map(queue.JobTemplates, func(template *JobTemplate) string { return template.Id })
	})
	jobTemplateIds := make([]string, 0)
	for _, singleQueueTemplateIds := range jobTemplateIdSlices {
		jobTemplateIds = append(jobTemplateIds, singleQueueTemplateIds...)
	}
	if !slices.Equal(jobTemplateIds, armadaslices.Unique(jobTemplateIds)) {
		return errors.Errorf("duplicate job template ids: %v", jobTemplateIds)
	}

	return nil
}

func (s *Simulator) setupClusters() error {
	for _, pool := range s.ClusterSpec.Pools {
		totalResourcesForPool := schedulerobjects.ResourceList{}
		for executorGroupIndex, executorGroup := range pool.ClusterGroups {
			nodeDb, err := nodedb.NewNodeDb(
				s.schedulingConfig.Preemption.PriorityClasses,
				s.schedulingConfig.MaxExtraNodesToConsider,
				s.schedulingConfig.IndexedResources,
				s.schedulingConfig.IndexedTaints,
				s.schedulingConfig.IndexedNodeLabels,
			)
			if err != nil {
				return err
			}
			for executorIndex, executor := range executorGroup.Clusters {
				executorName := fmt.Sprintf("%s-%d-%d", pool.Name, executorGroupIndex, executorIndex)
				s.nodeDbByExecutorName[executorName] = nodeDb
				for nodeTemplateIndex, nodeTemplate := range executor.NodeTemplates {
					for i := 0; i < int(nodeTemplate.Number); i++ {
						nodeId := fmt.Sprintf("%s-%d-%d-%d-%d", pool.Name, executorGroupIndex, executorIndex, nodeTemplateIndex, i)
						allocatableByPriorityAndResource := make(map[int32]schedulerobjects.ResourceList)
						for _, priorityClass := range s.schedulingConfig.Preemption.PriorityClasses {
							allocatableByPriorityAndResource[priorityClass.Priority] = nodeTemplate.TotalResources.DeepCopy()
						}
						node := &schedulerobjects.Node{
							Id:                               nodeId,
							Name:                             nodeId,
							Executor:                         executorName,
							Taints:                           slices.Clone(nodeTemplate.Taints),
							Labels:                           maps.Clone(nodeTemplate.Labels),
							TotalResources:                   nodeTemplate.TotalResources.DeepCopy(),
							AllocatableByPriorityAndResource: allocatableByPriorityAndResource,
						}
						txn := nodeDb.Txn(true)
						if err := nodeDb.CreateAndInsertWithApiJobsWithTxn(txn, nil, node); err != nil {
							txn.Abort()
							return err
						}
						txn.Commit()
						s.poolByNodeId[nodeId] = pool.Name
					}
				}
			}
			s.nodeDbByPoolAndExecutorGroup[pool.Name] = append(s.nodeDbByPoolAndExecutorGroup[pool.Name], nodeDb)
			totalResourcesForPool.Add(nodeDb.TotalResources())
		}
		s.totalResourcesByPool[pool.Name] = totalResourcesForPool
	}
	return nil
}

func (s *Simulator) bootstrapWorkload() error {
	// Mark all jobTemplates as active.
	for _, queue := range s.WorkloadSpec.Queues {
		for _, jobTemplate := range queue.JobTemplates {
			s.activeJobTemplatesById[jobTemplate.Id] = jobTemplate
		}
	}

	// Publish submitJob messages for all jobTemplates without dependencies.
	for _, queue := range s.WorkloadSpec.Queues {
		for _, jobTemplate := range queue.JobTemplates {
			if len(jobTemplate.Dependencies) > 0 {
				continue
			}
			eventSequence := &armadaevents.EventSequence{
				Queue:      queue.Name,
				JobSetName: jobTemplate.JobSet,
			}
			for k := 0; k < int(jobTemplate.Number); k++ {
				if len(jobTemplate.Dependencies) > 0 {
					continue
				}
				jobId := util.ULID()
				eventSequence.Events = append(
					eventSequence.Events,
					&armadaevents.EventSequence_Event{
						Created: pointer(s.time.Add(jobTemplate.EarliestSubmitTime)),
						Event: &armadaevents.EventSequence_Event_SubmitJob{
							SubmitJob: submitJobFromJobTemplate(jobId, jobTemplate),
						},
					},
				)
				s.jobTemplateByJobId[jobId.String()] = jobTemplate
			}
			if len(eventSequence.Events) > 0 {
				s.pushEventSequence(eventSequence)
			}
		}
	}

	// Setup the jobTemplate dependency map.
	for _, queue := range s.WorkloadSpec.Queues {
		for _, jobTemplate := range queue.JobTemplates {
			for _, dependencyJobTemplateId := range jobTemplate.Dependencies {
				dependencyJobTemplate, ok := s.activeJobTemplatesById[dependencyJobTemplateId]
				if !ok {
					return errors.Errorf(
						"jobTemplate %s depends on jobTemplate %s, which does not exist",
						jobTemplate.Id, dependencyJobTemplate.Id,
					)
				}
				m := s.jobTemplatesByDependencyIds[dependencyJobTemplateId]
				if m == nil {
					m = make(map[string]*JobTemplate)
					s.jobTemplatesByDependencyIds[dependencyJobTemplateId] = m
				}
				m[jobTemplate.Id] = jobTemplate
			}
		}
	}
	return nil
}

func submitJobFromJobTemplate(jobId ulid.ULID, jobTemplate *JobTemplate) *armadaevents.SubmitJob {
	return &armadaevents.SubmitJob{
		JobId:    armadaevents.ProtoUuidFromUlid(jobId),
		Priority: jobTemplate.QueuePriority,
		MainObject: &armadaevents.KubernetesMainObject{
			ObjectMeta: &armadaevents.ObjectMeta{
				Annotations: jobTemplate.Requirements.Annotations,
			},
			Object: &armadaevents.KubernetesMainObject_PodSpec{
				PodSpec: &armadaevents.PodSpecWithAvoidList{
					PodSpec: &v1.PodSpec{
						NodeSelector:      jobTemplate.Requirements.NodeSelector,
						Affinity:          jobTemplate.Requirements.Affinity,
						Tolerations:       jobTemplate.Requirements.Tolerations,
						PriorityClassName: jobTemplate.PriorityClassName,
						Containers: []v1.Container{
							{
								Resources: jobTemplate.Requirements.ResourceRequirements,
							},
						},
					},
				},
			},
		},
	}
}

func (s *Simulator) pushEventSequence(eventSequence *armadaevents.EventSequence) {
	if len(eventSequence.Events) == 0 {
		return
	}
	heap.Push(
		&s.eventLog,
		Event{
			// We assume that all events in the sequence have the same Created time.
			time:                         *eventSequence.Events[0].Created,
			sequenceNumber:               s.sequenceNumber,
			eventSequenceOrScheduleEvent: eventSequence,
		},
	)
	s.sequenceNumber++
}

func (s *Simulator) pushScheduleEvent(time time.Time) {
	heap.Push(
		&s.eventLog,
		Event{
			time:                         time,
			sequenceNumber:               s.sequenceNumber,
			eventSequenceOrScheduleEvent: scheduleEvent{},
		},
	)
	s.sequenceNumber++
}

func (s *Simulator) handleSimulatorEvent(ctx *armadacontext.Context, event Event) error {
	s.time = event.time
	ctx = armadacontext.New(ctx.Context, ctx.FieldLogger.WithField("simulated time", event.time))
	switch e := event.eventSequenceOrScheduleEvent.(type) {
	case *armadaevents.EventSequence:
		if err := s.handleEventSequence(ctx, e); err != nil {
			return err
		}
	case scheduleEvent:
		if err := s.handleScheduleEvent(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *Simulator) handleScheduleEvent(ctx *armadacontext.Context) error {
	// Schedule the next run of the scheduler, unless there are no more active jobTemplates.
	// TODO: Make timeout configurable.
	if len(s.activeJobTemplatesById) > 0 {
		s.pushScheduleEvent(s.time.Add(10 * time.Second))
	}
	if !s.shouldSchedule {
		return nil
	}

	var eventSequences []*armadaevents.EventSequence
	txn := s.jobDb.WriteTxn()
	defer txn.Abort()
	for _, pool := range s.ClusterSpec.Pools {
		for i := range pool.ClusterGroups {
			nodeDb := s.nodeDbByPoolAndExecutorGroup[pool.Name][i]
			if err := nodeDb.Reset(); err != nil {
				return err
			}
			totalResources := s.totalResourcesByPool[pool.Name]
			fairnessCostProvider, err := fairness.NewDominantResourceFairness(
				totalResources,
				s.schedulingConfig.DominantResourceFairnessResourcesToConsider,
			)
			if err != nil {
				return err
			}
			sctx := schedulercontext.NewSchedulingContext(
				fmt.Sprintf("%s-%d", pool.Name, i),
				pool.Name,
				s.schedulingConfig.Preemption.PriorityClasses,
				s.schedulingConfig.Preemption.DefaultPriorityClass,
				fairnessCostProvider,
				s.limiter,
				totalResources,
			)

			sctx.Started = s.time
			for _, queue := range s.WorkloadSpec.Queues {
				limiter, ok := s.limiterByQueue[queue.Name]
				if !ok {
					limiter = rate.NewLimiter(
						rate.Limit(s.schedulingConfig.MaximumPerQueueSchedulingRate),
						s.schedulingConfig.MaximumPerQueueSchedulingBurst,
					)
					limiter.SetBurstAt(s.time, s.schedulingConfig.MaximumPerQueueSchedulingBurst)
					s.limiterByQueue[queue.Name] = limiter
				}
				err := sctx.AddQueueSchedulingContext(
					queue.Name,
					queue.Weight,
					s.allocationByPoolAndQueueAndPriorityClass[pool.Name][queue.Name],
					limiter,
				)
				if err != nil {
					return err
				}
			}
			constraints := schedulerconstraints.SchedulingConstraintsFromSchedulingConfig(
				pool.Name,
				totalResources,
				// Minimum job size not used for simulation; use taints/tolerations instead.
				schedulerobjects.ResourceList{},
				s.schedulingConfig,
			)
			sch := scheduler.NewPreemptingQueueScheduler(
				sctx,
				constraints,
				s.schedulingConfig.Preemption.NodeEvictionProbability,
				s.schedulingConfig.Preemption.NodeOversubscriptionEvictionProbability,
				s.schedulingConfig.Preemption.ProtectedFractionOfFairShare,
				scheduler.NewSchedulerJobRepositoryAdapter(txn),
				nodeDb,
				// TODO: Necessary to support partial eviction.
				nil,
				nil,
				nil,
			)
			if s.schedulingConfig.EnableNewPreemptionStrategy {
				sch.EnableNewPreemptionStrategy()
			}
			schedulerCtx := ctx
			if s.SuppressSchedulerLogs {
				schedulerCtx = &armadacontext.Context{
					Context:     ctx.Context,
					FieldLogger: logging.NullLogger,
				}
			}
			result, err := sch.Schedule(schedulerCtx)
			if err != nil {
				return err
			}

			// Update jobDb to reflect the decisions by the scheduler.
			// Sort jobs to ensure deterministic event ordering.
			preemptedJobs := scheduler.PreemptedJobsFromSchedulerResult[*jobdb.Job](result)
			scheduledJobs := scheduler.ScheduledJobsFromSchedulerResult[*jobdb.Job](result)
			failedJobs := scheduler.FailedJobsFromSchedulerResult[*jobdb.Job](result)
			less := func(a, b *jobdb.Job) bool {
				if a.Queue() < b.Queue() {
					return true
				} else if a.Queue() > b.Queue() {
					return false
				}
				if a.Id() < b.Id() {
					return true
				} else if a.Id() > b.Id() {
					return false
				}
				return false
			}
			slices.SortFunc(preemptedJobs, less)
			slices.SortFunc(scheduledJobs, less)
			slices.SortFunc(failedJobs, less)
			for i, job := range preemptedJobs {
				if run := job.LatestRun(); run != nil {
					job = job.WithUpdatedRun(run.WithFailed(true))
				} else {
					return errors.Errorf("attempting to preempt job %s with no associated runs", job.Id())
				}
				preemptedJobs[i] = job.WithQueued(false).WithFailed(true)
			}
			for i, job := range scheduledJobs {
				nodeId := result.NodeIdByJobId[job.GetId()]
				if nodeId == "" {
					return errors.Errorf("job %s not mapped to any node", job.GetId())
				}
				if node, err := nodeDb.GetNode(nodeId); err != nil {
					return err
				} else {
					scheduledJobs[i] = job.WithQueued(false).WithNewRun(node.Executor, node.Id, node.Name)
				}
			}
			for i, job := range failedJobs {
				if run := job.LatestRun(); run != nil {
					job = job.WithUpdatedRun(run.WithFailed(true))
				}
				failedJobs[i] = job.WithQueued(false).WithFailed(true)
			}
			if err := txn.Upsert(preemptedJobs); err != nil {
				return err
			}
			if err := txn.Upsert(scheduledJobs); err != nil {
				return err
			}
			if err := txn.Upsert(failedJobs); err != nil {
				return err
			}

			// Update allocation.
			s.allocationByPoolAndQueueAndPriorityClass[pool.Name] = sctx.AllocatedByQueueAndPriority()

			// Generate eventSequences.
			// TODO: Add time taken to run the scheduler to s.time.
			eventSequences, err = scheduler.AppendEventSequencesFromPreemptedJobs(eventSequences, preemptedJobs, s.time)
			if err != nil {
				return err
			}
			eventSequences, err = scheduler.AppendEventSequencesFromScheduledJobs(eventSequences, scheduledJobs, s.time)
			if err != nil {
				return err
			}
			eventSequences, err = scheduler.AppendEventSequencesFromUnschedulableJobs(eventSequences, failedJobs, s.time)
			if err != nil {
				return err
			}

			// If nothing changed, we're in steady state and can safely skip scheduling until something external has changed.
			// Do this only if a non-zero amount of time has passed.
			if !s.time.Equal(time.Time{}) && len(result.ScheduledJobs) == 0 && len(result.PreemptedJobs) == 0 && len(result.FailedJobs) == 0 {
				s.shouldSchedule = false
			}
		}
	}
	txn.Commit()

	// Publish simulator events.
	for _, eventSequence := range eventSequences {
		s.pushEventSequence(eventSequence)
	}
	return nil
}

// TODO: Write events to disk unless they should be discarded.
func (s *Simulator) handleEventSequence(ctx *armadacontext.Context, es *armadaevents.EventSequence) error {
	txn := s.jobDb.WriteTxn()
	defer txn.Abort()
	eventsToPublish := make([]*armadaevents.EventSequence_Event, 0, len(es.Events))
	for _, event := range es.Events {
		var ok bool
		var err error = nil
		switch eventType := event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_SubmitJob:
			s.shouldSchedule = true
			ok, err = s.handleSubmitJob(txn, event.GetSubmitJob(), *event.Created, es)
		case *armadaevents.EventSequence_Event_JobRunLeased:
			ok, err = s.handleJobRunLeased(txn, event.GetJobRunLeased())
		case *armadaevents.EventSequence_Event_JobSucceeded:
			s.shouldSchedule = true
			ok, err = s.handleJobSucceeded(txn, event.GetJobSucceeded())
		case *armadaevents.EventSequence_Event_JobRunPreempted:
			s.shouldSchedule = true
			ok, err = s.handleJobRunPreempted(txn, event.GetJobRunPreempted())
		case *armadaevents.EventSequence_Event_JobRunErrors:
			for _, e := range event.GetJobRunErrors().Errors {
				if e.GetJobRunPreemptedError() == nil {
					return errors.Errorf("received unexpected JobRunErrors reason: %T", e.Reason)
				}
			}
		case *armadaevents.EventSequence_Event_JobErrors:
			for _, e := range event.GetJobErrors().Errors {
				if e.GetJobRunPreemptedError() == nil {
					return errors.Errorf("received unexpected JobErrors reason: %T", e.Reason)
				}
			}
		default:
			// This is an event type we haven't consider; log a warning.
			return errors.Errorf("received unknown event type %T", eventType)
		}
		if err != nil {
			return err
		}
		if ok {
			eventsToPublish = append(eventsToPublish, event)
		}
	}
	txn.Commit()
	es.Events = eventsToPublish
	if len(es.Events) > 0 {
		for _, c := range s.outputs {
			c <- es
		}
	}
	return nil
}

func (s *Simulator) handleSubmitJob(txn *jobdb.Txn, e *armadaevents.SubmitJob, time time.Time, eventSequence *armadaevents.EventSequence) (bool, error) {
	schedulingInfo, err := scheduleringester.SchedulingInfoFromSubmitJob(e, time, s.schedulingConfig.Preemption.PriorityClasses)
	if err != nil {
		return false, err
	}
	job := s.jobDb.NewJob(
		armadaevents.UlidFromProtoUuid(e.JobId).String(),
		eventSequence.JobSetName,
		eventSequence.Queue,
		e.Priority,
		schedulingInfo,
		true,
		0,
		false,
		false,
		false,
		s.logicalJobCreatedTimestamp.Add(1),
	)
	if err := txn.Upsert([]*jobdb.Job{job}); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Simulator) handleJobRunLeased(txn *jobdb.Txn, e *armadaevents.JobRunLeased) (bool, error) {
	jobId := armadaevents.UlidFromProtoUuid(e.JobId).String()
	job := txn.GetById(jobId)
	jobTemplate := s.jobTemplateByJobId[jobId]
	if jobTemplate == nil {
		return false, errors.Errorf("no jobTemplate associated with job %s", jobId)
	}
	jobSuccessTime := s.time
	jobSuccessTime = jobSuccessTime.Add(s.generateRandomShiftedExponentialDuration(s.ClusterSpec.PendingDelayDistribution))
	jobSuccessTime = jobSuccessTime.Add(s.generateRandomShiftedExponentialDuration(jobTemplate.RuntimeDistribution))
	s.pushEventSequence(
		&armadaevents.EventSequence{
			Queue:      job.Queue(),
			JobSetName: job.Jobset(),
			Events: []*armadaevents.EventSequence_Event{
				{
					Created: &jobSuccessTime,
					Event: &armadaevents.EventSequence_Event_JobSucceeded{
						JobSucceeded: &armadaevents.JobSucceeded{
							JobId: e.JobId,
						},
					},
				},
			},
		},
	)
	return true, nil
}

func (s *Simulator) generateRandomShiftedExponentialDuration(rv ShiftedExponential) time.Duration {
	return generateRandomShiftedExponentialDuration(s.rand, rv)
}

func generateRandomShiftedExponentialDuration(r *rand.Rand, rv ShiftedExponential) time.Duration {
	if rv.TailMean == 0 {
		return rv.Minimum
	} else {
		return rv.Minimum + time.Duration(r.ExpFloat64()*float64(rv.TailMean))
	}
}

func (s *Simulator) handleJobSucceeded(txn *jobdb.Txn, e *armadaevents.JobSucceeded) (bool, error) {
	jobId := armadaevents.UlidFromProtoUuid(e.JobId).String()
	job := txn.GetById(jobId)
	if job == nil || job.InTerminalState() {
		// Job already terminated; nothing more to do.
		return false, nil
	}
	if err := txn.BatchDelete([]string{jobId}); err != nil {
		return false, err
	}

	// Subtract the allocation of this job from the queue allocation.
	run := job.LatestRun()
	pool := s.poolByNodeId[run.NodeId()]
	s.allocationByPoolAndQueueAndPriorityClass[pool][job.Queue()].SubV1ResourceList(
		job.GetPriorityClassName(),
		job.GetResourceRequirements().Requests,
	)

	// Unbind the job from the node on which it was scheduled.
	if err := s.unbindRunningJob(job); err != nil {
		return false, errors.WithMessagef(err, "failed to unbind job %s", job.Id())
	}

	// Increase the successful job count for this jobTemplate.
	// If all jobs created from this template have succeeded, update dependent templates
	// and submit any templates for which this was the last dependency.
	jobTemplate := s.jobTemplateByJobId[job.GetId()]
	jobTemplate.NumberSuccessful++
	if jobTemplate.Number == jobTemplate.NumberSuccessful {
		delete(s.activeJobTemplatesById, jobTemplate.Id)
		for _, dependentJobTemplate := range s.jobTemplatesByDependencyIds[jobTemplate.Id] {
			i := slices.Index(dependentJobTemplate.Dependencies, jobTemplate.Id)
			dependentJobTemplate.Dependencies = slices.Delete(dependentJobTemplate.Dependencies, i, i+1)
			if len(dependentJobTemplate.Dependencies) > 0 {
				continue
			}
			eventSequence := &armadaevents.EventSequence{
				Queue:      dependentJobTemplate.Queue,
				JobSetName: dependentJobTemplate.JobSet,
			}
			for k := 0; k < int(dependentJobTemplate.Number); k++ {
				jobId := util.ULID()
				eventSequence.Events = append(
					eventSequence.Events,
					&armadaevents.EventSequence_Event{
						// EarliestSubmitTimeFromDependencyCompletion must be positive
						Created: pointer(maxTime(time.Time{}.Add(dependentJobTemplate.EarliestSubmitTime), s.time.Add(dependentJobTemplate.EarliestSubmitTimeFromDependencyCompletion))),
						Event: &armadaevents.EventSequence_Event_SubmitJob{
							SubmitJob: submitJobFromJobTemplate(jobId, dependentJobTemplate),
						},
					},
				)
				s.jobTemplateByJobId[jobId.String()] = dependentJobTemplate
			}
			if len(eventSequence.Events) > 0 {
				s.pushEventSequence(eventSequence)
			}
		}
		delete(s.jobTemplatesByDependencyIds, jobTemplate.Id)
	}
	return true, nil
}

func (s *Simulator) unbindRunningJob(job *jobdb.Job) error {
	if job.InTerminalState() {
		return errors.Errorf("job %s has terminated", job.Id())
	}
	run := job.LatestRun()
	if run == nil {
		return errors.Errorf("job %s has no runs associated with it", job.Id())
	}
	if run.Executor() == "" {
		return errors.Errorf("empty executor for run %s of job %s", run.Id(), job.Id())
	}
	if run.NodeId() == "" {
		return errors.Errorf("empty nodeId for run %s of job %s", run.Id(), job.Id())
	}
	nodeDb := s.nodeDbByExecutorName[run.Executor()]
	node, err := nodeDb.GetNode(run.NodeId())
	if err != nil {
		return err
	} else if node == nil {
		return errors.Errorf("node %s not found", run.NodeId())
	}
	node, err = nodedb.UnbindJobFromNode(s.schedulingConfig.Preemption.PriorityClasses, job, node)
	if err != nil {
		return err
	}
	if err := nodeDb.Upsert(node); err != nil {
		return err
	}
	return nil
}

func (s *Simulator) handleJobRunPreempted(txn *jobdb.Txn, e *armadaevents.JobRunPreempted) (bool, error) {
	jobId := armadaevents.UlidFromProtoUuid(e.PreemptedJobId).String()
	job := txn.GetById(jobId)

	// Submit a retry for this job.
	jobTemplate := s.jobTemplateByJobId[job.GetId()]
	retryJobId := util.ULID()
	resubmitTime := s.time.Add(s.generateRandomShiftedExponentialDuration(s.ClusterSpec.WorkflowManagerDelayDistribution))
	s.pushEventSequence(
		&armadaevents.EventSequence{
			Queue:      job.Queue(),
			JobSetName: job.Jobset(),
			Events: []*armadaevents.EventSequence_Event{
				{
					Created: &resubmitTime,
					Event: &armadaevents.EventSequence_Event_SubmitJob{
						SubmitJob: submitJobFromJobTemplate(retryJobId, jobTemplate),
					},
				},
			},
		},
	)
	s.jobTemplateByJobId[retryJobId.String()] = jobTemplate
	return true, nil
}

func maxTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return b
	}
	return a
}

func pointer[T any](t T) *T {
	return &t
}
