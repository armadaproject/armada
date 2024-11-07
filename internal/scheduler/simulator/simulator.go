package simulator

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/logging"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/simulator/model"
	"github.com/armadaproject/armada/internal/scheduler/simulator/sink"
	"github.com/armadaproject/armada/internal/scheduleringester"
	serverconfig "github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const clusterLabel = "armadaproject.io/clusterName"

var epochStart = time.Unix(0, 0).UTC()

type accounting struct {
	// Map from node id to the pool to which the node belongs.
	poolByNodeId map[string]string
	// Separate nodeDb per pool
	nodeDbByPool map[string]*nodedb.NodeDb
	// Allocation by pool for each queue and priority class.
	// Stored across invocations of the scheduler.
	allocationByPoolAndQueueAndPriorityClass map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string]
	// Demand for each queue
	demandByQueue map[string]schedulerobjects.ResourceList
	// Total resources across all executorGroups for each pool.
	totalResourcesByPool map[string]schedulerobjects.ResourceList
	// Mapping of job Id -> nodeId.  Needed by preemptingqueuescheduler for gang preemption.
	nodeIdByJobId map[string]string
	// Mapping of gangId -> jobsINGang.  Needed by preemptingqueuescheduler for gang preemption.
	jobIdsByGangId map[string]map[string]bool
	// Mapping of jobId -> gangId.  Needed by preemptingqueuescheduler for gang preemption.
	gangIdByJobId map[string]string
}

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
	jobDb          *jobdb.JobDb
	shouldSchedule bool
	// Current simulated time.
	time time.Time
	// Sequence number of the next event to be published.
	sequenceNumber int
	// Events stored in a priority queue ordered first by timestamp and second by sequence number.
	eventLog EventLog
	// Simulated events are emitted on these event channels.
	// Create a channel by calling s.StateTransitions() before running the simulator.
	stateTransitionChannels []chan model.StateTransition
	// Global job scheduling rate-limiter. Note that this will always be set to unlimited as we do not yet support
	// effective rate limiting based on simulated time.
	limiter *rate.Limiter
	// Used to generate random numbers from a chosen seed.
	rand *rand.Rand
	// Used to ensure each job is given a unique time stamp.
	logicalJobCreatedTimestamp atomic.Int64
	// If true, scheduler logs are omitted.
	// This since the logs are very verbose when scheduling large numbers of jobs.
	SuppressSchedulerLogs bool
	// For making internaltypes.ResourceList
	resourceListFactory *internaltypes.ResourceListFactory
	// Skips schedule events when we're in a steady state
	enableFastForward bool
	// Limit the time simulated
	hardTerminationMinutes int
	// Determines how often we trigger schedule events
	schedulerCyclePeriodSeconds int
	// Used to exhaust events
	sink sink.Sink
	// Floating resource info
	floatingResourceTypes *floatingresources.FloatingResourceTypes
	// Keeps track of what's allocated where
	accounting accounting
}

func NewSimulator(
	clusterSpec *ClusterSpec,
	workloadSpec *WorkloadSpec,
	schedulingConfig configuration.SchedulingConfig,
	enableFastForward bool,
	hardTerminationMinutes int,
	schedulerCyclePeriodSeconds int,
	sink sink.Sink,
) (*Simulator, error) {
	resourceListFactory, err := internaltypes.NewResourceListFactory(
		schedulingConfig.SupportedResourceTypes,
		schedulingConfig.ExperimentalFloatingResources,
	)
	if err != nil {
		return nil, errors.WithMessage(err, "Error with the .scheduling.supportedResourceTypes field in config")
	}

	floatingResourceTypes, err := floatingresources.NewFloatingResourceTypes(schedulingConfig.ExperimentalFloatingResources)
	if err != nil {
		return nil, err
	}

	initialiseWorkloadSpec(workloadSpec)
	if err := validateClusterSpec(clusterSpec); err != nil {
		return nil, err
	}
	if err := validateWorkloadSpec(workloadSpec); err != nil {
		return nil, err
	}
	workloadSpec = expandRepeatingTemplates(workloadSpec)
	jobDb := jobdb.NewJobDb(
		schedulingConfig.PriorityClasses,
		schedulingConfig.DefaultPriorityClassName,
		stringinterner.New(1024),
		resourceListFactory,
	)
	randomSeed := workloadSpec.RandomSeed
	if randomSeed == 0 {
		// Seed the RNG using the local time if no explicit random seed is provided.
		randomSeed = time.Now().Unix()
	}
	s := &Simulator{
		ClusterSpec:                 clusterSpec,
		WorkloadSpec:                workloadSpec,
		schedulingConfig:            schedulingConfig,
		jobTemplateByJobId:          make(map[string]*JobTemplate),
		jobTemplatesByDependencyIds: make(map[string]map[string]*JobTemplate),
		activeJobTemplatesById:      make(map[string]*JobTemplate),
		jobDb:                       jobDb,
		limiter:                     rate.NewLimiter(rate.Inf, math.MaxInt), // Unlimited
		rand:                        rand.New(rand.NewSource(randomSeed)),
		resourceListFactory:         resourceListFactory,
		enableFastForward:           enableFastForward,
		hardTerminationMinutes:      hardTerminationMinutes,
		schedulerCyclePeriodSeconds: schedulerCyclePeriodSeconds,
		floatingResourceTypes:       floatingResourceTypes,
		time:                        epochStart,
		sink:                        sink,
		accounting: accounting{
			nodeDbByPool:                             make(map[string]*nodedb.NodeDb),
			poolByNodeId:                             make(map[string]string),
			allocationByPoolAndQueueAndPriorityClass: make(map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string]),
			demandByQueue:                            make(map[string]schedulerobjects.ResourceList),
			totalResourcesByPool:                     make(map[string]schedulerobjects.ResourceList),
			nodeIdByJobId:                            make(map[string]string),
			jobIdsByGangId:                           make(map[string]map[string]bool),
			gangIdByJobId:                            make(map[string]string),
		},
	}
	jobDb.SetClock(s)
	if err := s.setupClusters(); err != nil {
		return nil, err
	}
	if err := s.bootstrapWorkload(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Simulator) Now() time.Time {
	return s.time
}

func (s *Simulator) Since(t time.Time) time.Duration {
	return s.Now().Sub(t)
}

// Run runs the scheduler until all jobs have finished successfully.
func (s *Simulator) Run(ctx *armadacontext.Context) error {
	startTime := time.Now()
	defer func() {
		for _, c := range s.stateTransitionChannels {
			close(c)
		}
	}()
	// Bootstrap the simulator by pushing an event that triggers a scheduler run.
	s.pushScheduleEvent(s.time)

	simTerminationTime := s.time.Add(100 * 365 * 24 * time.Hour)
	if s.hardTerminationMinutes > 0 {
		simTerminationTime := s.time.Add(time.Minute * time.Duration(s.hardTerminationMinutes))
		ctx.Infof("Will stop simulating at %s", simTerminationTime)
	} else {
		ctx.Infof("No termination time set, will run until all workloads have completed")
	}

	lastLogTime := time.Now()
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
		if time.Now().Unix()-lastLogTime.Unix() >= 5 {
			ctx.Infof("Simulator time %s", s.time)
			lastLogTime = s.time
		}
		if s.time.After(simTerminationTime) {
			ctx.Infof("Current simulated time (%s) exceeds runtime deadline (%s). Terminating", s.time, simTerminationTime)
			return nil
		}
	}
	ctx.Infof("All workloads complete at %s. Simulation took %s", s.time, time.Since(startTime))
	return nil
}

// StateTransitions returns a channel on which all simulated events are sent.
// This function must be called before *Simulator.Run.
func (s *Simulator) StateTransitions() <-chan model.StateTransition {
	c := make(chan model.StateTransition, 128)
	s.stateTransitionChannels = append(s.stateTransitionChannels, c)
	return c
}

func validateClusterSpec(clusterSpec *ClusterSpec) error {
	executorNames := map[string]bool{}
	for _, cluster := range clusterSpec.Clusters {

		if cluster.Name == "" {
			return errors.Errorf("cluster name cannot be empty")
		}
		if cluster.Pool == "" {
			return errors.Errorf("cluster %v has no pool set", cluster.Name)
		}
		_, exists := executorNames[cluster.Name]
		if exists {
			return errors.Errorf("duplicate cluster name: %v", cluster.Name)
		}
		executorNames[cluster.Name] = true
	}
	return nil
}

func validateWorkloadSpec(workloadSpec *WorkloadSpec) error {
	queueNames := armadaslices.Map(workloadSpec.Queues, func(queue *Queue) string { return queue.Name })
	if !slices.Equal(queueNames, armadaslices.Unique(queueNames)) {
		return errors.Errorf("duplicate queue name: %v", queueNames)
	}
	jobTemplateIdSlices := armadaslices.Map(workloadSpec.Queues, func(queue *Queue) []string {
		return armadaslices.Map(queue.JobTemplates, func(template *JobTemplate) string { return template.Id })
	})
	jobTemplateIds := make([]string, 0)
	for _, singleQueueTemplateIds := range jobTemplateIdSlices {
		jobTemplateIds = append(jobTemplateIds, singleQueueTemplateIds...)
	}
	if !slices.Equal(jobTemplateIds, armadaslices.Unique(jobTemplateIds)) {
		return errors.Errorf("duplicate job template ids: %v", jobTemplateIds)
	}
	for _, queue := range workloadSpec.Queues {
		for _, template := range queue.JobTemplates {
			// Confirm that we can create an exact number of gang jobs
			if template.GangCardinality != 0 && int(template.Number)%int(template.GangCardinality) != 0 {
				return errors.Errorf("template.Number [%d] is not exactly divisible by template.GangCardinality [%d]", template.Number, template.GangCardinality)
			}
			if template.Repeat != nil {
				if template.Repeat.Period == nil {
					return errors.Errorf("template.Repeat.Period is unset")
				}
				if template.Repeat.NumTimes < 1 {
					return errors.Errorf("template.Repeat.NumTimes must be greater than 0")
				}
			}
		}
	}
	return nil
}

func (s *Simulator) setupClusters() error {
	indexedNodeLabels := s.schedulingConfig.IndexedNodeLabels
	if indexedNodeLabels == nil {
		indexedNodeLabels = []string{}
	}
	if !slices.Contains(indexedNodeLabels, clusterLabel) {
		indexedNodeLabels = append(indexedNodeLabels, clusterLabel)
	}

	nodeFactory := internaltypes.NewNodeFactory(s.schedulingConfig.IndexedTaints,
		indexedNodeLabels,
		s.resourceListFactory)

	for _, cluster := range s.ClusterSpec.Clusters {
		nodeDb, ok := s.accounting.nodeDbByPool[cluster.Pool]
		if !ok {
			newNodeDb, err := nodedb.NewNodeDb(
				s.schedulingConfig.PriorityClasses,
				s.schedulingConfig.IndexedResources,
				s.schedulingConfig.IndexedTaints,
				indexedNodeLabels,
				s.schedulingConfig.WellKnownNodeTypes,
				s.resourceListFactory,
			)
			if err != nil {
				return err
			}
			nodeDb = newNodeDb
			s.accounting.nodeDbByPool[cluster.Pool] = nodeDb
		}

		totalResourcesForPool, ok := s.accounting.totalResourcesByPool[cluster.Pool]
		if !ok {
			totalResourcesForPool = schedulerobjects.ResourceList{}
		}

		for nodeTemplateIndex, nodeTemplate := range cluster.NodeTemplates {
			labels := map[string]string{}
			if nodeTemplate.Labels != nil {
				labels = maps.Clone(nodeTemplate.Labels)
			}
			labels[clusterLabel] = cluster.Name
			for i := 0; i < int(nodeTemplate.Number); i++ {
				nodeId := fmt.Sprintf("%s-%d-%d", cluster.Name, nodeTemplateIndex, i)
				node := &schedulerobjects.Node{
					Id:             nodeId,
					Name:           nodeId,
					Executor:       cluster.Name,
					Pool:           cluster.Pool,
					Taints:         slices.Clone(nodeTemplate.Taints),
					Labels:         labels,
					TotalResources: nodeTemplate.TotalResources.DeepCopy(),
					AllocatableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
						types.AllowedPriorities(s.schedulingConfig.PriorityClasses),
						nodeTemplate.TotalResources,
					),
				}
				dbNode, err := nodeFactory.FromSchedulerObjectsNode(node)
				if err != nil {
					return err
				}

				txn := nodeDb.Txn(true)
				if err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, dbNode); err != nil {
					txn.Abort()
					return err
				}
				txn.Commit()
				s.accounting.poolByNodeId[nodeId] = cluster.Pool
			}
		}
		totalResourcesForPool.Add(nodeDb.TotalKubernetesResources())
		s.accounting.totalResourcesByPool[cluster.Pool] = totalResourcesForPool
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
			gangId := ""
			gangCardinality := int(jobTemplate.GangCardinality)
			for k := 0; k < int(jobTemplate.Number); k++ {
				if len(jobTemplate.Dependencies) > 0 {
					continue
				}
				jobId := util.NewULID()
				if gangCardinality != 0 && k%gangCardinality == 0 {
					gangId = fmt.Sprintf("%s-0", util.NewULID())
				}
				eventSequence.Events = append(
					eventSequence.Events,
					&armadaevents.EventSequence_Event{
						Created: protoutil.ToTimestamp(s.time.Add(jobTemplate.EarliestSubmitTime)),
						Event: &armadaevents.EventSequence_Event_SubmitJob{
							SubmitJob: submitJobFromJobTemplate(jobId, jobTemplate, gangId),
						},
					},
				)
				s.jobTemplateByJobId[jobId] = jobTemplate
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

func submitJobFromJobTemplate(jobId string, jobTemplate *JobTemplate, gangId string) *armadaevents.SubmitJob {
	annotations := jobTemplate.Requirements.Annotations
	if annotations == nil {
		annotations = map[string]string{}
	}

	if jobTemplate.GangCardinality > 0 {
		annotations[serverconfig.GangIdAnnotation] = gangId
		annotations[serverconfig.GangCardinalityAnnotation] = strconv.Itoa(int(jobTemplate.GangCardinality))
		if jobTemplate.GangNodeUniformityLabel != "" {
			annotations[serverconfig.GangNodeUniformityLabelAnnotation] = jobTemplate.GangNodeUniformityLabel
		} else {
			annotations[serverconfig.GangNodeUniformityLabelAnnotation] = "armadaproject.io/clusterName"
		}
	}

	return &armadaevents.SubmitJob{
		JobId:    jobId,
		Priority: jobTemplate.QueuePriority,
		MainObject: &armadaevents.KubernetesMainObject{
			ObjectMeta: &armadaevents.ObjectMeta{
				Annotations: annotations,
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
			time:                         protoutil.ToStdTime(eventSequence.Events[0].Created),
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
		s.pushScheduleEvent(s.time.Add(time.Duration(s.schedulerCyclePeriodSeconds) * time.Second))
	}
	if !s.shouldSchedule && s.enableFastForward {
		return nil
	}

	var eventSequences []*armadaevents.EventSequence
	txn := s.jobDb.WriteTxn()
	defer txn.Abort()
	for pool, nodeDb := range s.accounting.nodeDbByPool {
		if err := nodeDb.Reset(); err != nil {
			return err
		}
		totalResources := s.accounting.totalResourcesByPool[pool]
		fairnessCostProvider, err := fairness.NewDominantResourceFairness(
			totalResources,
			s.schedulingConfig,
		)
		if err != nil {
			return err
		}
		sctx := schedulercontext.NewSchedulingContext(
			pool,
			fairnessCostProvider,
			s.limiter,
			totalResources,
		)

		sctx.Started = s.time
		for _, queue := range s.WorkloadSpec.Queues {
			demand, hasDemand := s.accounting.demandByQueue[queue.Name]
			if !hasDemand {
				// To ensure fair share is computed only from active queues, i.e., queues with jobs queued or running.
				continue
			}
			err := sctx.AddQueueSchedulingContext(
				queue.Name,
				queue.Weight,
				s.accounting.allocationByPoolAndQueueAndPriorityClass[pool][queue.Name],
				demand,
				demand,
				s.limiter,
			)
			if err != nil {
				return err
			}
		}
		sctx.UpdateFairShares()
		constraints := schedulerconstraints.NewSchedulingConstraints(pool, totalResources, s.schedulingConfig, nil)
		sch := scheduling.NewPreemptingQueueScheduler(
			sctx,
			constraints,
			s.floatingResourceTypes,
			s.schedulingConfig.ProtectedFractionOfFairShare,
			s.schedulingConfig.MaxQueueLookback,
			txn,
			nodeDb,
			maps.Clone(s.accounting.nodeIdByJobId),
			maps.Clone(s.accounting.jobIdsByGangId),
			maps.Clone(s.accounting.gangIdByJobId),
		)

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

		err = s.sink.OnCycleEnd(s.time, result)
		if err != nil {
			return err
		}

		// Update jobDb to reflect the decisions by the scheduler.
		// Sort jobs to ensure deterministic event ordering.
		preemptedJobs := slices.Clone(result.PreemptedJobs)
		scheduledJobs := slices.Clone(result.ScheduledJobs)

		lessJob := func(a, b *jobdb.Job) int {
			if a.Queue() < b.Queue() {
				return -1
			} else if a.Queue() > b.Queue() {
				return 1
			}
			if a.Id() < b.Id() {
				return -1
			} else if a.Id() > b.Id() {
				return 1
			}
			return 0
		}
		slices.SortFunc(preemptedJobs, func(a, b *schedulercontext.JobSchedulingContext) int {
			return lessJob(a.Job, b.Job)
		})
		slices.SortFunc(scheduledJobs, func(a, b *schedulercontext.JobSchedulingContext) int {
			return lessJob(a.Job, b.Job)
		})
		for i, jctx := range preemptedJobs {
			job := jctx.Job
			delete(s.accounting.nodeIdByJobId, job.Id())
			if run := job.LatestRun(); run != nil {
				job = job.WithUpdatedRun(run.WithFailed(true))
			} else {
				return errors.Errorf("attempting to preempt job %s with no associated runs", job.Id())
			}
			preemptedJobs[i].Job = job.WithQueued(false).WithFailed(true)
		}
		for i, jctx := range scheduledJobs {
			job := jctx.Job
			nodeId := result.NodeIdByJobId[job.Id()]
			if nodeId == "" {
				return errors.Errorf("job %s not mapped to a node", job.Id())
			}
			if node, err := nodeDb.GetNode(nodeId); err != nil {
				return err
			} else {
				s.accounting.nodeIdByJobId[job.Id()] = nodeId
				priority, ok := nodeDb.GetScheduledAtPriority(job.Id())
				if !ok {
					return errors.Errorf("job %s not mapped to a priority", job.Id())
				}
				scheduledJobs[i].Job = job.WithQueued(false).WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), node.GetPool(), priority)
			}
		}
		if err := txn.Upsert(armadaslices.Map(preemptedJobs, func(jctx *schedulercontext.JobSchedulingContext) *jobdb.Job { return jctx.Job })); err != nil {
			return err
		}
		if err := txn.Upsert(armadaslices.Map(scheduledJobs, func(jctx *schedulercontext.JobSchedulingContext) *jobdb.Job { return jctx.Job })); err != nil {
			return err
		}

		// Update allocation.
		s.accounting.allocationByPoolAndQueueAndPriorityClass[pool] = sctx.AllocatedByQueueAndPriority()

		// Generate eventSequences.
		eventSequences, err = scheduler.AppendEventSequencesFromPreemptedJobs(eventSequences, result.PreemptedJobs, s.time)
		if err != nil {
			return err
		}
		eventSequences, err = scheduler.AppendEventSequencesFromScheduledJobs(eventSequences, scheduledJobs)
		if err != nil {
			return err
		}

		// Update event timestamps to be consistent with simulated time.
		for _, eventSequence := range eventSequences {
			for _, event := range eventSequence.Events {
				event.Created = protoutil.ToTimestamp(s.time)
			}
		}

		// If nothing changed, we're in steady state and can safely skip scheduling until something external has changed.
		// Do this only if a non-zero amount of time has passed.
		if !s.time.Equal(epochStart) && len(result.ScheduledJobs) == 0 && len(result.PreemptedJobs) == 0 {
			s.shouldSchedule = false
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
func (s *Simulator) handleEventSequence(_ *armadacontext.Context, es *armadaevents.EventSequence) error {
	txn := s.jobDb.WriteTxn()
	defer txn.Abort()
	eventsToPublish := make([]*armadaevents.EventSequence_Event, 0, len(es.Events))
	jobs := make([]*jobdb.Job, len(es.Events))
	for i, event := range es.Events {
		var err error
		var shouldPublish bool
		switch eventType := event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_SubmitJob:
			s.shouldSchedule = true
			jobs[i], shouldPublish, err = s.handleSubmitJob(txn, event.GetSubmitJob(), protoutil.ToStdTime(event.Created), es)
		case *armadaevents.EventSequence_Event_JobRunLeased:
			jobs[i], shouldPublish, err = s.handleJobRunLeased(txn, event.GetJobRunLeased())
		case *armadaevents.EventSequence_Event_JobSucceeded:
			s.shouldSchedule = true
			jobs[i], shouldPublish, err = s.handleJobSucceeded(txn, event.GetJobSucceeded())
		case *armadaevents.EventSequence_Event_JobRunPreempted:
			s.shouldSchedule = true
			jobs[i], shouldPublish, err = s.handleJobRunPreempted(txn, event.GetJobRunPreempted())
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
			// This is an event type we haven't considered
			return errors.Errorf("received unknown event type %T", eventType)
		}
		if err != nil {
			return err
		}

		if shouldPublish {
			eventsToPublish = append(eventsToPublish, event)
		}
	}
	txn.Commit()
	es.Events = eventsToPublish
	if len(es.Events) > 0 {
		stateTransition := model.StateTransition{
			Jobs:          jobs,
			EventSequence: es,
		}
		err := s.sink.OnNewStateTransitions([]*model.StateTransition{&stateTransition})
		if err != nil {
			return err
		}
		for _, c := range s.stateTransitionChannels {
			c <- stateTransition
		}
	}
	return nil
}

func (s *Simulator) handleSubmitJob(txn *jobdb.Txn, e *armadaevents.SubmitJob, time time.Time, eventSequence *armadaevents.EventSequence) (*jobdb.Job, bool, error) {
	schedulingInfo, err := scheduleringester.SchedulingInfoFromSubmitJob(e, time)
	if err != nil {
		return nil, false, err
	}
	poolNames := make([]string, 0, len(s.ClusterSpec.Clusters))
	for _, cluster := range s.ClusterSpec.Clusters {
		poolNames = append(poolNames, cluster.Pool)
	}
	job, err := s.jobDb.NewJob(
		e.JobId,
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
		false,
		poolNames,
	)
	if err != nil {
		return nil, false, err
	}
	s.addJobToDemand(job)
	gangInfo, err := schedulercontext.GangInfoFromLegacySchedulerJob(job)
	if err != nil {
		return nil, false, err
	}
	if gangInfo.Cardinality > 1 {
		gangIds := s.accounting.jobIdsByGangId[gangInfo.Id]
		if gangIds == nil {
			gangIds = make(map[string]bool, gangInfo.Cardinality)
			s.accounting.jobIdsByGangId[gangInfo.Id] = gangIds
		}
		gangIds[job.Id()] = true
		s.accounting.gangIdByJobId[job.Id()] = gangInfo.Id
	}
	if err := txn.Upsert([]*jobdb.Job{job}); err != nil {
		return nil, false, err
	}
	return job, true, nil
}

func (s *Simulator) handleJobRunLeased(txn *jobdb.Txn, e *armadaevents.JobRunLeased) (*jobdb.Job, bool, error) {
	jobId := e.JobId
	job := txn.GetById(jobId)
	jobTemplate := s.jobTemplateByJobId[jobId]
	if jobTemplate == nil {
		return nil, false, errors.Errorf("no jobTemplate associated with job %s", jobId)
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
					Created: protoutil.ToTimestamp(jobSuccessTime),
					Event: &armadaevents.EventSequence_Event_JobSucceeded{
						JobSucceeded: &armadaevents.JobSucceeded{
							JobId: e.JobId,
						},
					},
				},
			},
		},
	)

	updatedJob := job.WithUpdatedRun(job.LatestRun().WithRunning(true).WithPool(e.Pool))
	if err := txn.Upsert([]*jobdb.Job{updatedJob}); err != nil {
		return nil, false, err
	}
	return updatedJob, true, nil
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

func (s *Simulator) handleJobSucceeded(txn *jobdb.Txn, e *armadaevents.JobSucceeded) (*jobdb.Job, bool, error) {
	jobId := e.JobId
	job := txn.GetById(jobId)
	if job == nil || job.InTerminalState() {
		// Job already terminated; nothing more to do.
		return nil, false, nil
	}

	delete(s.accounting.nodeIdByJobId, job.Id())
	delete(s.accounting.gangIdByJobId, job.Id())
	gangInfo, err := schedulercontext.GangInfoFromLegacySchedulerJob(job)
	if err != nil {
		return nil, false, err
	}
	if gangInfo.Cardinality > 1 {
		gangIds := s.accounting.jobIdsByGangId[gangInfo.Id]
		if gangIds != nil {
			delete(s.accounting.jobIdsByGangId[gangInfo.Id], jobId)
		}
		s.accounting.gangIdByJobId[job.Id()] = gangInfo.Id
	}
	if err := txn.BatchDelete([]string{jobId}); err != nil {
		return nil, false, err
	}

	// Subtract the allocation of this job from the queue allocation.
	run := job.LatestRun()
	pool := s.accounting.poolByNodeId[run.NodeId()]
	s.accounting.allocationByPoolAndQueueAndPriorityClass[pool][job.Queue()].SubV1ResourceList(
		job.PriorityClassName(),
		job.ResourceRequirements().Requests,
	)
	s.removeJobFromDemand(job)

	// Unbind the job from the node on which it was scheduled.
	if err := s.unbindRunningJob(job); err != nil {
		return nil, false, errors.WithMessagef(err, "failed to unbind job %s", job.Id())
	}

	// Increase the successful job count for this jobTemplate.
	// If all jobs created from this template have succeeded, update dependent templates
	// and submit any templates for which this was the last dependency.
	jobTemplate := s.jobTemplateByJobId[job.Id()]
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
			gangId := ""
			gangCardinality := int(dependentJobTemplate.GangCardinality)
			for k := 0; k < int(dependentJobTemplate.Number); k++ {
				jobId := util.NewULID()
				if gangCardinality != 0 && k%gangCardinality == 0 {
					gangId = fmt.Sprintf("%s-0", util.NewULID())
				}
				eventSequence.Events = append(
					eventSequence.Events,
					&armadaevents.EventSequence_Event{
						// EarliestSubmitTimeFromDependencyCompletion must be positive
						Created: protoutil.ToTimestamp(maxTime(time.Time{}.Add(dependentJobTemplate.EarliestSubmitTime), s.time.Add(dependentJobTemplate.EarliestSubmitTimeFromDependencyCompletion))),
						Event: &armadaevents.EventSequence_Event_SubmitJob{
							SubmitJob: submitJobFromJobTemplate(jobId, dependentJobTemplate, gangId),
						},
					},
				)
				s.jobTemplateByJobId[jobId] = dependentJobTemplate
			}
			if len(eventSequence.Events) > 0 {
				s.pushEventSequence(eventSequence)
			}
		}
		delete(s.jobTemplatesByDependencyIds, jobTemplate.Id)
	}
	return job.WithSucceeded(true).WithUpdatedRun(run.WithRunning(false).WithSucceeded(true)), true, nil
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
	nodeDb := s.accounting.nodeDbByPool[run.Pool()]
	node, err := nodeDb.GetNode(run.NodeId())
	if err != nil {
		return err
	} else if node == nil {
		return errors.Errorf("node %s not found", run.NodeId())
	}
	node, err = nodeDb.UnbindJobFromNode(job, node)
	if err != nil {
		return err
	}
	if err := nodeDb.Upsert(node); err != nil {
		return err
	}
	return nil
}

func (s *Simulator) handleJobRunPreempted(txn *jobdb.Txn, e *armadaevents.JobRunPreempted) (*jobdb.Job, bool, error) {
	jobId := e.PreemptedJobId
	job := txn.GetById(jobId)
	s.removeJobFromDemand(job)
	// Submit a retry for this job.
	jobTemplate := s.jobTemplateByJobId[job.Id()]
	retryJobId := util.NewULID()
	resubmitTime := s.time.Add(s.generateRandomShiftedExponentialDuration(s.ClusterSpec.WorkflowManagerDelayDistribution))
	gangInfo, err := schedulercontext.GangInfoFromLegacySchedulerJob(job)
	if err != nil {
		return nil, false, err
	}
	gangId := ""
	if gangInfo.Cardinality > 1 {
		toks := strings.Split(gangInfo.Id, "-")
		attempt, err := strconv.Atoi(toks[1])
		if err != nil {
			return nil, false, err
		}
		gangId = fmt.Sprintf("%s-%d", gangInfo.Id, attempt+1)
	}
	s.pushEventSequence(
		&armadaevents.EventSequence{
			Queue:      job.Queue(),
			JobSetName: job.Jobset(),
			Events: []*armadaevents.EventSequence_Event{
				{
					Created: protoutil.ToTimestamp(resubmitTime),
					Event: &armadaevents.EventSequence_Event_SubmitJob{
						SubmitJob: submitJobFromJobTemplate(retryJobId, jobTemplate, gangId),
					},
				},
			},
		},
	)
	s.jobTemplateByJobId[retryJobId] = jobTemplate
	updatedJob := job.WithUpdatedRun(job.LatestRun().WithReturned(true))
	if err := txn.Upsert([]*jobdb.Job{updatedJob}); err != nil {
		return nil, false, err
	}
	return updatedJob, true, nil
}

func maxTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return b
	}
	return a
}

func (s *Simulator) addJobToDemand(job *jobdb.Job) {
	r, ok := s.accounting.demandByQueue[job.Queue()]
	if !ok {
		r = schedulerobjects.NewResourceList(len(job.PodRequirements().ResourceRequirements.Requests))
		s.accounting.demandByQueue[job.Queue()] = r
	}
	r.AddV1ResourceList(job.PodRequirements().ResourceRequirements.Requests)
}

func (s *Simulator) removeJobFromDemand(job *jobdb.Job) {
	r, ok := s.accounting.demandByQueue[job.Queue()]
	if ok {
		r.SubV1ResourceList(job.PodRequirements().ResourceRequirements.Requests)
	}
}

func expandRepeatingTemplates(w *WorkloadSpec) *WorkloadSpec {
	workload := proto.Clone(w).(*WorkloadSpec)
	for _, q := range workload.GetQueues() {
		var templates []*JobTemplate
		for _, template := range q.GetJobTemplates() {
			if template.Repeat != nil {
				period := *template.Repeat.Period
				for i := 0; i < int(template.Repeat.NumTimes); i++ {
					t := proto.Clone(template).(*JobTemplate)
					t.Repeat = nil
					t.Id = fmt.Sprintf("%s-repeat-%d", t.Id, i)
					t.EarliestSubmitTime = t.EarliestSubmitTime + time.Duration(i)*period
					templates = append(templates, t)
				}
			} else {
				templates = append(templates, template)
			}
		}
		q.JobTemplates = templates
	}
	return workload
}
