package simulator

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mattn/go-zglob"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/renstrom/shortuuid"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/armadaproject/armada/internal/armada/configuration"
	commonconfig "github.com/armadaproject/armada/internal/common/config"
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
	testCase         *TestCase
	schedulingConfig configuration.SchedulingConfig
	// Map from jobId to the jobTemplate from which the job was created.
	jobTemplateByJobId map[string]*JobTemplate
	// Map from job template ids to slices of templates depending on those ids.
	jobTemplatesByDependencyIds map[string]map[string]*JobTemplate
	// Map from job template id to jobTemplate for templates for which all jobs have yet to succeed.
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
	// Current simulated time.
	time time.Time
	// Sequence number of the next event to be published.
	sequenceNumber int
	// Events stored in a priority queue ordered by submit time.
	eventLog EventLog
	// Simulated events are emitted on this channel in order.
	c chan *armadaevents.EventSequence
}

func NewSimulator(testCase *TestCase, schedulingConfig configuration.SchedulingConfig) (*Simulator, error) {
	initialiseTestCase(testCase)
	if err := validateTestCase(testCase); err != nil {
		return nil, err
	}

	// Setup nodes.
	nodeDbByPoolAndExecutorGroup := make(map[string][]*nodedb.NodeDb)
	totalResourcesByPool := make(map[string]schedulerobjects.ResourceList)
	poolByNodeId := make(map[string]string)
	// executorGroupByExecutor := make(map[string]string)
	nodeDbByExecutorName := make(map[string]*nodedb.NodeDb)
	for _, pool := range testCase.Pools {
		totalResourcesForPool := schedulerobjects.ResourceList{}
		for executorGroupIndex, executorGroup := range pool.ExecutorGroups {
			nodeDb, err := nodedb.NewNodeDb(
				schedulingConfig.Preemption.PriorityClasses,
				schedulingConfig.MaxExtraNodesToConsider,
				schedulingConfig.IndexedResources,
				schedulingConfig.IndexedTaints,
				schedulingConfig.IndexedNodeLabels,
			)
			if err != nil {
				return nil, err
			}
			for executorIndex, executor := range executorGroup.Executors {
				executorName := fmt.Sprintf("%s-%d-%d", pool.Name, executorGroupIndex, executorIndex)
				nodeDbByExecutorName[executorName] = nodeDb
				for nodeTemplateIndex, nodeTemplate := range executor.NodeTemplates {
					for i := 0; i < int(nodeTemplate.Number); i++ {
						nodeId := fmt.Sprintf("%s-%d-%d-%d-%d", pool.Name, executorGroupIndex, executorIndex, nodeTemplateIndex, i)
						allocatableByPriorityAndResource := make(map[int32]schedulerobjects.ResourceList)
						for _, priorityClass := range schedulingConfig.Preemption.PriorityClasses {
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
							return nil, err
						}
						txn.Commit()
						poolByNodeId[nodeId] = pool.Name
					}
				}
			}
			nodeDbByPoolAndExecutorGroup[pool.Name] = append(nodeDbByPoolAndExecutorGroup[pool.Name], nodeDb)
			totalResourcesForPool.Add(nodeDb.TotalResources())
		}
		totalResourcesByPool[pool.Name] = totalResourcesForPool
	}
	s := &Simulator{
		testCase:                                 testCase,
		schedulingConfig:                         schedulingConfig,
		jobTemplateByJobId:                       make(map[string]*JobTemplate),
		jobTemplatesByDependencyIds:              make(map[string]map[string]*JobTemplate),
		activeJobTemplatesById:                   make(map[string]*JobTemplate),
		jobDb:                                    jobdb.NewJobDb(),
		poolByNodeId:                             poolByNodeId,
		nodeDbByPoolAndExecutorGroup:             nodeDbByPoolAndExecutorGroup,
		nodeDbByExecutorName:                     nodeDbByExecutorName,
		allocationByPoolAndQueueAndPriorityClass: make(map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string]),
		totalResourcesByPool:                     totalResourcesByPool,
		c:                                        make(chan *armadaevents.EventSequence),
	}

	// Mark all jobTemplates as active.
	for _, queue := range testCase.Queues {
		for _, jobTemplate := range queue.JobTemplates {
			s.activeJobTemplatesById[jobTemplate.Id] = jobTemplate
		}
	}

	// Publish submitJob messages for all jobTemplates without dependencies.
	for _, queue := range testCase.Queues {
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
						Created: pointer(maxTime(s.time, jobTemplate.MinSubmitTime)),
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
	for _, queue := range testCase.Queues {
		for _, jobTemplate := range queue.JobTemplates {
			for _, dependencyJobTemplateId := range jobTemplate.Dependencies {
				dependencyJobTemplate, ok := s.activeJobTemplatesById[dependencyJobTemplateId]
				if !ok {
					return nil, errors.Errorf(
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

	// Publish scheduleEvent.
	s.pushScheduleEvent(s.time.Add(10 * time.Second))
	return s, nil
}

func (s *Simulator) C() <-chan *armadaevents.EventSequence {
	return s.c
}

func validateTestCase(testCase *TestCase) error {
	poolNames := util.Map(testCase.Pools, func(pool *Pool) string { return pool.Name })
	if !slices.Equal(poolNames, armadaslices.Unique(poolNames)) {
		return errors.Errorf("duplicate pool name: %v", poolNames)
	}

	executorNames := make([]string, 0)
	for _, pool := range testCase.Pools {
		for _, executorGroup := range pool.ExecutorGroups {
			for _, executor := range executorGroup.Executors {
				executorNames = append(executorNames, executor.Name)
			}
		}
	}
	if !slices.Equal(executorNames, armadaslices.Unique(executorNames)) {
		return errors.Errorf("duplicate executor name: %v", executorNames)
	}

	queueNames := util.Map(testCase.Queues, func(queue Queue) string { return queue.Name })
	if !slices.Equal(queueNames, armadaslices.Unique(queueNames)) {
		return errors.Errorf("duplicate queue name: %v", queueNames)
	}
	return nil
}

func initialiseTestCase(testCase *TestCase) {
	// Assign names to executors with none specified.
	for _, pool := range testCase.Pools {
		for i, executorGroup := range pool.ExecutorGroups {
			for j, executor := range executorGroup.Executors {
				if executor.Name == "" {
					executor.Name = fmt.Sprintf("%s-%d-%d", pool.Name, i, j)
				}
			}
		}
	}

	// Assign names to jobTemplates with none specified.
	for _, queue := range testCase.Queues {
		for i, jobTemplate := range queue.JobTemplates {
			if jobTemplate.Id == "" {
				jobTemplate.Id = fmt.Sprintf("%s-%d", queue.Name, i)
			}
			jobTemplate.Queue = queue.Name
		}
	}
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

type EventLog []Event

type Event struct {
	// Time at which the event was submitted.
	time time.Time
	// Each event is assigned a sequence number.
	// Events with equal time are ordered by their sequence number.
	sequenceNumber int
	// One of armadaevents.EventSequence or scheduleEvent..
	eventSequenceOrScheduleEvent any
	// Maintained by the heap.Interface methods.
	index int
}

func (el EventLog) Len() int { return len(el) }

func (el EventLog) Less(i, j int) bool {
	if el[i].time == el[j].time {
		return el[i].sequenceNumber < el[j].sequenceNumber
	}
	return el[j].time.After(el[i].time)
}

func (el EventLog) Swap(i, j int) {
	el[i], el[j] = el[j], el[i]
	el[i].index = i
	el[j].index = j
}

func (el *EventLog) Push(x any) {
	n := len(*el)
	item := x.(Event)
	item.index = n
	*el = append(*el, item)
}

func (el *EventLog) Pop() any {
	old := *el
	n := len(old)
	item := old[n-1]
	old[n-1] = Event{} // avoid memory leak
	item.index = -1    // for safety
	*el = old[0 : n-1]
	return item
}

// scheduleEvent is an event indicating the scheduler should be run.
type scheduleEvent struct{}

func (s *Simulator) Run() error {
	defer close(s.c)
	for s.eventLog.Len() > 0 {
		event := heap.Pop(&s.eventLog).(Event)
		if err := s.handleSimulatorEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func (s *Simulator) handleSimulatorEvent(event Event) error {
	s.time = event.time
	switch e := event.eventSequenceOrScheduleEvent.(type) {
	case *armadaevents.EventSequence:
		if err := s.handleEventSequence(e); err != nil {
			return err
		}
	case scheduleEvent:
		if err := s.handleScheduleEvent(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Simulator) handleScheduleEvent() error {
	var eventSequences []*armadaevents.EventSequence
	txn := s.jobDb.WriteTxn()
	defer txn.Abort()
	for _, pool := range s.testCase.Pools {
		for i := range pool.ExecutorGroups {
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
				totalResources,
			)
			for _, queue := range s.testCase.Queues {
				err := sctx.AddQueueSchedulingContext(
					queue.Name,
					queue.Weight,
					s.allocationByPoolAndQueueAndPriorityClass[pool.Name][queue.Name],
				)
				if err != nil {
					return err
				}
			}
			constraints := schedulerconstraints.SchedulingConstraintsFromSchedulingConfig(
				pool.Name,
				totalResources,
				// Minimum job size not not used for simulation; use taints/tolerations instead.
				schedulerobjects.ResourceList{},
				s.schedulingConfig,
			)
			sch := scheduler.NewPreemptingQueueScheduler(
				sctx,
				constraints,
				s.schedulingConfig.Preemption.NodeEvictionProbability,
				s.schedulingConfig.Preemption.NodeOversubscriptionEvictionProbability,
				s.schedulingConfig.Preemption.ProtectedFractionOfFairShare,
				scheduler.NewSchedulerJobRepositoryAdapter(s.jobDb, txn),
				nodeDb,
				// TODO: Necessary to support partial eviction.
				nil,
				nil,
				nil,
			)
			if s.schedulingConfig.EnableNewPreemptionStrategy {
				sch.EnableNewPreemptionStrategy()
			}

			result, err := sch.Schedule(context.Background(), logrus.WithField("pool", "pool"))
			if err != nil {
				return err
			}

			// Update jobDb to reflect the decisions by the scheduler.
			// Sort jobs to ensure deterministic event ordering.
			preemptedJobs := scheduler.PreemptedJobsFromSchedulerResult[*jobdb.Job](result)
			scheduledJobs := scheduler.ScheduledJobsFromSchedulerResult[*jobdb.Job](result)
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
			if err := s.jobDb.Upsert(txn, preemptedJobs); err != nil {
				return err
			}
			if err := s.jobDb.Upsert(txn, scheduledJobs); err != nil {
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
		}
	}
	txn.Commit()

	// Publish simulator events.
	for _, eventSequence := range eventSequences {
		s.pushEventSequence(eventSequence)
	}

	// Schedule the next run of the scheduler, unless there are no more active jobTemplates.
	// TODO: Make timeout configurable.
	if len(s.activeJobTemplatesById) > 0 {
		s.pushScheduleEvent(s.time.Add(10 * time.Second))
	}
	return nil
}

// TODO: Write events to disk unless they should be discarded.
func (s *Simulator) handleEventSequence(es *armadaevents.EventSequence) error {
	txn := s.jobDb.WriteTxn()
	defer txn.Abort()
	eventsToPublish := make([]*armadaevents.EventSequence_Event, 0, len(es.Events))
	for _, event := range es.Events {
		var ok bool
		var err error = nil
		switch eventType := event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_SubmitJob:
			ok, err = s.handleSubmitJob(txn, event.GetSubmitJob(), *event.Created, es)
		case *armadaevents.EventSequence_Event_JobRunLeased:
			ok, err = s.handleJobRunLeased(txn, event.GetJobRunLeased())
		case *armadaevents.EventSequence_Event_JobSucceeded:
			ok, err = s.handleJobSucceeded(txn, event.GetJobSucceeded())
		case *armadaevents.EventSequence_Event_JobRunPreempted:
			ok, err = s.handleJobRunPreempted(txn, event.GetJobRunPreempted())
		case *armadaevents.EventSequence_Event_ReprioritisedJob,
			*armadaevents.EventSequence_Event_JobDuplicateDetected,
			*armadaevents.EventSequence_Event_ResourceUtilisation,
			*armadaevents.EventSequence_Event_StandaloneIngressInfo,
			*armadaevents.EventSequence_Event_JobRunAssigned,
			*armadaevents.EventSequence_Event_JobRunRunning,
			*armadaevents.EventSequence_Event_JobRunSucceeded,
			*armadaevents.EventSequence_Event_JobRunErrors,
			*armadaevents.EventSequence_Event_ReprioritiseJob,
			*armadaevents.EventSequence_Event_ReprioritiseJobSet,
			*armadaevents.EventSequence_Event_CancelledJob,
			*armadaevents.EventSequence_Event_JobRequeued,
			*armadaevents.EventSequence_Event_PartitionMarker,
			*armadaevents.EventSequence_Event_JobErrors,
			*armadaevents.EventSequence_Event_CancelJob,
			*armadaevents.EventSequence_Event_CancelJobSet:
			// These events can be safely ignored.
			logrus.Debugf("Ignoring event type %T", event)
		default:
			// This is an event type we haven't consider; log a warning.
			logrus.Warnf("Ignoring unknown event type %T", eventType)
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
		s.c <- es
	}
	return nil
}

func (s *Simulator) handleSubmitJob(txn *jobdb.Txn, e *armadaevents.SubmitJob, time time.Time, eventSequence *armadaevents.EventSequence) (bool, error) {
	schedulingInfo, err := scheduleringester.SchedulingInfoFromSubmitJob(e, time, s.schedulingConfig.Preemption.PriorityClasses)
	if err != nil {
		return false, err
	}
	job := jobdb.NewJob(
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
		time.UnixNano(),
	)
	if err := s.jobDb.Upsert(txn, []*jobdb.Job{job}); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Simulator) handleJobRunLeased(txn *jobdb.Txn, e *armadaevents.JobRunLeased) (bool, error) {
	jobId := armadaevents.UlidFromProtoUuid(e.JobId).String()
	job := s.jobDb.GetById(txn, jobId)
	// TODO: Randomise runtime.
	jobTemplate := s.jobTemplateByJobId[jobId]
	if jobTemplate == nil {
		return false, errors.Errorf("no jobTemplate associated with job %s", jobId)
	}
	jobSuccessTime := s.time.Add(time.Duration(jobTemplate.RuntimeMean) * time.Second)
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

func (s *Simulator) handleJobSucceeded(txn *jobdb.Txn, e *armadaevents.JobSucceeded) (bool, error) {
	jobId := armadaevents.UlidFromProtoUuid(e.JobId).String()
	job := s.jobDb.GetById(txn, jobId)
	if job == nil || job.InTerminalState() {
		// Job already terminated; nothing more to do.
		return false, nil
	}
	if err := s.jobDb.BatchDelete(txn, []string{jobId}); err != nil {
		return false, err
	}

	// Subtract the allocation of this job from the queue allocation.
	run := job.LatestRun()
	pool := s.poolByNodeId[run.NodeId()]
	s.allocationByPoolAndQueueAndPriorityClass[pool][job.Queue()].SubV1ResourceList(job.GetPriorityClassName(), job.GetResourceRequirements().Requests)

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
						Created: pointer(maxTime(s.time, dependentJobTemplate.MinSubmitTime)),
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
	job := s.jobDb.GetById(txn, jobId)

	// Submit a retry for this job.
	jobTemplate := s.jobTemplateByJobId[job.GetId()]
	retryJobId := util.ULID()
	s.pushEventSequence(
		&armadaevents.EventSequence{
			Queue:      job.Queue(),
			JobSetName: job.Jobset(),
			Events: []*armadaevents.EventSequence_Event{
				{
					Created: &s.time,
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

// func (a *App) TestPattern(ctx context.Context, pattern string) (*TestSuiteReport, error) {
// 	testSpecs, err := TestSpecsFromPattern(pattern)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return a.RunTests(ctx, testSpecs)
// }

func SchedulingConfigsFromPattern(pattern string) ([]configuration.SchedulingConfig, error) {
	filePaths, err := zglob.Glob(pattern)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return SchedulingConfigsFromFilePaths(filePaths)
}

func SchedulingConfigsFromFilePaths(filePaths []string) ([]configuration.SchedulingConfig, error) {
	rv := make([]configuration.SchedulingConfig, len(filePaths))
	for i, filePath := range filePaths {
		config, err := SchedulingConfigFromFilePath(filePath)
		if err != nil {
			return nil, err
		}
		rv[i] = config
	}
	return rv, nil
}

func SchedulingConfigFromFilePath(filePath string) (configuration.SchedulingConfig, error) {
	config := configuration.SchedulingConfig{}
	v := viper.New()
	v.SetConfigFile(filePath)
	if err := v.ReadInConfig(); err != nil {
		return config, errors.WithStack(err)
	}
	if err := v.Unmarshal(&config, commonconfig.CustomHooks...); err != nil {
		return config, errors.WithStack(err)
	}
	return config, nil
}

func TestCasesFromPattern(pattern string) ([]*TestCase, error) {
	filePaths, err := zglob.Glob(pattern)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return TestCasesFromFilePaths(filePaths)
}

func TestCasesFromFilePaths(filePaths []string) ([]*TestCase, error) {
	rv := make([]*TestCase, len(filePaths))
	for i, filePath := range filePaths {
		testCase, err := TestCaseFromFilePath(filePath)
		if err != nil {
			return nil, err
		}
		rv[i] = testCase
	}
	return rv, nil
}

func TestCaseFromFilePath(filePath string) (*TestCase, error) {
	yamlBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(yamlBytes) == 0 {
		return nil, errors.Errorf("%s does not exist or is empty", filePath)
	}
	testCase, err := TestCaseFromBytes(yamlBytes)
	if err != nil {
		return nil, err
	}

	// If no test name is provided, set it to be the filename.
	if testCase.Name == "" {
		fileName := filepath.Base(filePath)
		fileName = strings.TrimSuffix(fileName, filepath.Ext(fileName))
		testCase.Name = fileName
	}

	// Generate random ids for any job templates without an explicitly set id.
	for i, queue := range testCase.Queues {
		for j, jobTemplate := range queue.JobTemplates {
			if jobTemplate.Id == "" {
				jobTemplate.Id = shortuuid.New()
			}
			queue.JobTemplates[j] = jobTemplate
		}
		testCase.Queues[i] = queue
	}

	return testCase, nil
}

// TestCaseFromBytes unmarshalls bytes into a TestCase.
func TestCaseFromBytes(yamlBytes []byte) (*TestCase, error) {
	var testCase TestCase
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(yamlBytes), 128).Decode(&testCase); err != nil {
		return nil, errors.WithStack(err)
	}
	return &testCase, nil
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
