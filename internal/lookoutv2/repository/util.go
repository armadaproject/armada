package repository

import (
	"fmt"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/types"
	"github.com/google/uuid"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/clock"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	log "github.com/armadaproject/armada/internal/common/logging"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/instructions"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type JobSimulator struct {
	queue            string
	jobSet           string
	owner            string
	namespace        string
	annotationPrefix string
	jobId            string
	apiJob           *api.Job
	job              *model.Job
	events           []*armadaevents.EventSequence_Event
	converter        *instructions.InstructionConverter
	store            *lookoutdb.LookoutDb
	clock            clock.Clock
}

type JobOptions struct {
	JobId            string
	Priority         int
	PriorityClass    string
	Cpu              resource.Quantity
	Memory           resource.Quantity
	EphemeralStorage resource.Quantity
	Gpu              resource.Quantity
	Annotations      map[string]string
}

type runPatch struct {
	runId       string
	cluster     *string
	exitCode    *int32
	finished    *time.Time
	jobRunState lookout.JobRunState
	node        *string
	leased      *time.Time
	pending     *time.Time
	started     *time.Time
}

func NewJobSimulator(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb) *JobSimulator {
	return &JobSimulator{
		converter: converter,
		store:     store,
		clock:     clock.RealClock{},
	}
}

func NewJobSimulatorWithClock(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb, clk clock.Clock) *JobSimulator {
	return &JobSimulator{
		converter: converter,
		store:     store,
		clock:     clk,
	}
}

func (js *JobSimulator) ForJobID(jobID string) *JobSimulator {
	js.jobId = jobID
	js.job = &model.Job{
		JobId: jobID,
	}
	return js
}

func (js *JobSimulator) Submit(queue, jobSet, owner, namespace string, timestamp time.Time, opts *JobOptions) *JobSimulator {
	js.queue = queue
	js.jobSet = jobSet
	js.owner = owner
	js.namespace = namespace
	jobId := opts.JobId
	if jobId == "" {
		jobId = util.NewULID()
	}
	js.jobId = jobId
	priorityClass := opts.PriorityClass
	if priorityClass == "" {
		priorityClass = "default"
	}
	if (&opts.Cpu).IsZero() {
		opts.Cpu = resource.MustParse("150m")
	}
	if (&opts.Memory).IsZero() {
		opts.Memory = resource.MustParse("64Mi")
	}
	resources := make(map[v1.ResourceName]resource.Quantity)
	resources[v1.ResourceCPU] = opts.Cpu
	resources[v1.ResourceMemory] = opts.Memory
	resources[v1.ResourceEphemeralStorage] = opts.EphemeralStorage
	resources["nvidia.com/gpu"] = opts.Gpu
	ts := timestampOrNow(timestamp)

	if opts.Annotations == nil {
		opts.Annotations = make(map[string]string)
	}

	submitEvent := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_SubmitJob{
			SubmitJob: &armadaevents.SubmitJob{
				JobId:           jobId,
				Priority:        uint32(opts.Priority),
				AtMostOnce:      true,
				Preemptible:     true,
				ConcurrencySafe: true,
				ObjectMeta: &armadaevents.ObjectMeta{
					Namespace:   queue,
					Name:        "test-job",
					Annotations: prefixAnnotations(js.annotationPrefix, opts.Annotations),
				},
				MainObject: &armadaevents.KubernetesMainObject{
					Object: &armadaevents.KubernetesMainObject_PodSpec{
						PodSpec: &armadaevents.PodSpecWithAvoidList{
							PodSpec: &v1.PodSpec{
								NodeSelector:      nil,
								Tolerations:       nil,
								PriorityClassName: priorityClass,
								Containers: []v1.Container{
									{
										Name:    "container1",
										Image:   "alpine:latest",
										Command: []string{"myprogram.sh"},
										Args:    []string{"foo", "bar"},
										Resources: v1.ResourceRequirements{
											Limits:   resources,
											Requests: resources,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	js.events = append(js.events, submitEvent)

	apiJob, _ := eventutil.ApiJobFromLogSubmitJob(owner, []string{}, queue, jobSet, protoutil.ToStdTime(ts), submitEvent.GetSubmitJob())
	js.apiJob = apiJob

	js.job = &model.Job{
		Annotations:        opts.Annotations,
		Cpu:                opts.Cpu.MilliValue(),
		EphemeralStorage:   opts.EphemeralStorage.Value(),
		Gpu:                opts.Gpu.Value(),
		JobId:              jobId,
		JobSet:             jobSet,
		LastTransitionTime: protoutil.ToStdTime(ts),
		Memory:             opts.Memory.Value(),
		Owner:              owner,
		Namespace:          &apiJob.Namespace,
		Priority:           int64(opts.Priority),
		PriorityClass:      &priorityClass,
		Queue:              queue,
		Runs:               []*model.Run{},
		State:              string(lookout.JobQueued),
		Submitted:          protoutil.ToStdTime(ts),
	}
	return js
}

func (js *JobSimulator) Lease(runId string, cluster string, node string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	leased := protoutil.ToStdTime(ts)
	leasedEvent := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_JobRunLeased{
			JobRunLeased: &armadaevents.JobRunLeased{
				RunId:      runId,
				JobId:      js.jobId,
				ExecutorId: cluster,
				NodeId:     node,
			},
		},
	}
	js.events = append(js.events, leasedEvent)

	js.job.LastActiveRunId = &runId
	js.job.LastTransitionTime = protoutil.ToStdTime(ts)
	js.job.State = string(lookout.JobLeased)
	js.job.Cluster = cluster
	js.job.Node = &node
	js.updateRun(js.job, &runPatch{
		runId:       runId,
		jobRunState: lookout.JobRunLeased,
		cluster:     &cluster,
		node:        &node,
		leased:      &leased,
	})
	return js
}

func (js *JobSimulator) Pending(runId string, cluster string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	pending := protoutil.ToStdTime(ts)
	assignedEvent := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_JobRunAssigned{
			JobRunAssigned: &armadaevents.JobRunAssigned{
				RunId: runId,
				JobId: js.jobId,
				ResourceInfos: []*armadaevents.KubernetesResourceInfo{
					{
						ObjectMeta: &armadaevents.ObjectMeta{
							KubernetesId: runId,
							Name:         "test-job",
							Namespace:    js.queue,
							ExecutorId:   cluster,
						},
						Info: &armadaevents.KubernetesResourceInfo_PodInfo{
							PodInfo: &armadaevents.PodInfo{
								PodNumber: 0,
							},
						},
					},
				},
			},
		},
	}
	js.events = append(js.events, assignedEvent)

	js.job.LastActiveRunId = &runId
	js.job.LastTransitionTime = pending
	js.job.State = string(lookout.JobPending)
	js.job.Cluster = cluster
	rp := &runPatch{
		runId:       runId,
		cluster:     &cluster,
		jobRunState: lookout.JobRunPending,
		pending:     &pending,
	}
	js.updateRun(js.job, rp)
	return js
}

func (js *JobSimulator) Running(runId string, node string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	running := protoutil.ToStdTime(ts)
	runningEvent := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_JobRunRunning{
			JobRunRunning: &armadaevents.JobRunRunning{
				RunId: runId,
				JobId: js.jobId,
				ResourceInfos: []*armadaevents.KubernetesResourceInfo{
					{
						Info: &armadaevents.KubernetesResourceInfo_PodInfo{
							PodInfo: &armadaevents.PodInfo{
								NodeName:  node,
								PodNumber: 0,
							},
						},
					},
				},
			},
		},
	}
	js.events = append(js.events, runningEvent)

	js.job.LastActiveRunId = &runId
	js.job.LastTransitionTime = running
	js.job.State = string(lookout.JobRunning)
	js.job.Node = &node
	js.updateRun(js.job, &runPatch{
		runId:       runId,
		jobRunState: lookout.JobRunRunning,
		node:        &node,
		started:     &running,
	})
	return js
}

func (js *JobSimulator) RunSucceeded(runId string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	succeeded := protoutil.ToStdTime(ts)
	runSucceeded := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
			JobRunSucceeded: &armadaevents.JobRunSucceeded{
				RunId: runId,
				JobId: js.jobId,
			},
		},
	}
	js.events = append(js.events, runSucceeded)

	js.job.LastActiveRunId = &runId
	js.updateRun(js.job, &runPatch{
		runId:       runId,
		exitCode:    pointer.Int32(0),
		finished:    &succeeded,
		jobRunState: lookout.JobRunSucceeded,
	})
	return js
}

func (js *JobSimulator) Succeeded(timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	succeededTime := protoutil.ToStdTime(ts)
	succeeded := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_JobSucceeded{
			JobSucceeded: &armadaevents.JobSucceeded{
				JobId: js.jobId,
			},
		},
	}
	js.events = append(js.events, succeeded)

	js.job.LastTransitionTime = succeededTime
	js.job.State = string(lookout.JobSucceeded)
	return js
}

func (js *JobSimulator) LeaseReturned(runId string, message string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	returnedTime := protoutil.ToStdTime(ts)
	leaseReturned := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: js.jobId,
				RunId: runId,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodLeaseReturned{
							PodLeaseReturned: &armadaevents.PodLeaseReturned{
								Message: message,
							},
						},
					},
				},
			},
		},
	}
	js.events = append(js.events, leaseReturned)

	js.updateRun(js.job, &runPatch{
		runId:       runId,
		finished:    &returnedTime,
		jobRunState: lookout.JobRunLeaseReturned,
	})
	return js
}

func (js *JobSimulator) Cancelled(timestamp time.Time, cancelUser string) *JobSimulator {
	ts := timestampOrNow(timestamp)
	cancelledTime := protoutil.ToStdTime(ts)
	cancelled := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_CancelledJob{
			CancelledJob: &armadaevents.CancelledJob{
				JobId:      js.jobId,
				CancelUser: cancelUser,
			},
		},
	}
	js.events = append(js.events, cancelled)

	js.job.State = string(lookout.JobCancelled)
	js.job.Cancelled = &cancelledTime
	js.job.CancelUser = &cancelUser
	js.job.LastTransitionTime = cancelledTime
	return js
}

func (js *JobSimulator) Reprioritized(newPriority uint32, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	reprioritized := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_ReprioritisedJob{
			ReprioritisedJob: &armadaevents.ReprioritisedJob{
				JobId:    js.jobId,
				Priority: newPriority,
			},
		},
	}
	js.events = append(js.events, reprioritized)
	js.apiJob.Priority = float64(newPriority)

	js.apiJob.Priority = float64(newPriority)
	js.job.Priority = int64(newPriority)
	return js
}

func (js *JobSimulator) RunFailed(runId string, node string, exitCode int32, message string, debug string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	failedTime := protoutil.ToStdTime(ts)
	runFailed := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: js.jobId,
				RunId: runId,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodError{
							PodError: &armadaevents.PodError{
								Message:      message,
								DebugMessage: debug,
								NodeName:     node,
								ContainerErrors: []*armadaevents.ContainerError{
									{ExitCode: exitCode},
								},
							},
						},
					},
				},
			},
		},
	}
	js.events = append(js.events, runFailed)

	js.job.LastActiveRunId = &runId
	js.updateRun(js.job, &runPatch{
		runId:       runId,
		exitCode:    &exitCode,
		finished:    &failedTime,
		jobRunState: lookout.JobRunFailed,
		node:        &node,
	})
	return js
}

func (js *JobSimulator) Rejected(message string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	rejectedTime := protoutil.ToStdTime(ts)
	rejected := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_JobErrors{
			JobErrors: &armadaevents.JobErrors{
				JobId: js.jobId,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_JobRejected{
							JobRejected: &armadaevents.JobRejected{
								Message: message,
							},
						},
					},
				},
			},
		},
	}
	js.events = append(js.events, rejected)

	js.job.LastTransitionTime = rejectedTime
	js.job.State = string(lookout.JobRejected)
	return js
}

func (js *JobSimulator) Failed(node string, exitCode int32, message string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	failedTime := protoutil.ToStdTime(ts)
	failed := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_JobErrors{
			JobErrors: &armadaevents.JobErrors{
				JobId: js.jobId,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodError{
							PodError: &armadaevents.PodError{
								Message:  message,
								NodeName: node,
								ContainerErrors: []*armadaevents.ContainerError{
									{ExitCode: exitCode},
								},
							},
						},
					},
				},
			},
		},
	}
	js.events = append(js.events, failed)

	js.job.LastTransitionTime = failedTime
	js.job.State = string(lookout.JobFailed)
	return js
}

func (js *JobSimulator) Preempted(timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	preemptedTime := protoutil.ToStdTime(ts)

	preemptedJob := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_JobErrors{
			JobErrors: &armadaevents.JobErrors{
				JobId: js.jobId,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_JobRunPreemptedError{
							JobRunPreemptedError: &armadaevents.JobRunPreemptedError{},
						},
					},
				},
			},
		},
	}

	preemptedRunId := uuid.NewString()
	preemptedRun := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_JobRunPreempted{
			JobRunPreempted: &armadaevents.JobRunPreempted{
				PreemptedJobId: js.jobId,
				PreemptedRunId: preemptedRunId,
			},
		},
	}
	js.events = append(js.events, preemptedJob, preemptedRun)

	js.job.LastTransitionTime = preemptedTime
	js.job.State = string(lookout.JobPreempted)
	return js
}

func (js *JobSimulator) RunUnschedulable(runId string, cluster string, node string, message string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	unschedulableTime := protoutil.ToStdTime(ts)
	runUnschedulable := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: js.jobId,
				RunId: runId,
				Errors: []*armadaevents.Error{
					{
						Terminal: false,
						Reason: &armadaevents.Error_PodUnschedulable{
							PodUnschedulable: &armadaevents.PodUnschedulable{
								NodeName: node,
								ObjectMeta: &armadaevents.ObjectMeta{
									ExecutorId: cluster,
								},
								Message: message,
							},
						},
					},
				},
			},
		},
	}
	js.events = append(js.events, runUnschedulable)

	js.updateRun(js.job, &runPatch{
		runId:       runId,
		cluster:     &cluster,
		finished:    &unschedulableTime,
		jobRunState: lookout.JobRunUnableToSchedule,
		node:        &node,
	})
	return js
}

func (js *JobSimulator) LeaseExpired(runId string, timestamp time.Time, _ clock.Clock) *JobSimulator {
	ts := timestampOrNow(timestamp)
	leaseExpiredTime := protoutil.ToStdTime(ts)
	leaseReturned := &armadaevents.EventSequence_Event{
		Created: ts,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: js.jobId,
				RunId: runId,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_LeaseExpired{
							LeaseExpired: &armadaevents.LeaseExpired{},
						},
					},
				},
			},
		},
	}
	js.events = append(js.events, leaseReturned)

	js.updateRun(js.job, &runPatch{
		runId:       runId,
		finished:    &leaseExpiredTime,
		jobRunState: lookout.JobRunLeaseExpired,
	})
	return js
}

func (js *JobSimulator) Build() *JobSimulator {
	// Cancelled job events must be part of a different event sequence, as they can be initiated by a user which is not the owner of the job
	eventSequences := []*armadaevents.EventSequence{}
	eventsInCurrentSequence := make([]*armadaevents.EventSequence_Event, 0, len(js.events))
	for i, event := range js.events {
		switch event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_CancelledJob:
			eventSequences = append(
				eventSequences,
				&armadaevents.EventSequence{
					Queue:      js.queue,
					JobSetName: js.jobSet,
					UserId:     js.owner,
					Events:     eventsInCurrentSequence,
				},
				&armadaevents.EventSequence{
					Queue:      js.queue,
					JobSetName: js.jobSet,
					UserId:     *js.job.CancelUser,
					Events:     []*armadaevents.EventSequence_Event{event},
				},
			)
			eventsInCurrentSequence = make([]*armadaevents.EventSequence_Event, 0, len(js.events)-i)
		default:
			eventsInCurrentSequence = append(eventsInCurrentSequence, event)
		}
	}
	if len(eventsInCurrentSequence) > 0 {
		eventSequences = append(
			eventSequences,
			&armadaevents.EventSequence{
				Queue:      js.queue,
				JobSetName: js.jobSet,
				UserId:     js.owner,
				Events:     eventsInCurrentSequence,
			},
		)
	}

	messageIds := make([]pulsar.MessageID, len(eventSequences))
	for i := 0; i < len(eventSequences); i++ {
		messageIds[i] = pulsarutils.NewMessageId(i + 1)
	}
	eventSequenceWithIds := &utils.EventsWithIds[*armadaevents.EventSequence]{
		Events:     eventSequences,
		MessageIds: messageIds,
	}

	instructionSet := js.converter.Convert(armadacontext.TODO(), eventSequenceWithIds)
	err := js.store.Store(armadacontext.TODO(), instructionSet)
	if err != nil {
		log.WithError(err).Error("Simulator failed to store job in database")
	}
	return js
}

func (js *JobSimulator) Job() *model.Job {
	return js.job
}

func (js *JobSimulator) ApiJob() *api.Job {
	return js.apiJob
}

func timestampOrNow(timestamp time.Time) *types.Timestamp {
	if timestamp.IsZero() {
		return types.TimestampNow()
	}
	return protoutil.ToTimestamp(timestamp)
}

func (js *JobSimulator) updateRun(job *model.Job, patch *runPatch) {
	if patch.exitCode != nil {
		job.ExitCode = patch.exitCode
	}
	for _, run := range job.Runs {
		if run.RunId == patch.runId {
			patchRun(run, patch)
			job.RuntimeSeconds = calculateJobRuntime(run.Started, run.Finished, js.clock)
			return
		}
	}
	cluster := ""
	if patch.cluster != nil {
		cluster = *patch.cluster
	}
	job.Runs = append(job.Runs, &model.Run{
		Cluster:     cluster,
		ExitCode:    patch.exitCode,
		Finished:    model.NewPostgreSQLTime(patch.finished),
		JobRunState: lookout.JobRunStateOrdinalMap[patch.jobRunState],
		Node:        patch.node,
		Leased:      model.NewPostgreSQLTime(patch.leased),
		Pending:     model.NewPostgreSQLTime(patch.pending),
		RunId:       patch.runId,
		Started:     model.NewPostgreSQLTime(patch.started),
	})
	job.RuntimeSeconds = calculateJobRuntime(model.NewPostgreSQLTime(patch.started), model.NewPostgreSQLTime(patch.finished), js.clock)
}

func patchRun(run *model.Run, patch *runPatch) {
	if patch.cluster != nil {
		run.Cluster = *patch.cluster
	}
	if patch.exitCode != nil {
		run.ExitCode = patch.exitCode
	}
	if patch.finished != nil {
		run.Finished = model.NewPostgreSQLTime(patch.finished)
	}
	run.JobRunState = lookout.JobRunStateOrdinalMap[patch.jobRunState]
	if patch.node != nil {
		run.Node = patch.node
	}
	if patch.leased != nil {
		run.Leased = model.NewPostgreSQLTime(patch.leased)
	}
	if patch.pending != nil {
		run.Pending = model.NewPostgreSQLTime(patch.pending)
	}
	if patch.started != nil {
		run.Started = model.NewPostgreSQLTime(patch.started)
	}
}

func prefixAnnotations(prefix string, annotations map[string]string) map[string]string {
	prefixed := make(map[string]string)
	for key, value := range annotations {
		prefixed[fmt.Sprintf("%s%s", prefix, key)] = value
	}
	return prefixed
}

func logQueryDebug(query *Query, description string) {
	log.
		WithField("query", removeNewlinesAndTabs(query.Sql)).
		WithField("values", query.Args).
		Debug(description)
}

func logQueryError(query *Query, description string, duration time.Duration) {
	log.
		WithField("query", removeNewlinesAndTabs(query.Sql)).
		WithField("values", query.Args).
		WithField("duration", duration).
		Errorf("Error executing %s query", description)
}

func logSlowQuery(query *Query, description string, duration time.Duration) {
	if duration > 5*time.Second {
		log.
			WithField("query", removeNewlinesAndTabs(query.Sql)).
			WithField("values", query.Args).
			WithField("duration", duration).
			Infof("Slow %s query detected", description)
	}
}

func removeNewlinesAndTabs(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\t", "")
}
