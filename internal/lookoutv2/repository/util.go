package repository

import (
	"fmt"
	"strings"
	"time"

	"github.com/armadaproject/armada/internal/common/context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/ingest"
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
	annotationPrefix string
	jobId            *armadaevents.Uuid
	apiJob           *api.Job
	job              *model.Job
	events           []*armadaevents.EventSequence_Event
	converter        *instructions.InstructionConverter
	store            *lookoutdb.LookoutDb
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
	jobRunState *string
	node        *string
	leased      *time.Time
	pending     *time.Time
	started     *time.Time
}

func NewJobSimulator(converter *instructions.InstructionConverter, store *lookoutdb.LookoutDb) *JobSimulator {
	return &JobSimulator{
		converter: converter,
		store:     store,
	}
}

func (js *JobSimulator) Submit(queue, jobSet, owner string, timestamp time.Time, opts *JobOptions) *JobSimulator {
	js.queue = queue
	js.jobSet = jobSet
	js.owner = owner
	jobId := opts.JobId
	if jobId == "" {
		jobId = util.NewULID()
	}
	jobIdProto, err := armadaevents.ProtoUuidFromUlidString(jobId)
	if err != nil {
		log.WithError(err).Errorf("Could not convert job ID to UUID: %s", jobId)
	}
	js.jobId = jobIdProto
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
		Created: &ts,
		Event: &armadaevents.EventSequence_Event_SubmitJob{
			SubmitJob: &armadaevents.SubmitJob{
				JobId:           jobIdProto,
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

	apiJob, _ := eventutil.ApiJobFromLogSubmitJob(owner, []string{}, queue, jobSet, ts, submitEvent.GetSubmitJob())
	js.apiJob = apiJob

	js.job = &model.Job{
		Annotations:        opts.Annotations,
		Cpu:                opts.Cpu.MilliValue(),
		EphemeralStorage:   opts.EphemeralStorage.Value(),
		Gpu:                opts.Gpu.Value(),
		JobId:              jobId,
		JobSet:             jobSet,
		LastTransitionTime: ts,
		Memory:             opts.Memory.Value(),
		Owner:              owner,
		Priority:           int64(opts.Priority),
		PriorityClass:      &priorityClass,
		Queue:              queue,
		Runs:               []*model.Run{},
		State:              string(lookout.JobQueued),
		Submitted:          ts,
	}
	return js
}

func (js *JobSimulator) Lease(runId string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	leasedEvent := &armadaevents.EventSequence_Event{
		Created: &ts,
		Event: &armadaevents.EventSequence_Event_JobRunLeased{
			JobRunLeased: &armadaevents.JobRunLeased{
				RunId: armadaevents.ProtoUuidFromUuid(uuid.MustParse(runId)),
				JobId: js.jobId,
			},
		},
	}
	js.events = append(js.events, leasedEvent)

	js.job.LastActiveRunId = &runId
	js.job.LastTransitionTime = ts
	js.job.State = string(lookout.JobLeased)
	updateRun(js.job, &runPatch{
		runId:       runId,
		jobRunState: pointer.String(string(lookout.JobRunLeased)),
		leased:      &ts,
	})
	return js
}

func (js *JobSimulator) Pending(runId string, cluster string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	assignedEvent := &armadaevents.EventSequence_Event{
		Created: &ts,
		Event: &armadaevents.EventSequence_Event_JobRunAssigned{
			JobRunAssigned: &armadaevents.JobRunAssigned{
				RunId: armadaevents.ProtoUuidFromUuid(uuid.MustParse(runId)),
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
	js.job.LastTransitionTime = ts
	js.job.State = string(lookout.JobPending)
	rp := &runPatch{
		runId:       runId,
		cluster:     &cluster,
		jobRunState: pointer.String(string(lookout.JobRunPending)),
		pending:     &ts,
	}
	if js.converter.IsLegacy() {
		rp.leased = &ts
	}
	updateRun(js.job, rp)
	return js
}

func (js *JobSimulator) Running(runId string, node string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	runningEvent := &armadaevents.EventSequence_Event{
		Created: &ts,
		Event: &armadaevents.EventSequence_Event_JobRunRunning{
			JobRunRunning: &armadaevents.JobRunRunning{
				RunId: armadaevents.ProtoUuidFromUuid(uuid.MustParse(runId)),
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
	js.job.LastTransitionTime = ts
	js.job.State = string(lookout.JobRunning)
	updateRun(js.job, &runPatch{
		runId:       runId,
		jobRunState: pointer.String(string(lookout.JobRunRunning)),
		node:        &node,
		started:     &ts,
	})
	return js
}

func (js *JobSimulator) RunSucceeded(runId string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	runSucceeded := &armadaevents.EventSequence_Event{
		Created: &ts,
		Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
			JobRunSucceeded: &armadaevents.JobRunSucceeded{
				RunId: armadaevents.ProtoUuidFromUuid(uuid.MustParse(runId)),
				JobId: js.jobId,
			},
		},
	}
	js.events = append(js.events, runSucceeded)

	js.job.LastActiveRunId = &runId
	updateRun(js.job, &runPatch{
		runId:       runId,
		exitCode:    pointer.Int32(0),
		finished:    &ts,
		jobRunState: pointer.String(string(lookout.JobRunSucceeded)),
	})
	return js
}

func (js *JobSimulator) Succeeded(timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	succeeded := &armadaevents.EventSequence_Event{
		Created: &ts,
		Event: &armadaevents.EventSequence_Event_JobSucceeded{
			JobSucceeded: &armadaevents.JobSucceeded{
				JobId: js.jobId,
			},
		},
	}
	js.events = append(js.events, succeeded)

	js.job.LastTransitionTime = ts
	js.job.State = string(lookout.JobSucceeded)
	return js
}

func (js *JobSimulator) LeaseReturned(runId string, message string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	leaseReturned := &armadaevents.EventSequence_Event{
		Created: &ts,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: js.jobId,
				RunId: armadaevents.ProtoUuidFromUuid(uuid.MustParse(runId)),
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

	updateRun(js.job, &runPatch{
		runId:       runId,
		finished:    &ts,
		jobRunState: pointer.String(string(lookout.JobRunLeaseReturned)),
	})
	return js
}

func (js *JobSimulator) Cancelled(timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	cancelled := &armadaevents.EventSequence_Event{
		Created: &ts,
		Event: &armadaevents.EventSequence_Event_CancelledJob{
			CancelledJob: &armadaevents.CancelledJob{
				JobId: js.jobId,
			},
		},
	}
	js.events = append(js.events, cancelled)

	js.job.State = string(lookout.JobCancelled)
	js.job.Cancelled = &ts
	js.job.LastTransitionTime = ts
	return js
}

func (js *JobSimulator) Reprioritized(newPriority uint32, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	reprioritized := &armadaevents.EventSequence_Event{
		Created: &ts,
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

func (js *JobSimulator) RunFailed(runId string, node string, exitCode int32, message string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	runFailed := &armadaevents.EventSequence_Event{
		Created: &ts,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: js.jobId,
				RunId: armadaevents.ProtoUuidFromUuid(uuid.MustParse(runId)),
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
	js.events = append(js.events, runFailed)

	js.job.LastActiveRunId = &runId
	updateRun(js.job, &runPatch{
		runId:       runId,
		exitCode:    &exitCode,
		finished:    &ts,
		jobRunState: pointer.String(string(lookout.JobRunFailed)),
		node:        &node,
	})
	return js
}

func (js *JobSimulator) Failed(node string, exitCode int32, message string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	failed := &armadaevents.EventSequence_Event{
		Created: &ts,
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

	js.job.LastTransitionTime = ts
	js.job.State = string(lookout.JobFailed)
	return js
}

func (js *JobSimulator) Preempted(timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	jobIdProto, err := armadaevents.ProtoUuidFromUlidString(util.NewULID())
	if err != nil {
		log.WithError(err).Errorf("Could not convert job ID to UUID: %s", util.NewULID())
	}

	preempted := &armadaevents.EventSequence_Event{
		Created: &ts,
		Event: &armadaevents.EventSequence_Event_JobRunPreempted{
			JobRunPreempted: &armadaevents.JobRunPreempted{
				PreemptedJobId:  js.jobId,
				PreemptiveJobId: jobIdProto,
				PreemptedRunId:  armadaevents.ProtoUuidFromUuid(uuid.MustParse(uuid.NewString())),
				PreemptiveRunId: armadaevents.ProtoUuidFromUuid(uuid.MustParse(uuid.NewString())),
			},
		},
	}
	js.events = append(js.events, preempted)

	js.job.LastTransitionTime = ts
	js.job.State = string(lookout.JobPreempted)
	return js
}

func (js *JobSimulator) RunTerminated(runId string, cluster string, node string, message string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	terminated := &armadaevents.EventSequence_Event{
		Created: &ts,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: js.jobId,
				RunId: armadaevents.ProtoUuidFromUuid(uuid.MustParse(runId)),
				Errors: []*armadaevents.Error{
					{
						Terminal: false,
						Reason: &armadaevents.Error_PodTerminated{
							PodTerminated: &armadaevents.PodTerminated{
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
	js.events = append(js.events, terminated)

	updateRun(js.job, &runPatch{
		runId:       runId,
		cluster:     &cluster,
		finished:    &ts,
		jobRunState: pointer.String(string(lookout.JobRunTerminated)),
		node:        &node,
	})
	return js
}

func (js *JobSimulator) RunUnschedulable(runId string, cluster string, node string, message string, timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	runUnschedulable := &armadaevents.EventSequence_Event{
		Created: &ts,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: js.jobId,
				RunId: armadaevents.ProtoUuidFromUuid(uuid.MustParse(runId)),
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

	updateRun(js.job, &runPatch{
		runId:       runId,
		cluster:     &cluster,
		finished:    &ts,
		jobRunState: pointer.String(string(lookout.JobRunUnableToSchedule)),
		node:        &node,
	})
	return js
}

func (js *JobSimulator) LeaseExpired(timestamp time.Time) *JobSimulator {
	ts := timestampOrNow(timestamp)
	leaseReturned := &armadaevents.EventSequence_Event{
		Created: &ts,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				JobId: js.jobId,
				RunId: eventutil.LegacyJobRunId(),
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

	updateRun(js.job, &runPatch{
		runId:       eventutil.LEGACY_RUN_ID,
		finished:    &ts,
		jobRunState: pointer.String(string(lookout.JobRunLeaseExpired)),
	})
	return js
}

func (js *JobSimulator) Build() *JobSimulator {
	eventSequence := &armadaevents.EventSequence{
		Queue:      js.queue,
		JobSetName: js.jobSet,
		UserId:     js.owner,
		Events:     js.events,
	}
	eventSequenceWithIds := &ingest.EventSequencesWithIds{
		EventSequences: []*armadaevents.EventSequence{eventSequence},
		MessageIds:     []pulsar.MessageID{pulsarutils.NewMessageId(1)},
	}
	instructionSet := js.converter.Convert(context.TODO(), eventSequenceWithIds)
	err := js.store.Store(context.TODO(), instructionSet)
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

func timestampOrNow(timestamp time.Time) time.Time {
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	return timestamp
}

func updateRun(job *model.Job, patch *runPatch) {
	for _, run := range job.Runs {
		if run.RunId == patch.runId {
			patchRun(run, patch)
			return
		}
	}
	cluster := ""
	if patch.cluster != nil {
		cluster = *patch.cluster
	}
	state := ""
	if patch.jobRunState != nil {
		state = *patch.jobRunState
	}
	job.Runs = append(job.Runs, &model.Run{
		Cluster:     cluster,
		ExitCode:    patch.exitCode,
		Finished:    patch.finished,
		JobRunState: state,
		Node:        patch.node,
		Leased:      patch.leased,
		Pending:     patch.pending,
		RunId:       patch.runId,
		Started:     patch.started,
	})
}

func patchRun(run *model.Run, patch *runPatch) {
	if patch.cluster != nil {
		run.Cluster = *patch.cluster
	}
	if patch.exitCode != nil {
		run.ExitCode = patch.exitCode
	}
	if patch.finished != nil {
		run.Finished = patch.finished
	}
	if patch.jobRunState != nil {
		run.JobRunState = *patch.jobRunState
	}
	if patch.node != nil {
		run.Node = patch.node
	}
	if patch.leased != nil {
		run.Leased = patch.leased
	}
	if patch.pending != nil {
		run.Pending = patch.pending
	}
	if patch.started != nil {
		run.Started = patch.started
	}
}

func prefixAnnotations(prefix string, annotations map[string]string) map[string]string {
	prefixed := make(map[string]string)
	for key, value := range annotations {
		prefixed[fmt.Sprintf("%s%s", prefix, key)] = value
	}
	return prefixed
}

func logQuery(query *Query) {
	log.Debug(removeNewlinesAndTabs(query.Sql))
	log.Debugf("%v", query.Args)
}

func removeNewlinesAndTabs(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\t", "")
}
