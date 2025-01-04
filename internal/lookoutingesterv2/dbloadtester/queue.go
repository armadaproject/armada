package dbloadtester

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	log "github.com/armadaproject/armada/internal/common/logging"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

var templateAnnotations = map[string]string{
	"armadaproject.io/attempt":                 "0",
	"armadaproject.io/custom_name":             "jobcustomnameforreference",
	"armadaproject.io/stage":                   "first_stage_of_complex_workflow",
	"armadaproject.io/grade":                   "Dev",
	"armadaproject.io/tracking_guid":           "Loremipsumdolorsitametconsecteturadi",
	"armadaproject.io/tracking_name":           "00000000",
	"armadaproject.io/job-request-application": "orchestrate",
	"armadaproject.io/namespace":               "queueName",
	"armadaproject.io/task":                    "loremipsumdolorsitametconsecteturadipiscingelitseddoeiusmodte",
	"armadaproject.io/task_name":               "Loremipsumdolors",
	"armadaproject.io/workflow":                "workflow",
	"armadaproject.io/log-path":                "loremipsumdolorsi/ametconsecteturadipiscin/elitseddoeiusmodtemporinc/diduntu",
	"armadaproject.io/version":                 "0.00.00",
}

type QueueEventGenerator struct {
	queueName         string
	jobSetName        string
	totalJobs         int
	creationBatchSize int

	jobToJobState      map[string]int
	jobIdToJobRun      map[string]string
	newEventTimeToJobs map[int64][]string

	activeJobs    int
	maxActiveJobs int
	completedJobs int

	stepSeconds int64
	startTime   int64

	jobTemplate *v1.PodSpec
}

func NewQueueEventGenerator(queueName, jobSetName string, totalJobs, maxActiveJobs, creationBatchSize int, timeStep int64, jobTemplate *v1.PodSpec) *QueueEventGenerator {
	return &QueueEventGenerator{
		queueName:          queueName,
		jobSetName:         jobSetName,
		totalJobs:          totalJobs,
		creationBatchSize:  creationBatchSize,
		maxActiveJobs:      maxActiveJobs,
		jobToJobState:      map[string]int{},
		jobIdToJobRun:      map[string]string{},
		newEventTimeToJobs: map[int64][]string{},
		activeJobs:         0,
		completedJobs:      0,
		stepSeconds:        timeStep,
		jobTemplate:        jobTemplate,
	}
}

func (q *QueueEventGenerator) Generate(eventsCh chan<- *utils.EventsWithIds[*armadaevents.EventSequence]) {
	totalEventsGenerated := 0
	for i := q.startTime; ; i += q.stepSeconds {
		if q.completedJobs >= q.totalJobs {
			return
		}
		events, err := q.generateEventsAtTime(i)
		if err != nil {
			log.Fatalf("failed to generate events %s", err)
		}
		if len(events) == 0 {
			continue
		}
		eventSequenceWithIds := &utils.EventsWithIds[*armadaevents.EventSequence]{
			Events: []*armadaevents.EventSequence{
				{
					Queue:      q.queueName,
					JobSetName: q.jobSetName,
					UserId:     q.queueName,
					Events:     events,
				},
			},
			MessageIds: make([]pulsar.MessageID, len(events)),
		}
		totalEventsGenerated += len(events)
		// log.Infof("Queue %s generated %d events so far", q.queueName, totalEventsGenerated)
		eventsCh <- eventSequenceWithIds
	}
}

func (q *QueueEventGenerator) generateEventsAtTime(t int64) ([]*armadaevents.EventSequence_Event, error) {
	newEventJobs := q.newEventTimeToJobs[t]
	var eventsToPublish []*armadaevents.EventSequence_Event

	submitEvents, err := q.generateSubmitEvents(t)
	if err != nil {
		return nil, err
	}

	eventsToPublish = append(eventsToPublish, submitEvents...)
	for _, jobId := range newEventJobs {
		jobState := q.jobToJobState[jobId]
		newJobState := q.getNextState(jobState)

		jobEvents, err := q.getEventsFromTargetState(t, jobId, q.jobIdToJobRun[jobId], newJobState)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to create events from target state")
		}

		eventsToPublish = append(eventsToPublish, jobEvents...)
		if isStateTerminal(newJobState) {
			delete(q.jobToJobState, jobId)
			delete(q.jobIdToJobRun, jobId)
			q.activeJobs -= 1
			q.completedJobs += 1
		} else {
			timeAtNextEvent := t + q.getStateDuration(newJobState)
			q.jobToJobState[jobId] = newJobState
			q.newEventTimeToJobs[timeAtNextEvent] = append(q.newEventTimeToJobs[timeAtNextEvent], jobId)
		}
	}

	// clean up the event generation tracker at time t, as they have all now been processed
	delete(q.newEventTimeToJobs, t)

	return eventsToPublish, nil
}

func (q *QueueEventGenerator) generateSubmitEvents(t int64) ([]*armadaevents.EventSequence_Event, error) {
	available := q.maxActiveJobs - q.activeJobs
	remainingJobs := q.totalJobs - q.completedJobs - q.activeJobs
	if available == 0 || remainingJobs == 0 {
		return nil, nil
	}
	submitEvents := make([]*armadaevents.EventSequence_Event, min(q.creationBatchSize, remainingJobs, available))
	for i := range submitEvents {
		jobId := util.NewULID()
		jobRunId := uuid.NewString()

		q.jobToJobState[jobId] = lookout.JobQueuedOrdinal
		q.jobIdToJobRun[jobId] = jobRunId
		nextEventTime := t + q.getStateDuration(lookout.JobQueuedOrdinal)

		q.newEventTimeToJobs[nextEventTime] = append(q.newEventTimeToJobs[nextEventTime], jobId)

		events, err := q.getEventsFromTargetState(t, jobId, jobRunId, lookout.JobQueuedOrdinal)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to create submit event")
		}
		submitEvents[i] = events[0]
	}
	q.activeJobs += len(submitEvents)

	return submitEvents, nil
}

func (q *QueueEventGenerator) getEventsFromTargetState(
	t int64,
	jobId string,
	jobRunId string,
	targetState int,
) ([]*armadaevents.EventSequence_Event, error) {
	createdTime := protoutil.ToTimestamp(time.Unix(t, 0))
	switch targetState {
	case lookout.JobQueuedOrdinal:
		return []*armadaevents.EventSequence_Event{
			{
				Event: &armadaevents.EventSequence_Event_SubmitJob{
					SubmitJob: q.getJob(jobId),
				},
				Created: createdTime,
			},
		}, nil
	case lookout.JobLeasedOrdinal:
		return []*armadaevents.EventSequence_Event{
			{
				Event: &armadaevents.EventSequence_Event_JobRunLeased{
					JobRunLeased: &armadaevents.JobRunLeased{
						JobId:  jobId,
						RunId:  jobRunId,
						NodeId: "one_true_node",
					},
				},
				Created: createdTime,
			},
		}, nil
	case lookout.JobPendingOrdinal:
		return []*armadaevents.EventSequence_Event{
			{
				Event: &armadaevents.EventSequence_Event_JobRunAssigned{
					JobRunAssigned: &armadaevents.JobRunAssigned{
						JobId: jobId,
						RunId: jobRunId,
					},
				},
				Created: createdTime,
			},
		}, nil
	case lookout.JobRunningOrdinal:
		return []*armadaevents.EventSequence_Event{
			{
				Event: &armadaevents.EventSequence_Event_JobRunRunning{
					JobRunRunning: &armadaevents.JobRunRunning{
						JobId: jobId,
						RunId: jobRunId,
					},
				},
				Created: createdTime,
			},
		}, nil
	case lookout.JobSucceededOrdinal:
		return []*armadaevents.EventSequence_Event{
			{
				Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
					JobRunSucceeded: &armadaevents.JobRunSucceeded{
						JobId: jobId,
						RunId: jobRunId,
					},
				},
				Created: createdTime,
			},
			{
				Event: &armadaevents.EventSequence_Event_JobSucceeded{
					JobSucceeded: &armadaevents.JobSucceeded{
						JobId: jobId,
					},
				},
				Created: createdTime,
			},
		}, nil
	}

	return nil, fmt.Errorf("unknown target state %d %s", targetState, lookout.JobStateMap[targetState])
}

func (q *QueueEventGenerator) getNextState(state int) int {
	// submitted to leased, leased to pending, pending to running,
	switch state {
	case 0:
		return lookout.JobQueuedOrdinal
	case lookout.JobQueuedOrdinal:
		return lookout.JobLeasedOrdinal
	case lookout.JobLeasedOrdinal:
		return lookout.JobPendingOrdinal
	case lookout.JobPendingOrdinal:
		return lookout.JobRunningOrdinal
	case lookout.JobRunningOrdinal:
		return lookout.JobSucceededOrdinal
	default:
		return lookout.JobSucceededOrdinal
	}
}

func (q *QueueEventGenerator) getStateDuration(state int) int64 {
	switch state {
	case lookout.JobQueuedOrdinal:
		return q.getEventDuration(60 * q.stepSeconds)
	case lookout.JobLeasedOrdinal:
		return q.getEventDuration(10 * q.stepSeconds)
	case lookout.JobPendingOrdinal:
		return q.getEventDuration(10 * q.stepSeconds)
	case lookout.JobRunningOrdinal:
		return q.getEventDuration(120 * q.stepSeconds)
	default:
		return q.stepSeconds
	}
}

// getEventDuration returns the time before the next event, calculated as a random number of steps between the
// minDuration and max duration of one stepSeconds larger than two times the minDuration.
func (q *QueueEventGenerator) getEventDuration(minDuration int64) int64 {
	return minDuration + rand.Int63n((minDuration+q.stepSeconds)/q.stepSeconds+1)*q.stepSeconds
}

func (q *QueueEventGenerator) getJob(jobId string) *armadaevents.SubmitJob {
	submitJob := &armadaevents.SubmitJob{
		JobId:    jobId,
		Priority: 1000,
		ObjectMeta: &armadaevents.ObjectMeta{
			Namespace:   fmt.Sprintf("gold-%s", q.queueName),
			Annotations: templateAnnotations,
		},
		MainObject: &armadaevents.KubernetesMainObject{
			Object: &armadaevents.KubernetesMainObject_PodSpec{
				PodSpec: &armadaevents.PodSpecWithAvoidList{
					PodSpec: q.jobTemplate,
				},
			},
		},
	}

	return submitJob
}

func isStateTerminal(state int) bool {
	switch state {
	case lookout.JobQueuedOrdinal:
		return false
	case lookout.JobLeasedOrdinal:
		return false
	case lookout.JobPendingOrdinal:
		return false
	case lookout.JobRunningOrdinal:
		return false
	case lookout.JobSucceededOrdinal:
		return true
	default:
		return true
	}
}
