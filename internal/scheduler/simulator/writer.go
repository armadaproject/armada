package simulator

import (
	io "io"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/pkg/errors"
	parquetWriter "github.com/xitongsys/parquet-go/writer"
	v1 "k8s.io/api/core/v1"
)

const (
	unsupportedEvent = iota
	submitJob        = iota
	jobRunLeased     = iota
	jobRunRunning    = iota
	jobSucceeded     = iota
	jobRunPreempted  = iota
	jobCancelled     = iota
)

type Writer struct {
	c                    <-chan StateTransition
	writer               io.Writer
	prevSeenEventByJobId map[string]*armadaevents.EventSequence_Event
}

type FlattenedArmadaEvent struct {
	Time                  int64   `parquet:"name=elapsed_time, type=INT64"`
	Queue                 string  `parquet:"name=queue, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	JobSet                string  `parquet:"name=job_set, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	JobId                 string  `parquet:"name=job_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RunIndex              int     `parquet:"name=run_index, type=INT32"`
	NumRuns               int     `parquet:"name=num_runs, type=INT32"`
	PriorityClass         string  `parquet:"name=priority_class, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PreviousEventType     int     `parquet:"name=previous_event_type, type=INT32"`
	EventType             int     `parquet:"name=event_type, type=INT32"`
	SecondsSinceLastEvent float64 `parquet:"name=seconds_since_last_event, type=DOUBLE"`
	Cpu                   float64 `parquet:"name=cpu, type=DOUBLE"`
	Memory                float64 `parquet:"name=memory, type=DOUBLE"`
	Gpu                   float64 `parquet:"name=gpu, type=DOUBLE"`
	EphemeralStorage      float64 `parquet:"name=ephemeral_storage, type=DOUBLE"`
	ExitCode              int     `parquet:"name=exit_code, type=INT32"`
}

func NewWriter(writer io.Writer, c <-chan StateTransition) (*Writer, error) {
	if c == nil {
		return nil, errors.Errorf("uninitialised channel passed into FileWriter")
	}

	fw := &Writer{
		c:                    c,
		writer:               writer,
		prevSeenEventByJobId: make(map[string]*armadaevents.EventSequence_Event),
	}

	return fw, nil
}

// Presently only converts events supported in the simulator
func (w *Writer) encodeEvent(e *armadaevents.EventSequence_Event) int {
	switch e.GetEvent().(type) {
	case *armadaevents.EventSequence_Event_SubmitJob:
		return submitJob
	case *armadaevents.EventSequence_Event_JobRunLeased:
		return jobRunLeased
	case *armadaevents.EventSequence_Event_JobRunRunning:
		return jobRunRunning
	case *armadaevents.EventSequence_Event_JobSucceeded:
		return jobSucceeded
	case *armadaevents.EventSequence_Event_JobRunPreempted:
		return jobRunPreempted
	case *armadaevents.EventSequence_Event_CancelledJob:
		return jobCancelled
	default:
		// Undefined event type
		return unsupportedEvent
	}
}

func (w *Writer) flattenStateTransition(flattenedStateTransitions []*FlattenedArmadaEvent, st StateTransition) ([]*FlattenedArmadaEvent, error) {
	startTime := time.Time{}

	events := st.EventSequence
	jobsList := st.Jobs
	for i, event := range events.Events {
		// Assumes all supported events have an associated job
		associatedJob := jobsList[i]
		prevSeenEvent := w.prevSeenEventByJobId[associatedJob.GetId()]
		// Resource requirements
		cpuLimit := associatedJob.GetResourceRequirements().Requests[v1.ResourceCPU]
		memoryLimit := associatedJob.GetResourceRequirements().Requests[v1.ResourceMemory]
		ephemeralStorageLimit := associatedJob.GetResourceRequirements().Requests[v1.ResourceEphemeralStorage]
		gpuLimit := associatedJob.GetResourceRequirements().Requests["nvidia.com/gpu"]

		prevEventType := 0
		prevEventTime := *event.Created
		if prevSeenEvent != nil {
			prevEventType = w.encodeEvent(prevSeenEvent)
			prevEventTime = *prevSeenEvent.Created
		}

		flattenedStateTransitions = append(flattenedStateTransitions, &FlattenedArmadaEvent{
			Time:                  event.Created.Sub(startTime).Milliseconds(),
			Queue:                 events.Queue,
			JobSet:                events.JobSetName,
			JobId:                 associatedJob.GetId(),
			RunIndex:              len(associatedJob.AllRuns()) - 1, // Assumed to be related to latest run in simulation
			NumRuns:               len(associatedJob.AllRuns()),
			PriorityClass:         associatedJob.GetPriorityClassName(),
			PreviousEventType:     prevEventType,
			EventType:             w.encodeEvent(event),
			SecondsSinceLastEvent: event.Created.Sub(prevEventTime).Seconds(),
			Cpu:                   cpuLimit.AsApproximateFloat64(),
			Memory:                memoryLimit.AsApproximateFloat64(),
			Gpu:                   gpuLimit.AsApproximateFloat64(),
			EphemeralStorage:      ephemeralStorageLimit.AsApproximateFloat64(),
			ExitCode:              0,
		})
		w.prevSeenEventByJobId[associatedJob.GetId()] = event

		if associatedJob.Succeeded() || associatedJob.Failed() || associatedJob.Cancelled() {
			delete(w.prevSeenEventByJobId, associatedJob.GetId())
		}
	}

	return flattenedStateTransitions, nil
}

func (w *Writer) Run(ctx *armadacontext.Context) (err error) {
	pw, err := parquetWriter.NewParquetWriterFromWriter(w.writer, new(FlattenedArmadaEvent), 1)
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			return
		}

		if err = pw.WriteStop(); err != nil {
			ctx.Errorf("error closing parquet writer: %s", err.Error())
			return
		}
	}()

	flattenedStateTransitions := make([]*FlattenedArmadaEvent, 100)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case stateTransitions, ok := <-w.c:
			if !ok {
				return
			}

			flattenedStateTransitions, err := w.flattenStateTransition(flattenedStateTransitions[0:0], stateTransitions)
			if err != nil {
				return err
			}

			for _, flattenedStateTransition := range flattenedStateTransitions {
				if err := pw.Write(*flattenedStateTransition); err != nil {
					return err
				}
			}
		}
	}
}
