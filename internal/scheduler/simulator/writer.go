package simulator

import (
	"io"

	parquetWriter "github.com/xitongsys/parquet-go/writer"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type Writer struct {
	c      <-chan StateTransition
	writer io.Writer
}

type JobRunRow struct {
	Queue            string  `parquet:"name=queue, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	JobSet           string  `parquet:"name=job_set, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	JobId            string  `parquet:"name=job_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	RunId            string  `parquet:"name=run_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	PriorityClass    string  `parquet:"name=priority_class, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Cpu              float64 `parquet:"name=cpu, type=DOUBLE"`
	Memory           float64 `parquet:"name=memory, type=DOUBLE"`
	Gpu              float64 `parquet:"name=gpu, type=DOUBLE"`
	EphemeralStorage float64 `parquet:"name=ephemeral_storage, type=DOUBLE"`
	ExitCode         int     `parquet:"name=exit_code, type=INT32"`
	State            string  `parquet:"name=state, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SubmittedTime    int64   `parquet:"name=submitted_time, type=INT64"`
	ScheduledTime    int64   `parquet:"name=scheduled_time, type=INT64"`
	FinishedTime     int64   `parquet:"name=finished_time, type=INT64"`
}

func NewWriter(writer io.Writer, c <-chan StateTransition) (*Writer, error) {
	fw := &Writer{
		c:      c,
		writer: writer,
	}

	return fw, nil
}

// Presently only converts events supported in the simulator
func (w *Writer) toEventState(e *armadaevents.EventSequence_Event) string {
	switch e.GetEvent().(type) {
	case *armadaevents.EventSequence_Event_JobSucceeded:
		return "SUCCEEDED"
	case *armadaevents.EventSequence_Event_JobRunPreempted:
		return "PREEMPTED"
	case *armadaevents.EventSequence_Event_CancelledJob:
		return "CANCELLED"
	default:
		// Undefined event type
		return "UNKNOWN"
	}
}

func (w *Writer) createJobRunRow(st StateTransition) ([]*JobRunRow, error) {
	rows := make([]*JobRunRow, 0, len(st.EventSequence.Events))
	events := st.EventSequence
	jobsList := st.Jobs
	for i, event := range events.Events {
		// Assumes all supported events have an associated job
		associatedJob := jobsList[i]
		if event.GetCancelledJob() != nil || event.GetJobSucceeded() != nil || event.GetJobRunPreempted() != nil {
			// Resource requirements
			cpuLimit := associatedJob.ResourceRequirements().Requests[v1.ResourceCPU]
			memoryLimit := associatedJob.ResourceRequirements().Requests[v1.ResourceMemory]
			ephemeralStorageLimit := associatedJob.ResourceRequirements().Requests[v1.ResourceEphemeralStorage]
			gpuLimit := associatedJob.ResourceRequirements().Requests["nvidia.com/gpu"]
			eventTime := protoutil.ToStdTime(event.Created)

			rows = append(rows, &JobRunRow{
				Queue:            associatedJob.Queue(),
				JobSet:           associatedJob.Jobset(),
				JobId:            associatedJob.Id(),
				RunId:            associatedJob.LatestRun().Id().String(),
				PriorityClass:    associatedJob.PriorityClassName(),
				Cpu:              cpuLimit.AsApproximateFloat64(),
				Memory:           memoryLimit.AsApproximateFloat64(),
				Gpu:              gpuLimit.AsApproximateFloat64(),
				EphemeralStorage: ephemeralStorageLimit.AsApproximateFloat64(),
				ExitCode:         0,
				State:            w.toEventState(event),
				SubmittedTime:    associatedJob.SubmitTime().UnixNano() / 1000000000,
				ScheduledTime:    associatedJob.LatestRun().Created() / 1000000000,
				FinishedTime:     eventTime.UnixNano() / 1000000000,
			})
		}
	}
	return rows, nil
}

func (w *Writer) Run(ctx *armadacontext.Context) (err error) {
	pw, err := parquetWriter.NewParquetWriterFromWriter(w.writer, new(JobRunRow), 1)
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

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case stateTransitions, ok := <-w.c:
			if !ok {
				return
			}

			jobRunRows, err := w.createJobRunRow(stateTransitions)
			if err != nil {
				return err
			}

			for _, flattenedStateTransition := range jobRunRows {
				if err := pw.Write(*flattenedStateTransition); err != nil {
					return err
				}
			}
		}
	}
}
