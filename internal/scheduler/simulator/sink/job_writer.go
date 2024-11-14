package sink

import (
	"os"

	parquetWriter "github.com/xitongsys/parquet-go/writer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/scheduler/simulator/model"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type JobWriter struct {
	writer *parquetWriter.ParquetWriter
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

func NewJobWriter(path string) (*JobWriter, error) {
	fileWriter, err := os.Create(path + "/jobs.parquet")
	if err != nil {
		return nil, err
	}
	pw, err := parquetWriter.NewParquetWriterFromWriter(fileWriter, new(JobRunRow), 1)
	if err != nil {
		return nil, err
	}
	return &JobWriter{
		writer: pw,
	}, nil
}

func (j *JobWriter) Update(st *model.StateTransition) error {
	jobRunRows, err := j.createJobRunRow(st)
	if err != nil {
		return err
	}

	for _, jobRow := range jobRunRows {
		if err := j.writer.Write(*jobRow); err != nil {
			return err
		}
	}
	return nil
}

func (j *JobWriter) Close(ctx *armadacontext.Context) {
	err := j.writer.WriteStop()
	if err != nil {
		ctx.Warnf("Could not clearnly close fair share parquet file: %s", err)
	}
}

func (j *JobWriter) createJobRunRow(st *model.StateTransition) ([]*JobRunRow, error) {
	rows := make([]*JobRunRow, 0, len(st.EventSequence.Events))
	events := st.EventSequence
	jobsList := st.Jobs
	for i, event := range events.Events {
		// Assumes all supported events have an associated job
		associatedJob := jobsList[i]
		if event.GetCancelledJob() != nil || event.GetJobSucceeded() != nil || event.GetJobRunPreempted() != nil {
			// Resource requirements
			cpuLimit := associatedJob.AllResourceRequirements().GetResourceByNameZeroIfMissing("cpu")
			memoryLimit := associatedJob.AllResourceRequirements().GetResourceByNameZeroIfMissing("memory")
			ephemeralStorageLimit := associatedJob.AllResourceRequirements().GetResourceByNameZeroIfMissing("ephemeral-storage")
			gpuLimit := associatedJob.AllResourceRequirements().GetResourceByNameZeroIfMissing("nvidia.com/gpu")
			eventTime := protoutil.ToStdTime(event.Created)

			rows = append(rows, &JobRunRow{
				Queue:            associatedJob.Queue(),
				JobSet:           associatedJob.Jobset(),
				JobId:            associatedJob.Id(),
				RunId:            associatedJob.LatestRun().Id(),
				PriorityClass:    associatedJob.PriorityClassName(),
				Cpu:              cpuLimit.AsApproximateFloat64(),
				Memory:           memoryLimit.AsApproximateFloat64(),
				Gpu:              gpuLimit.AsApproximateFloat64(),
				EphemeralStorage: ephemeralStorageLimit.AsApproximateFloat64(),
				ExitCode:         0,
				State:            j.toEventState(event),
				SubmittedTime:    associatedJob.SubmitTime().UnixNano() / 1000000000,
				ScheduledTime:    associatedJob.LatestRun().Created() / 1000000000,
				FinishedTime:     eventTime.UnixNano() / 1000000000,
			})
		}
	}
	return rows, nil
}

// Presently only converts events supported in the simulator
func (j *JobWriter) toEventState(e *armadaevents.EventSequence_Event) string {
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
