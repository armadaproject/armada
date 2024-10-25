package sink

import (
	"os"
	"time"

	parquetWriter "github.com/xitongsys/parquet-go/writer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type QueueStatsRow struct {
	Ts                int64   `parquet:"name=ts, type=INT64"`
	Queue             string  `parquet:"name=queue, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Pool              string  `parquet:"name=pool, type=BYTE_ARRAY, convertedtype=UTF8"`
	FairShare         float64 `parquet:"name=fair_share, type=DOUBLE"`
	AdjustedFairShare float64 `parquet:"name=adjusted_fair_share, type=DOUBLE"`
	ActualShare       float64 `parquet:"name=actual_share, type=DOUBLE"`
	CpuShare          float64 `parquet:"name=cpu_share, type=DOUBLE"`
	MemoryShare       float64 `parquet:"name=memory_share, type=DOUBLE"`
	GpuShare          float64 `parquet:"name=gpu_share, type=DOUBLE"`
	AllocatedCPU      int     `parquet:"name=allocated_cpu, type=INT64"`
	AllocatedMemory   int     `parquet:"name=allocated_memory, type=INT64"`
	AllocatedGPU      int     `parquet:"name=allocated_gpu, type=INT64"`
	NumScheduled      int     `parquet:"name=num_scheduled, type=INT32"`
	NumPreempted      int     `parquet:"name=num_preempted, type=INT32"`
	NumEvicted        int     `parquet:"name=num_evicted, type=INT32"`
}

type QueueStatsWriter struct {
	writer *parquetWriter.ParquetWriter
}

func NewQueueStatsWriter(path string) (*QueueStatsWriter, error) {
	fileWriter, err := os.Create(path + "/queue_stats.parquet")
	if err != nil {
		return nil, err
	}
	pw, err := parquetWriter.NewParquetWriterFromWriter(fileWriter, new(QueueStatsRow), 1)
	if err != nil {
		return nil, err
	}
	return &QueueStatsWriter{
		writer: pw,
	}, nil
}

func (j *QueueStatsWriter) Update(time time.Time, result *scheduling.SchedulerResult) error {
	// Work out number of preemptions per queue
	preemptedJobsByQueue := map[string]int{}
	for _, job := range result.PreemptedJobs {
		preemptedJobsByQueue[job.Job.Queue()] = preemptedJobsByQueue[job.Job.Queue()] + 1
	}

	for _, sctx := range result.SchedulingContexts {
		for _, qctx := range sctx.QueueSchedulingContexts {
			row := QueueStatsRow{
				Ts:                time.Unix(),
				Queue:             qctx.Queue,
				Pool:              sctx.Pool,
				FairShare:         qctx.FairShare,
				AdjustedFairShare: qctx.AdjustedFairShare,
				ActualShare:       sctx.FairnessCostProvider.UnweightedCostFromQueue(qctx),
				CpuShare:          calculateResourceShare(sctx, qctx, "cpu"),
				MemoryShare:       calculateResourceShare(sctx, qctx, "memory"),
				GpuShare:          calculateResourceShare(sctx, qctx, "nvidia.com/gpu"),
				AllocatedCPU:      allocatedResources(qctx, "cpu"),
				AllocatedMemory:   allocatedResources(qctx, "memory") / (1024 * 1024), // in MB
				AllocatedGPU:      allocatedResources(qctx, "nvidia.com/gpu"),
				NumScheduled:      len(qctx.SuccessfulJobSchedulingContexts),
				NumPreempted:      preemptedJobsByQueue[qctx.Queue],
				NumEvicted:        len(qctx.EvictedJobsById),
			}
			err := j.writer.Write(row)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (j *QueueStatsWriter) Close(ctx *armadacontext.Context) {
	err := j.writer.WriteStop()
	if err != nil {
		ctx.Warnf("Could not cleanly close queue_stats parquet file: %s", err)
	}
}

func calculateResourceShare(sctx *context.SchedulingContext, qctx *context.QueueSchedulingContext, resource string) float64 {
	total := sctx.Allocated.Resources[resource]
	allocated := qctx.Allocated.Resources[resource]
	return allocated.AsApproximateFloat64() / total.AsApproximateFloat64()
}

func allocatedResources(qctx *context.QueueSchedulingContext, resource string) int {
	allocated := qctx.Allocated.Resources[resource]
	return int(allocated.AsApproximateFloat64())
}
