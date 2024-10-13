package sink

import (
	"os"

	"github.com/armadaproject/armada/internal/common/armadacontext"

	"github.com/armadaproject/armada/internal/scheduler/scheduling"

	parquetWriter "github.com/xitongsys/parquet-go/writer"
)

type FairShareRow struct {
	Ts                int64   `parquet:"name=ts, type=INT64"`
	Queue             string  `parquet:"name=queue, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Pool              string  `parquet:"name=pool, type=BYTE_ARRAY, convertedtype=UTF8"`
	FairShare         float64 `parquet:"name=fair_share, type=DOUBLE"`
	AdjustedFairShare float64 `parquet:"name=adjusted_fair_share, type=DOUBLE"`
	ActualShare       float64 `parquet:"name=actual_share, type=DOUBLE"`
}

type FairShareWriter struct {
	writer *parquetWriter.ParquetWriter
}

func NewFairShareWriter(path string) (*FairShareWriter, error) {
	fileWriter, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	pw, err := parquetWriter.NewParquetWriterFromWriter(fileWriter, new(FairShareRow), 1)
	if err != nil {
		return nil, err
	}
	return &FairShareWriter{
		writer: pw,
	}, nil
}

func (j *FairShareWriter) Update(result *scheduling.SchedulerResult) error {
	for _, sctx := range result.SchedulingContexts {
		for _, qctx := range sctx.QueueSchedulingContexts {
			row := FairShareRow{
				Ts:                0,
				Queue:             qctx.Queue,
				Pool:              sctx.Pool,
				FairShare:         qctx.FairShare,
				AdjustedFairShare: qctx.AdjustedFairShare,
				ActualShare:       sctx.FairnessCostProvider.UnweightedCostFromQueue(qctx),
			}
			err := j.writer.Write(row)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (j *FairShareWriter) Close(ctx *armadacontext.Context) {
	err := j.writer.WriteStop()
	if err != nil {
		ctx.Warnf("Could not clearnly close fair share parquet file: %s", err)
	}
}
