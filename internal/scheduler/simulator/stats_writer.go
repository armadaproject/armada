package simulator

import (
	"io"

	parquetWriter "github.com/xitongsys/parquet-go/writer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

type StatsWriter struct {
	c      <-chan CycleStats
	writer io.Writer
}

func NewStatsWriter(writer io.Writer, c <-chan CycleStats) (*StatsWriter, error) {
	sw := &StatsWriter{
		c:      c,
		writer: writer,
	}

	return sw, nil
}

func (w *StatsWriter) Run(ctx *armadacontext.Context) (err error) {
	pw, err := parquetWriter.NewParquetWriterFromWriter(w.writer, new(CycleStats), 1)
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
		case cycleStats, ok := <-w.c:
			if !ok {
				return
			}
			if err := pw.Write(cycleStats); err != nil {
				return err
			}
		}
	}
}
