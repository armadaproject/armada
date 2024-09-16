package sink

import (
	parquetWriter "github.com/xitongsys/parquet-go/writer"
	"os"
)

type StatsWriter struct {
	writer *parquetWriter.ParquetWriter
}

func NewStatsWriter(path string) (*StatsWriter, error) {
	fileWriter, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	pw, err := parquetWriter.NewParquetWriterFromWriter(fileWriter, new(JobRunRow), 1)
	if err != nil {
		return nil, err
	}
	return &StatsWriter{
		writer: pw,
	}, nil
}

func (j *StatsWriter) Close() {
	j.writer.WriteStop()
}
