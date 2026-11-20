package replay

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	parquetreader "github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/schema"
	parquetwriter "github.com/xitongsys/parquet-go/writer"
)

const (
	attemptsFile       = "attempts.parquet"
	queueIntervalsFile = "queue_intervals.parquet"
)

type attemptRow struct {
	JobID              string `parquet:"name=logical_job_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	Ordinal            int64  `parquet:"name=attempt, type=INT64"`
	RunID              string `parquet:"name=run_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	SchedulingPool     string `parquet:"name=scheduling_pool, type=BYTE_ARRAY, convertedtype=UTF8"`
	Pool               string `parquet:"name=pool, type=BYTE_ARRAY, convertedtype=UTF8"`
	Node               string `parquet:"name=node, type=BYTE_ARRAY, convertedtype=UTF8"`
	Requests           string `parquet:"name=requests_json, type=BYTE_ARRAY, convertedtype=UTF8"`
	AllocatedSeconds   int64  `parquet:"name=allocated_at_s, type=INT64"`
	EndSeconds         int64  `parquet:"name=end_at_s, type=INT64"`
	State              string `parquet:"name=state, type=BYTE_ARRAY, convertedtype=UTF8"`
	Reason             string `parquet:"name=reason, type=BYTE_ARRAY, convertedtype=UTF8"`
	StartupSeconds     int64  `parquet:"name=startup_s, type=INT64"`
	RetainedSeconds    int64  `parquet:"name=retained_s, type=INT64"`
	LostSeconds        int64  `parquet:"name=lost_s, type=INT64"`
	ProvisionalSeconds int64  `parquet:"name=provisional_s, type=INT64"`
}

type queueIntervalRow struct {
	JobID         string `parquet:"name=logical_job_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	QueuedSeconds int64  `parquet:"name=queued_at_s, type=INT64"`
	EndSeconds    int64  `parquet:"name=end_at_s, type=INT64"`
	Censored      bool   `parquet:"name=censored, type=BOOLEAN"`
}

type parquetOutput struct {
	file   *os.File
	writer *parquetwriter.ParquetWriter
}

func createParquet(path string, row any) (*parquetOutput, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	writer, err := parquetwriter.NewParquetWriterFromWriter(f, row, 1)
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	writer.RowGroupSize = 4 << 20
	return &parquetOutput{file: f, writer: writer}, nil
}

func (p *parquetOutput) close() error {
	if p.file == nil {
		return nil
	}
	f := p.file
	p.file = nil
	if err := errors.Join(p.writer.WriteStop(), f.Close()); err != nil {
		return fmt.Errorf("finalize %s: %w", f.Name(), err)
	}
	return nil
}

func (o *comparisonObservations) writeAttempt(row AttemptResult) error {
	requests, err := json.Marshal(row.Offered.Requests)
	if err != nil {
		return err
	}
	return o.attempts.writer.Write(attemptRow{
		JobID: row.JobID, Ordinal: row.Ordinal, RunID: row.Placement.RunID,
		SchedulingPool: row.Placement.SchedulingPool, Pool: row.Placement.Pool, Node: row.Placement.Node,
		Requests: string(requests), AllocatedSeconds: row.AllocatedSeconds, EndSeconds: row.EndSeconds,
		State: string(row.State), Reason: row.Reason, StartupSeconds: row.StartupSeconds,
		RetainedSeconds: row.RetainedSeconds, LostSeconds: row.LostSeconds, ProvisionalSeconds: row.ProvisionalSeconds,
	})
}

func (o *comparisonObservations) Queue(interval QueueInterval) error {
	return o.queues.writer.Write(queueIntervalRow{
		JobID: interval.Offered.ID, QueuedSeconds: interval.QueuedSeconds,
		EndSeconds: interval.EndSeconds, Censored: interval.Censored,
	})
}

func verifyObservations(dir string) error {
	for _, artifact := range []struct {
		name string
		row  any
	}{
		{attemptsFile, new(attemptRow)},
		{queueIntervalsFile, new(queueIntervalRow)},
	} {
		path := filepath.Join(dir, artifact.name)
		if err := checkParquetSchema(path, artifact.row); err != nil {
			return fmt.Errorf("%s: %w", artifact.name, err)
		}
	}
	return nil
}

func checkParquetSchema(path string, row any) error {
	const magic = "PAR1"
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("expected a regular Parquet file")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	var header [4]byte
	var trailer [8]byte
	maxFooterSize := info.Size() - int64(len(header)+len(trailer))
	if maxFooterSize <= 0 {
		return errors.New("invalid Parquet file size")
	}
	if _, err := file.ReadAt(header[:], 0); err != nil {
		return err
	}
	if _, err := file.ReadAt(trailer[:], info.Size()-int64(len(trailer))); err != nil {
		return err
	}
	footerSize := int64(binary.LittleEndian.Uint32(trailer[:4]))
	if string(header[:]) != magic || string(trailer[4:]) != magic ||
		footerSize <= 0 || footerSize > maxFooterSize {
		return errors.New("invalid Parquet signature or footer length")
	}
	reader := parquetreader.ParquetReader{PFile: &local.LocalFile{File: file}}
	if err := reader.ReadFooter(); err != nil {
		return fmt.Errorf("invalid Parquet footer: %w", err)
	}
	expected, err := schema.NewSchemaHandlerFromStruct(row)
	if err != nil {
		return err
	}
	actual := reader.Footer.Schema
	if len(actual) == 0 || actual[0].Type != nil {
		return errors.New("invalid Parquet schema root")
	}
	rootFields := make(map[string]*parquet.SchemaElement)
	var descendants int64
	// The footer lists schema elements in preorder. Only root fields satisfy
	// the observation contract. Skip the descendants of extra fields.
	for _, column := range actual[1:] {
		children := column.GetNumChildren()
		if children < 0 {
			return errors.New("invalid Parquet schema shape")
		}
		if descendants == 0 {
			if _, exists := rootFields[column.Name]; exists {
				return fmt.Errorf("duplicate Parquet field %q", column.Name)
			}
			rootFields[column.Name] = column
		} else {
			descendants--
		}
		descendants += int64(children)
	}
	if descendants != 0 || len(rootFields) != int(actual[0].GetNumChildren()) {
		return errors.New("invalid Parquet schema shape")
	}
	for i, column := range expected.SchemaElements[1:] {
		name := expected.Infos[i+1].ExName
		field, ok := rootFields[name]
		if !ok {
			return fmt.Errorf("missing Parquet column %q", name)
		}
		if field.Type == nil || field.GetNumChildren() != 0 {
			return fmt.Errorf("Parquet column %q is a group, expected %s", name, *column.Type)
		}
		if *field.Type != *column.Type {
			return fmt.Errorf("Parquet column %q has type %s, expected %s", name, *field.Type, *column.Type)
		}
	}
	return nil
}
