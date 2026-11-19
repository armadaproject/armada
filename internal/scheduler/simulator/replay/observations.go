package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	parquetwriter "github.com/xitongsys/parquet-go/writer"
)

const (
	attemptsFile             = "attempts.parquet"
	queueIntervalsFile       = "queue_intervals.parquet"
	observationsManifestFile = "observations.json"
	observationsVersion      = 1
)

type attemptRow struct {
	JobID          string `parquet:"name=logical_job_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	Ordinal        int64  `parquet:"name=attempt, type=INT64"`
	RunID          string `parquet:"name=run_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	SchedulingPool string `parquet:"name=scheduling_pool, type=BYTE_ARRAY, convertedtype=UTF8"`
	Pool           string `parquet:"name=pool, type=BYTE_ARRAY, convertedtype=UTF8"`
	Node           string `parquet:"name=node, type=BYTE_ARRAY, convertedtype=UTF8"`
	Requests       string `parquet:"name=requests_json, type=BYTE_ARRAY, convertedtype=UTF8"`
	AllocatedUS    int64  `parquet:"name=allocated_at_us, type=INT64"`
	EndUS          int64  `parquet:"name=end_at_us, type=INT64"`
	State          string `parquet:"name=state, type=BYTE_ARRAY, convertedtype=UTF8"`
	Reason         string `parquet:"name=reason, type=BYTE_ARRAY, convertedtype=UTF8"`
	StartupUS      int64  `parquet:"name=startup_us, type=INT64"`
	RetainedUS     int64  `parquet:"name=retained_us, type=INT64"`
	LostUS         int64  `parquet:"name=lost_us, type=INT64"`
	ProvisionalUS  int64  `parquet:"name=provisional_us, type=INT64"`
}

type queueIntervalRow struct {
	JobID    string `parquet:"name=logical_job_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	QueuedUS int64  `parquet:"name=queued_at_us, type=INT64"`
	EndUS    int64  `parquet:"name=end_at_us, type=INT64"`
	Censored bool   `parquet:"name=censored, type=BOOLEAN"`
}

type observationsManifest struct {
	Version int               `json:"version"`
	SHA256  map[string]string `json:"sha256"`
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
		Requests: string(requests), AllocatedUS: row.AllocatedUS, EndUS: row.EndUS,
		State: string(row.State), Reason: row.Reason, StartupUS: row.StartupUS,
		RetainedUS: row.RetainedUS, LostUS: row.LostUS, ProvisionalUS: row.ProvisionalUS,
	})
}

func (o *comparisonObservations) Queue(interval QueueInterval) error {
	return o.queues.writer.Write(queueIntervalRow{
		JobID: interval.Offered.ID, QueuedUS: interval.QueuedUS,
		EndUS: interval.EndUS, Censored: interval.Censored,
	})
}

func writeObservationsManifest(ctx context.Context, dir string) error {
	manifest := observationsManifest{Version: observationsVersion, SHA256: make(map[string]string, 2)}
	for _, name := range []string{attemptsFile, queueIntervalsFile} {
		hash, err := hashFile(ctx, filepath.Join(dir, name))
		if err != nil {
			return err
		}
		manifest.SHA256[name] = hash
	}
	return writeJSON(filepath.Join(dir, observationsManifestFile), manifest)
}

func verifyObservations(ctx context.Context, dir string) error {
	raw, err := readLocal(filepath.Join(dir, observationsManifestFile), maxInput)
	if errors.Is(err, os.ErrNotExist) {
		// Older workers write only run.json. An artifact without a manifest is incomplete.
		for _, name := range []string{attemptsFile, queueIntervalsFile} {
			_, statErr := os.Lstat(filepath.Join(dir, name))
			if statErr == nil {
				return errors.New("missing Parquet observations manifest")
			}
			if !errors.Is(statErr, os.ErrNotExist) {
				return statErr
			}
		}
		return nil
	}
	if err != nil {
		return err
	}
	var manifest observationsManifest
	if err := strictJSON(raw, &manifest); err != nil {
		return err
	}
	if manifest.Version != observationsVersion || len(manifest.SHA256) != 2 {
		return errors.New("invalid Parquet observations manifest")
	}
	for _, name := range []string{attemptsFile, queueIntervalsFile} {
		hash, err := hashFile(ctx, filepath.Join(dir, name))
		if err != nil {
			return err
		}
		if hash != manifest.SHA256[name] {
			return fmt.Errorf("Parquet observations checksum mismatch for %s", name)
		}
	}
	return nil
}
