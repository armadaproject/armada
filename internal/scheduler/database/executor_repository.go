package database

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// ExecutorRepository is an interface to be implemented by structs which provide executor information
type ExecutorRepository interface {
	// GetExecutors returns all known executors, regardless of their last heartbeat time
	GetExecutors(ctx *armadacontext.Context) ([]*schedulerobjects.Executor, error)
	// GetLastUpdateTimes returns a map of executor name -> last heartbeat time
	GetLastUpdateTimes(ctx *armadacontext.Context) (map[string]time.Time, error)
	// StoreExecutor persists the latest executor state
	StoreExecutor(ctx *armadacontext.Context, executor *schedulerobjects.Executor) error
}

// PostgresExecutorRepository is an implementation of ExecutorRepository that stores its state in postgres
type PostgresExecutorRepository struct {
	// pool of database connections
	db *pgxpool.Pool
	// proto objects are stored compressed
	compressor   compress.Compressor
	decompressor compress.Decompressor
}

func NewPostgresExecutorRepository(db *pgxpool.Pool) *PostgresExecutorRepository {
	return &PostgresExecutorRepository{
		db:           db,
		compressor:   compress.NewThreadSafeZlibCompressor(1024),
		decompressor: compress.NewThreadSafeZlibDecompressor(),
	}
}

// GetExecutors returns all known executors, regardless of their last heartbeat time
func (r *PostgresExecutorRepository) GetExecutors(ctx *armadacontext.Context) ([]*schedulerobjects.Executor, error) {
	queries := New(r.db)
	requests, err := queries.SelectAllExecutors(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	executors := make([]*schedulerobjects.Executor, len(requests))
	for i, request := range requests {
		executor := &schedulerobjects.Executor{}
		err := decompressAndMarshall(request.LastRequest, r.decompressor, executor)
		if err != nil {
			return nil, err
		}
		executors[i] = executor
	}
	return executors, nil
}

// GetLastUpdateTimes returns a map of executor name -> last heartbeat time
func (r *PostgresExecutorRepository) GetLastUpdateTimes(ctx *armadacontext.Context) (map[string]time.Time, error) {
	queries := New(r.db)
	rows, err := queries.SelectExecutorUpdateTimes(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	lastUpdateTimes := make(map[string]time.Time, len(rows))
	for _, row := range rows {
		// pgx defaults to local time, so we convert to utc here
		lastUpdateTimes[row.ExecutorID] = row.LastUpdated.UTC()
	}
	return lastUpdateTimes, nil
}

// StoreExecutor persists the latest executor state
func (r *PostgresExecutorRepository) StoreExecutor(ctx *armadacontext.Context, executor *schedulerobjects.Executor) error {
	queries := New(r.db)
	bytes, err := proto.Marshal(executor)
	if err != nil {
		return errors.WithStack(err)
	}
	compressed, err := r.compressor.Compress(bytes)
	if err != nil {
		return errors.WithStack(err)
	}
	err = queries.UpsertExecutor(ctx, UpsertExecutorParams{
		ExecutorID:  executor.Id,
		LastRequest: compressed,
		UpdateTime:  executor.LastUpdateTime,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func decompressAndMarshall(b []byte, decompressor compress.Decompressor, msg proto.Message) error {
	decompressed, err := decompressor.Decompress(b)
	if err != nil {
		return err
	}
	return proto.Unmarshal(decompressed, msg)
}
