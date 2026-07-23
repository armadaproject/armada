package database

import (
	"hash/fnv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/constants"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

var (
	ErrExecutorDeleted    = errors.New("executor has been deleted")
	ErrExecutorNotDrained = errors.New("executor not drained")
)

// ExecutorRepository is an interface to be implemented by structs which provide executor information
type ExecutorRepository interface {
	// GetExecutors returns all known executors, regardless of their last heartbeat time
	GetExecutors(ctx *armadacontext.Context) ([]*schedulerobjects.Executor, error)
	// GetExecutorSettings returns all defined executor settings
	GetExecutorSettings(ctx *armadacontext.Context) ([]*schedulerobjects.ExecutorSettings, error)
	// GetLastUpdateTimes returns a map of executor name -> last heartbeat time
	GetLastUpdateTimes(ctx *armadacontext.Context) (map[string]time.Time, error)
	// StoreExecutor persists the latest executor state
	StoreExecutor(ctx *armadacontext.Context, executor *schedulerobjects.Executor) error
	// DeleteExecutor removes an executor from the database
	DeleteExecutor(ctx *armadacontext.Context, executorId string) error
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
		// pgx defaults to local time so we convert to utc here
		lastUpdateTimes[row.ExecutorID] = row.LastUpdated.UTC()
	}
	return lastUpdateTimes, nil
}

// StoreExecutor persists the latest executor state
func (r *PostgresExecutorRepository) StoreExecutor(ctx *armadacontext.Context, executor *schedulerobjects.Executor) error {
	bytes, err := proto.Marshal(executor)
	if err != nil {
		return errors.WithStack(err)
	}
	compressed, err := r.compressor.Compress(bytes)
	if err != nil {
		return errors.WithStack(err)
	}
	err = pgx.BeginTxFunc(ctx, r.db, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		if err := acquireExecutorLock(ctx, tx, executor.Id); err != nil {
			return err
		}
		queries := New(tx)
		rowsAffected, err := queries.UpsertExecutor(ctx, UpsertExecutorParams{
			ExecutorID:   executor.Id,
			LastRequest:  compressed,
			UpdateTime:   protoutil.ToStdTime(executor.LastUpdateTime),
			CordonReason: constants.ExecutorDeletedCordonReason,
		})
		if err != nil {
			return errors.WithStack(err)
		}
		if rowsAffected == 0 {
			return errors.WithStack(ErrExecutorDeleted)
		}
		return nil
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r *PostgresExecutorRepository) GetExecutorSettings(ctx *armadacontext.Context) ([]*schedulerobjects.ExecutorSettings, error) {
	queries := New(r.db)
	results, err := queries.SelectAllExecutorSettings(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	executorSettings := make([]*schedulerobjects.ExecutorSettings, len(results))
	for i, result := range results {
		executorSettings[i] = executorSettingsFromDb(result)
	}
	return executorSettings, nil
}

func (r *PostgresExecutorRepository) DeleteExecutor(ctx *armadacontext.Context, executorId string) error {
	queries := New(r.db)
	rowsAffected, err := queries.DeleteExecutor(ctx, executorId)
	if err != nil {
		return errors.WithStack(err)
	}
	if rowsAffected > 0 {
		return nil
	}

	_, err = queries.SelectExecutorById(ctx, executorId)
	if err == nil {
		return errors.WithStack(ErrExecutorNotDrained)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	return errors.WithStack(err)
}

func AcquireExecutorLock(ctx *armadacontext.Context, tx pgx.Tx, executorId string) error {
	return acquireExecutorLock(ctx, tx, executorId)
}

func acquireExecutorLock(ctx *armadacontext.Context, tx pgx.Tx, executorId string) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", executorLockKey(executorId)); err != nil {
		return errors.Wrapf(err, "could not obtain lock for executor %s", executorId)
	}
	return nil
}

func executorLockKey(executorId string) int64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte("executor:"))
	_, _ = hash.Write([]byte(executorId))
	return int64(hash.Sum64())
}

func executorSettingsFromDb(result ExecutorSetting) *schedulerobjects.ExecutorSettings {
	return &schedulerobjects.ExecutorSettings{
		ExecutorId:   result.ExecutorID,
		Cordoned:     result.Cordoned,
		CordonReason: result.CordonReason,
		SetByUser:    result.SetByUser,
		SetAtTime:    protoutil.ToTimestamp(result.SetAtTime),
	}
}

func decompressAndMarshall(b []byte, decompressor compress.Decompressor, msg proto.Message) error {
	decompressed, err := decompressor.Decompress(b)
	if err != nil {
		return err
	}
	return proto.Unmarshal(decompressed, msg)
}
