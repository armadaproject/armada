package database

import (
	"context"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"time"

	"github.com/armadaproject/armada/pkg/executorapi"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// Executor is a representation of an Armada Executor
type Executor struct {
	// Name of the executor
	Name string
	// Pool that the executor belongs to
	Pool string
	// The nodes available for scheduling via this executor
	Nodes []*schedulerobjects.Node
	// Minimum resources which a job must request in order to be considered for scheduling on this executor
	MinimumJobSize map[string]resource.Quantity
	// Last time the executor provided a heartbeat to say it was still accepting jobs
	LastUpdateTime time.Time
}

// ExecutorRepository is an interface to be implemented by structs which provide executor information
type ExecutorRepository interface {
	// GetExecutors returns all known executors, regardless of their last heartbeat time
	GetExecutors(ctx context.Context) ([]*Executor, error)
	// GetLastUpdateTimes returns a map of executor name -> last heartbeat time
	GetLastUpdateTimes(ctx context.Context) (map[string]time.Time, error)
	// StoreRequest persists the last lease request made by an executor
	StoreRequest(ctx context.Context, req *executorapi.LeaseRequest) error
}

// PostgresExecutorRepository is an implementation of ExecutorRepository that stores its state in postgres
type PostgresExecutorRepository struct {
	// pool of database connections
	db *pgxpool.Pool
	// proto objects are stored compressed
	compressor   compress.Compressor
	decompressor compress.Decompressor
}

func NewPostgresExecutorRepository(db *pgxpool.Pool, compressor compress.Compressor, decompressor compress.Decompressor) *PostgresExecutorRepository {
	return &PostgresExecutorRepository{
		db:           db,
		compressor:   compressor,
		decompressor: decompressor,
	}
}

func (r *PostgresExecutorRepository) GetExecutors(ctx context.Context) ([]*Executor, error) {
	queries := New(r.db)
	requests, err := queries.SelectAllExecutors(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	executors := make([]*Executor, len(requests))
	for i, request := range requests {
		req := &executorapi.LeaseRequest{}
		err := decompressAndMarshall(request.LastRequest, r.decompressor, req)
		if err != nil{
			return nil, err
		}
		executors[i] = &Executor{
			Name:           request.ExecutorID,
			Pool:           req.Pool,
			Nodes:          nil,
			MinimumJobSize: nil,
			LastUpdateTime: request.,
		}
	}
	return executors, nil
}

func (r *PostgresExecutorRepository) GetLastUpdateTimes(ctx context.Context) (map[string]time.Time, error) {
	return nil, nil
}

func (r *PostgresExecutorRepository) StoreRequest(ctx context.Context, req *executorapi.LeaseRequest) error {
	return nil
}

func decompressAndMarshall(b []byte, decompressor compress.Decompressor, msg proto.Message) error {
	decompressed, err := decompressor.Decompress(b)
	if err != nil {
		return err
	}
	return proto.Unmarshal(decompressed, msg)
}

func createNodes() []*schedulerobjects.Node
