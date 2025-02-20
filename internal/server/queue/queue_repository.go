package queue

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

type ErrQueueNotFound struct {
	QueueName string
}

func (err *ErrQueueNotFound) Error() string {
	return fmt.Sprintf("could not find queue %q", err.QueueName)
}

type ErrQueueAlreadyExists struct {
	QueueName string
}

func (err *ErrQueueAlreadyExists) Error() string {
	return fmt.Sprintf("queue %s already exists", err.QueueName)
}

type QueueRepository interface {
	GetAllQueues(ctx *armadacontext.Context) ([]queue.Queue, error)
	GetQueue(ctx *armadacontext.Context, name string) (queue.Queue, error)
	CreateQueue(*armadacontext.Context, queue.Queue) error
	UpdateQueue(*armadacontext.Context, queue.Queue) error
	DeleteQueue(ctx *armadacontext.Context, name string) error
	CordonQueue(ctx *armadacontext.Context, name string) error
	UncordonQueue(ctx *armadacontext.Context, name string) error
}

type ReadOnlyQueueRepository interface {
	GetAllQueues(ctx *armadacontext.Context) ([]queue.Queue, error)
	GetQueue(ctx *armadacontext.Context, name string) (queue.Queue, error)
}

type PostgresQueueRepository struct {
	// pool of database connections
	db *pgxpool.Pool
}

func NewPostgresQueueRepository(db *pgxpool.Pool) *PostgresQueueRepository {
	return &PostgresQueueRepository{db: db}
}

func (r *PostgresQueueRepository) GetAllQueues(ctx *armadacontext.Context) ([]queue.Queue, error) {
	rows, err := r.db.Query(ctx, "SELECT definition FROM queue ORDER BY name")
	if err != nil {
		return nil, errors.WithStack(err)
	}

	defer rows.Close()

	queues := make([]queue.Queue, 0)
	for rows.Next() {
		var definitionBytes []byte
		err := rows.Scan(&definitionBytes)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		q, err := r.unmarshalQueue(definitionBytes)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		queues = append(queues, q)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	return queues, nil
}

func (r *PostgresQueueRepository) GetQueue(ctx *armadacontext.Context, name string) (queue.Queue, error) {
	var definitionBytes []byte
	query := "SELECT definition FROM queue WHERE name = $1"

	err := r.db.QueryRow(ctx, query, name).Scan(&definitionBytes)
	if err != nil {
		q := queue.Queue{}
		if errors.Is(err, pgx.ErrNoRows) {
			return q, &ErrQueueNotFound{QueueName: name}
		}
		return q, errors.WithStack(err)
	}

	q, err := r.unmarshalQueue(definitionBytes)
	if err != nil {
		return queue.Queue{}, errors.WithStack(err)
	}
	return q, nil
}

func (r *PostgresQueueRepository) CreateQueue(ctx *armadacontext.Context, queue queue.Queue) error {
	return r.upsertQueue(ctx, queue)
}

func (r *PostgresQueueRepository) UpdateQueue(ctx *armadacontext.Context, queue queue.Queue) error {
	return r.upsertQueue(ctx, queue)
}

func (r *PostgresQueueRepository) DeleteQueue(ctx *armadacontext.Context, name string) error {
	query := "DELETE FROM queue WHERE name = $1"
	_, err := r.db.Exec(ctx, query, name)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r *PostgresQueueRepository) CordonQueue(ctx *armadacontext.Context, name string) error {
	queueToCordon, err := r.GetQueue(ctx, name)
	if err != nil {
		return err
	}
	queueToCordon.Cordoned = true
	return r.upsertQueue(ctx, queueToCordon)
}

func (r *PostgresQueueRepository) UncordonQueue(ctx *armadacontext.Context, name string) error {
	queueToUncordon, err := r.GetQueue(ctx, name)
	if err != nil {
		return err
	}
	queueToUncordon.Cordoned = false
	return r.upsertQueue(ctx, queueToUncordon)
}

func (r *PostgresQueueRepository) upsertQueue(ctx *armadacontext.Context, queue queue.Queue) error {
	data, err := proto.Marshal(queue.ToAPI())
	if err != nil {
		return errors.WithStack(err)
	}

	query := "INSERT INTO queue (name, definition) VALUES ($1, $2) ON CONFLICT(name) DO UPDATE SET definition = EXCLUDED.definition"
	_, err = r.db.Exec(ctx, query, queue.Name, data)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r *PostgresQueueRepository) unmarshalQueue(definitionBytes []byte) (queue.Queue, error) {
	apiQueue := &api.Queue{}
	if err := proto.Unmarshal(definitionBytes, apiQueue); err != nil {
		return queue.Queue{}, err
	}
	q, err := queue.NewQueue(apiQueue)
	if err != nil {
		return queue.Queue{}, err
	}
	return q, nil
}
