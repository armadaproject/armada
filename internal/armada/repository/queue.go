package repository

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

const queueHashKey = "Queue"

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
}

type RedisQueueRepository struct {
	db redis.UniversalClient
}

func NewRedisQueueRepository(db redis.UniversalClient) *RedisQueueRepository {
	return &RedisQueueRepository{db: db}
}

func (r *RedisQueueRepository) GetAllQueues(ctx *armadacontext.Context) ([]queue.Queue, error) {
	result, err := r.db.HGetAll(ctx, queueHashKey).Result()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	queues := make([]queue.Queue, len(result))
	i := 0
	for _, v := range result {
		apiQueue := &api.Queue{}
		if err := proto.Unmarshal([]byte(v), apiQueue); err != nil {
			return nil, errors.WithStack(err)
		}
		queue, err := queue.NewQueue(apiQueue)
		if err != nil {
			return nil, err
		}
		queues[i] = queue
		i++
	}
	return queues, nil
}

func (r *RedisQueueRepository) GetQueue(ctx *armadacontext.Context, name string) (queue.Queue, error) {
	result, err := r.db.HGet(ctx, queueHashKey, name).Result()
	if err == redis.Nil {
		return queue.Queue{}, &ErrQueueNotFound{QueueName: name}
	} else if err != nil {
		return queue.Queue{}, fmt.Errorf("[RedisQueueRepository.GetQueue] error reading from database: %s", err)
	}

	apiQueue := &api.Queue{}
	e := proto.Unmarshal([]byte(result), apiQueue)
	if e != nil {
		return queue.Queue{}, fmt.Errorf("[RedisQueueRepository.GetQueue] error unmarshalling queue: %s", err)
	}

	return queue.NewQueue(apiQueue)
}

func (r *RedisQueueRepository) CreateQueue(ctx *armadacontext.Context, queue queue.Queue) error {
	data, err := proto.Marshal(queue.ToAPI())
	if err != nil {
		return fmt.Errorf("[RedisQueueRepository.CreateQueue] error marshalling queue: %s", err)
	}

	// HSetNX sets a key-value pair if the key doesn't already exist.
	// If the key exists, this is a no-op, and result is false.
	result, err := r.db.HSetNX(ctx, queueHashKey, queue.Name, data).Result()
	if err != nil {
		return fmt.Errorf("[RedisQueueRepository.CreateQueue] error writing to database: %s", err)
	}
	if !result {
		return &ErrQueueAlreadyExists{QueueName: queue.Name}
	}

	return nil
}

// TODO If the queue to be updated is deleted between this method checking if the queue exists and
// making the update, the deleted queue is re-added to Redis. There's no "update if exists"
// operation in Redis, so we need to do this with a script or transaction.
func (r *RedisQueueRepository) UpdateQueue(ctx *armadacontext.Context, queue queue.Queue) error {
	existsResult, err := r.db.HExists(ctx, queueHashKey, queue.Name).Result()
	if err != nil {
		return fmt.Errorf("[RedisQueueRepository.UpdateQueue] error reading from database: %s", err)
	} else if !existsResult {
		return &ErrQueueNotFound{QueueName: queue.Name}
	}

	data, err := proto.Marshal(queue.ToAPI())
	if err != nil {
		return fmt.Errorf("[RedisQueueRepository.UpdateQueue] error marshalling queue: %s", err)
	}

	result := r.db.HSet(ctx, queueHashKey, queue.Name, data)
	if err := result.Err(); err != nil {
		return fmt.Errorf("[RedisQueueRepository.UpdateQueue] error writing to database: %s", err)
	}

	return nil
}

func (r *RedisQueueRepository) DeleteQueue(ctx *armadacontext.Context, name string) error {
	result := r.db.HDel(ctx, queueHashKey, name)
	if err := result.Err(); err != nil {
		return fmt.Errorf("[RedisQueueRepository.DeleteQueue] error deleting queue: %s", err)
	}
	return nil
}

type PostgresQueueRepository struct {
	// pool of database connections
	db *pgxpool.Pool
}

func NewPostgresQueueRepository(db *pgxpool.Pool) *PostgresQueueRepository {
	return &PostgresQueueRepository{db: db}
}

func (r *PostgresQueueRepository) GetAllQueues(ctx *armadacontext.Context) ([]queue.Queue, error) {
	rows, err := r.db.Query(ctx, "SELECT definition FROM queue")
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
	data, err := proto.Marshal(queue.ToAPI())
	if err != nil {
		return errors.WithStack(err)
	}

	query := "INSERT INTO queue (name, definition) VALUES ($1, $2)"
	_, err = r.db.Exec(ctx, query, queue.Name, data)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == pgerrcode.UniqueViolation {
				return &ErrQueueAlreadyExists{QueueName: queue.Name}
			}
		}
		return errors.WithStack(err)
	}
	return nil
}

func (r *PostgresQueueRepository) UpdateQueue(ctx *armadacontext.Context, queue queue.Queue) error {
	data, err := proto.Marshal(queue.ToAPI())
	if err != nil {
		return errors.WithStack(err)
	}

	query := "UPDATE queue SET definition = $2 WHERE name = $1"
	cmdTag, err := r.db.Exec(ctx, query, queue.Name, data)
	if err != nil {
		return errors.WithStack(err)
	}

	if cmdTag.RowsAffected() == 0 {
		return &ErrQueueNotFound{QueueName: queue.Name}
	}

	return nil
}

func (r *PostgresQueueRepository) DeleteQueue(ctx *armadacontext.Context, name string) error {
	query := "DELETE FROM queue WHERE name = $1"
	_, err := r.db.Exec(ctx, query, name)
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

type DualQueueRepository struct {
	primaryRepo   QueueRepository
	secondaryRepo QueueRepository
}

func NewDualQueueRepository(redis redis.UniversalClient, postgres *pgxpool.Pool, usePostgresForPrimary bool) *DualQueueRepository {
	redisRepo := NewRedisQueueRepository(redis)
	postgresRepo := NewPostgresQueueRepository(postgres)
	if usePostgresForPrimary {
		return &DualQueueRepository{
			primaryRepo:   postgresRepo,
			secondaryRepo: redisRepo,
		}
	} else {
		return &DualQueueRepository{
			primaryRepo:   redisRepo,
			secondaryRepo: postgresRepo,
		}
	}
}

func (r *DualQueueRepository) GetAllQueues(ctx *armadacontext.Context) ([]queue.Queue, error) {
	return r.primaryRepo.GetAllQueues(ctx)
}

func (r *DualQueueRepository) GetQueue(ctx *armadacontext.Context, name string) (queue.Queue, error) {
	return r.primaryRepo.GetQueue(ctx, name)
}

func (r *DualQueueRepository) CreateQueue(ctx *armadacontext.Context, queue queue.Queue) error {
	err := r.primaryRepo.CreateQueue(ctx, queue)
	if err != nil {
		return err
	}
	err = r.secondaryRepo.CreateQueue(ctx, queue)
	if err != nil {
		ctx.Warnf("Could not create queue %s on secondaryd repo", queue.Name)
	}
	return nil
}

func (r *DualQueueRepository) UpdateQueue(ctx *armadacontext.Context, queue queue.Queue) error {
	err := r.primaryRepo.UpdateQueue(ctx, queue)
	if err != nil {
		return err
	}
	err = r.secondaryRepo.UpdateQueue(ctx, queue)
	if err != nil {
		ctx.Warnf("Could not update queue %s on secondary repo", queue.Name)
	}
	return nil
}

func (r *DualQueueRepository) DeleteQueue(ctx *armadacontext.Context, name string) error {
	err := r.primaryRepo.DeleteQueue(ctx, name)
	if err != nil {
		return err
	}
	err = r.secondaryRepo.DeleteQueue(ctx, name)
	if err != nil {
		ctx.Warnf("Could not delete queue %s on secondary repo", name)
	}
	return nil
}
