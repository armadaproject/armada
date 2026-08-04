package queue

import (
	"fmt"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/server/queryapi/database"
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

// ErrUnknownRetryPolicies is returned when a queue write references retry
// policies that do not exist. Names may be empty when the missing policy could
// not be identified.
type ErrUnknownRetryPolicies struct {
	Names []string
}

func (err *ErrUnknownRetryPolicies) Error() string {
	if len(err.Names) == 0 {
		return "one or more referenced retry policies do not exist"
	}
	// Quoted so that a name with stray whitespace is visible in the message.
	quoted := make([]string, len(err.Names))
	for i, name := range err.Names {
		quoted[i] = fmt.Sprintf("%q", name)
	}
	return fmt.Sprintf("retry policies do not exist: %s", strings.Join(quoted, ", "))
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
	db      *pgxpool.Pool
	queries *database.Queries
}

func NewPostgresQueueRepository(db *pgxpool.Pool) *PostgresQueueRepository {
	return &PostgresQueueRepository{db: db, queries: database.New(db)}
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

	attachments, err := r.queries.GetAllQueueRetryPolicies(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	policiesByQueue := map[string][]string{}
	for _, row := range attachments {
		policiesByQueue[row.QueueName] = append(policiesByQueue[row.QueueName], row.PolicyName)
	}
	for i := range queues {
		queues[i].RetryPolicies = policiesByQueue[queues[i].Name]
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

	// Ordered by ordinal, so the list reads back as it was submitted, which is
	// the order the scheduler evaluates the policies in.
	q.RetryPolicies, err = r.queries.GetQueueRetryPolicies(ctx, name)
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

const (
	// foreignKeyViolation is the PostgreSQL SQLSTATE raised when a write breaks a
	// foreign key constraint.
	foreignKeyViolation = "23503"

	// retryPolicyForeignKey is named explicitly in the migration so that this
	// match cannot silently stop working.
	retryPolicyForeignKey = "queue_retry_policy_policy_name_fkey"
)

// isUnknownRetryPolicyViolation reports whether err is the foreign key
// violation raised when a queue write references a policy that no longer
// exists, which happens if it is deleted after the names are checked.
func isUnknownRetryPolicyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) &&
		pgErr.Code == foreignKeyViolation &&
		pgErr.ConstraintName == retryPolicyForeignKey
}

func (r *PostgresQueueRepository) upsertQueue(ctx *armadacontext.Context, queue queue.Queue) error {
	apiQueue := queue.ToAPI()
	retryPolicies := apiQueue.RetryPolicies
	// The queue_retry_policy table is the source of truth for retry policy
	// attachments, so they are not persisted in the serialized definition.
	apiQueue.RetryPolicies = nil
	data, err := proto.Marshal(apiQueue)
	if err != nil {
		return errors.WithStack(err)
	}

	err = pgx.BeginTxFunc(ctx, r.db, pgx.TxOptions{}, func(tx pgx.Tx) error {
		query := "INSERT INTO queue (name, definition) VALUES ($1, $2) ON CONFLICT(name) DO UPDATE SET definition = EXCLUDED.definition"
		_, err := tx.Exec(ctx, query, queue.Name, data)
		if err != nil {
			return errors.WithStack(err)
		}
		return r.syncRetryPolicies(ctx, tx, queue.Name, retryPolicies)
	})

	if isUnknownRetryPolicyViolation(err) {
		return &ErrUnknownRetryPolicies{}
	}
	return err
}

// syncRetryPolicies replaces the queue's rows in queue_retry_policy with the
// given list, ordinal preserving the submitted order. The foreign key on
// policy_name remains the authoritative guard that every policy exists.
func (r *PostgresQueueRepository) syncRetryPolicies(ctx *armadacontext.Context, tx pgx.Tx, queueName string, policies []string) error {
	queries := r.queries.WithTx(tx)

	// Checked before the delete below, so a write naming an unknown policy fails
	// without having removed anything.
	if len(policies) > 0 {
		missing, err := missingPolicyNames(ctx, queries, policies)
		if err != nil {
			return err
		}
		if len(missing) > 0 {
			return &ErrUnknownRetryPolicies{Names: missing}
		}
	}

	if err := queries.DeleteQueueRetryPolicies(ctx, queueName); err != nil {
		return errors.WithStack(err)
	}

	for ordinal, name := range policies {
		err := queries.InsertQueueRetryPolicy(ctx, database.InsertQueueRetryPolicyParams{
			QueueName:  queueName,
			PolicyName: name,
			Ordinal:    int32(ordinal),
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// missingPolicyNames returns the subset of the given names with no retry policy.
func missingPolicyNames(ctx *armadacontext.Context, queries *database.Queries, names []string) ([]string, error) {
	found, err := queries.GetExistingRetryPolicyNames(ctx, names)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	existing := make(map[string]bool, len(found))
	for _, name := range found {
		existing[name] = true
	}

	var missing []string
	for _, name := range names {
		if !existing[name] {
			missing = append(missing, name)
		}
	}
	return missing, nil
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
