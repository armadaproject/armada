package retrypolicy

import (
	"fmt"
	"slices"

	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/server/queryapi/database"
	"github.com/armadaproject/armada/pkg/api"
)

type ErrRetryPolicyNotFound struct {
	Name string
}

func (err *ErrRetryPolicyNotFound) Error() string {
	return fmt.Sprintf("could not find retry policy %q", err.Name)
}

type RetryPolicyRepository interface {
	GetAllRetryPolicies(ctx *armadacontext.Context) ([]*api.RetryPolicy, error)
	GetRetryPolicy(ctx *armadacontext.Context, name string) (*api.RetryPolicy, error)
	CreateRetryPolicy(ctx *armadacontext.Context, policy *api.RetryPolicy) error
	UpdateRetryPolicy(ctx *armadacontext.Context, policy *api.RetryPolicy) error
	DeleteRetryPolicy(ctx *armadacontext.Context, name string) ([]string, error)
}

type PostgresRetryPolicyRepository struct {
	db      *pgxpool.Pool
	queries *database.Queries
}

func NewPostgresRetryPolicyRepository(db *pgxpool.Pool) *PostgresRetryPolicyRepository {
	return &PostgresRetryPolicyRepository{db: db, queries: database.New(db)}
}

func (r *PostgresRetryPolicyRepository) GetAllRetryPolicies(ctx *armadacontext.Context) ([]*api.RetryPolicy, error) {
	definitions, err := r.queries.GetAllRetryPolicies(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	policies := make([]*api.RetryPolicy, 0, len(definitions))
	for _, definitionBytes := range definitions {
		policy, err := unmarshalRetryPolicy(definitionBytes)
		if err != nil {
			return nil, err
		}
		policies = append(policies, policy)
	}
	return policies, nil
}

func (r *PostgresRetryPolicyRepository) GetRetryPolicy(ctx *armadacontext.Context, name string) (*api.RetryPolicy, error) {
	definitionBytes, err := r.queries.GetRetryPolicy(ctx, name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, &ErrRetryPolicyNotFound{Name: name}
		}
		return nil, errors.WithStack(err)
	}

	return unmarshalRetryPolicy(definitionBytes)
}

func (r *PostgresRetryPolicyRepository) CreateRetryPolicy(ctx *armadacontext.Context, policy *api.RetryPolicy) error {
	data, err := proto.Marshal(policy)
	if err != nil {
		return errors.WithStack(err)
	}

	// Writing over an existing name replaces it, matching CreateQueue, so that a
	// declarative pipeline can be re-run.
	err = r.queries.CreateRetryPolicy(ctx, database.CreateRetryPolicyParams{
		Name:       policy.Name,
		Definition: data,
	})
	return errors.WithStack(err)
}

func (r *PostgresRetryPolicyRepository) UpdateRetryPolicy(ctx *armadacontext.Context, policy *api.RetryPolicy) error {
	data, err := proto.Marshal(policy)
	if err != nil {
		return errors.WithStack(err)
	}

	rowsAffected, err := r.queries.UpdateRetryPolicy(ctx, database.UpdateRetryPolicyParams{
		Name:       policy.Name,
		Definition: data,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	if rowsAffected == 0 {
		return &ErrRetryPolicyNotFound{Name: policy.Name}
	}
	return nil
}

// DeleteRetryPolicy removes a policy and returns the queues it was detached
// from. Deleting a non-existent policy is a no-op, consistent with DeleteQueue.
//
// Attachments are deleted explicitly rather than by ON DELETE CASCADE, so
// RETURNING names the queues this transaction detached, and both this path and
// queue attachment touch queue_retry_policy before retry_policy.
func (r *PostgresRetryPolicyRepository) DeleteRetryPolicy(ctx *armadacontext.Context, name string) ([]string, error) {
	var detached []string
	err := pgx.BeginTxFunc(ctx, r.db, pgx.TxOptions{}, func(tx pgx.Tx) error {
		queries := r.queries.WithTx(tx)

		var err error
		if detached, err = queries.DeleteRetryPolicyAttachments(ctx, name); err != nil {
			return errors.WithStack(err)
		}
		return errors.WithStack(queries.DeleteRetryPolicy(ctx, name))
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(detached) // DELETE ... RETURNING cannot ORDER BY
	return detached, nil
}

func unmarshalRetryPolicy(definitionBytes []byte) (*api.RetryPolicy, error) {
	policy := &api.RetryPolicy{}
	if err := proto.Unmarshal(definitionBytes, policy); err != nil {
		return nil, errors.WithStack(err)
	}
	return policy, nil
}
