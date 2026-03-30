package retrypolicy

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/api"
)

type ErrRetryPolicyNotFound struct {
	Name string
}

func (err *ErrRetryPolicyNotFound) Error() string {
	return fmt.Sprintf("could not find retry policy %q", err.Name)
}

type ErrRetryPolicyAlreadyExists struct {
	Name string
}

func (err *ErrRetryPolicyAlreadyExists) Error() string {
	return fmt.Sprintf("retry policy %s already exists", err.Name)
}

type RetryPolicyRepository interface {
	GetAllRetryPolicies(ctx *armadacontext.Context) ([]*api.RetryPolicy, error)
	GetRetryPolicy(ctx *armadacontext.Context, name string) (*api.RetryPolicy, error)
	CreateRetryPolicy(ctx *armadacontext.Context, policy *api.RetryPolicy) error
	UpdateRetryPolicy(ctx *armadacontext.Context, policy *api.RetryPolicy) error
	DeleteRetryPolicy(ctx *armadacontext.Context, name string) error
}

type PostgresRetryPolicyRepository struct {
	db *pgxpool.Pool
}

func NewPostgresRetryPolicyRepository(db *pgxpool.Pool) *PostgresRetryPolicyRepository {
	return &PostgresRetryPolicyRepository{db: db}
}

func (r *PostgresRetryPolicyRepository) GetAllRetryPolicies(ctx *armadacontext.Context) ([]*api.RetryPolicy, error) {
	rows, err := r.db.Query(ctx, "SELECT definition FROM retry_policy ORDER BY name")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()

	var policies []*api.RetryPolicy
	for rows.Next() {
		var definitionBytes []byte
		err := rows.Scan(&definitionBytes)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		policy, err := unmarshalRetryPolicy(definitionBytes)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		policies = append(policies, policy)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	return policies, nil
}

func (r *PostgresRetryPolicyRepository) GetRetryPolicy(ctx *armadacontext.Context, name string) (*api.RetryPolicy, error) {
	var definitionBytes []byte
	query := "SELECT definition FROM retry_policy WHERE name = $1"

	err := r.db.QueryRow(ctx, query, name).Scan(&definitionBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, &ErrRetryPolicyNotFound{Name: name}
		}
		return nil, errors.WithStack(err)
	}

	policy, err := unmarshalRetryPolicy(definitionBytes)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return policy, nil
}

func (r *PostgresRetryPolicyRepository) CreateRetryPolicy(ctx *armadacontext.Context, policy *api.RetryPolicy) error {
	data, err := proto.Marshal(policy)
	if err != nil {
		return errors.WithStack(err)
	}

	query := "INSERT INTO retry_policy (name, definition) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING"
	result, err := r.db.Exec(ctx, query, policy.Name, data)
	if err != nil {
		return errors.WithStack(err)
	}
	if result.RowsAffected() == 0 {
		return &ErrRetryPolicyAlreadyExists{Name: policy.Name}
	}
	return nil
}

func (r *PostgresRetryPolicyRepository) UpdateRetryPolicy(ctx *armadacontext.Context, policy *api.RetryPolicy) error {
	data, err := proto.Marshal(policy)
	if err != nil {
		return errors.WithStack(err)
	}

	query := "UPDATE retry_policy SET definition = $2 WHERE name = $1"
	result, err := r.db.Exec(ctx, query, policy.Name, data)
	if err != nil {
		return errors.WithStack(err)
	}
	if result.RowsAffected() == 0 {
		return &ErrRetryPolicyNotFound{Name: policy.Name}
	}
	return nil
}

// DeleteRetryPolicy removes a retry policy by name.
// Deletes are intentionally idempotent - deleting a non-existent policy is a no-op,
// consistent with DeleteQueue in queue_repository.go.
func (r *PostgresRetryPolicyRepository) DeleteRetryPolicy(ctx *armadacontext.Context, name string) error {
	query := "DELETE FROM retry_policy WHERE name = $1"
	_, err := r.db.Exec(ctx, query, name)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func unmarshalRetryPolicy(definitionBytes []byte) (*api.RetryPolicy, error) {
	policy := &api.RetryPolicy{}
	if err := proto.Unmarshal(definitionBytes, policy); err != nil {
		return nil, err
	}
	return policy, nil
}
