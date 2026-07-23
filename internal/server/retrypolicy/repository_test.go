package retrypolicy

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/pkg/api"
)

func withRetryPolicyRepo(t *testing.T, action func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository)) {
	t.Helper()
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
	defer cancel()
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		action(ctx, NewPostgresRetryPolicyRepository(db))
		return nil
	})
	require.NoError(t, err)
}

func policyFixture(name string) *api.RetryPolicy {
	return &api.RetryPolicy{
		Name:          name,
		RetryLimit:    3,
		DefaultAction: api.RetryAction_RETRY_ACTION_FAIL,
		Rules: []*api.RetryRule{
			{Action: api.RetryAction_RETRY_ACTION_RETRY, OnCategory: "gpu", OnSubcategory: "transient"},
		},
	}
}

func TestPostgresRetryPolicyRepository_CreateThenGetRoundTrips(t *testing.T) {
	withRetryPolicyRepo(t, func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository) {
		policy := policyFixture("p1")
		require.NoError(t, repo.CreateRetryPolicy(ctx, policy))

		got, err := repo.GetRetryPolicy(ctx, "p1")
		require.NoError(t, err)
		assert.Equal(t, "p1", got.Name)
		assert.Equal(t, uint32(3), got.RetryLimit)
		assert.Equal(t, api.RetryAction_RETRY_ACTION_FAIL, got.DefaultAction)
		require.Len(t, got.Rules, 1)
		assert.Equal(t, "gpu", got.Rules[0].OnCategory)
		assert.Equal(t, "transient", got.Rules[0].OnSubcategory)
	})
}

func TestPostgresRetryPolicyRepository_CreateDuplicateReturnsAlreadyExists(t *testing.T) {
	withRetryPolicyRepo(t, func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository) {
		require.NoError(t, repo.CreateRetryPolicy(ctx, policyFixture("dup")))
		err := repo.CreateRetryPolicy(ctx, policyFixture("dup"))
		require.Error(t, err)
		assert.IsType(t, &ErrRetryPolicyAlreadyExists{}, err)
	})
}

func TestPostgresRetryPolicyRepository_GetMissingReturnsNotFound(t *testing.T) {
	withRetryPolicyRepo(t, func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository) {
		_, err := repo.GetRetryPolicy(ctx, "absent")
		require.Error(t, err)
		assert.IsType(t, &ErrRetryPolicyNotFound{}, err)
	})
}

func TestPostgresRetryPolicyRepository_UpdateReplacesDefinition(t *testing.T) {
	withRetryPolicyRepo(t, func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository) {
		require.NoError(t, repo.CreateRetryPolicy(ctx, policyFixture("p1")))
		updated := policyFixture("p1")
		updated.RetryLimit = 9
		require.NoError(t, repo.UpdateRetryPolicy(ctx, updated))

		got, err := repo.GetRetryPolicy(ctx, "p1")
		require.NoError(t, err)
		assert.Equal(t, uint32(9), got.RetryLimit)
	})
}

func TestPostgresRetryPolicyRepository_UpdateMissingReturnsNotFound(t *testing.T) {
	withRetryPolicyRepo(t, func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository) {
		err := repo.UpdateRetryPolicy(ctx, policyFixture("absent"))
		require.Error(t, err)
		assert.IsType(t, &ErrRetryPolicyNotFound{}, err)
	})
}

func TestPostgresRetryPolicyRepository_GetAllIsOrderedByNameAndEmptyWhenNone(t *testing.T) {
	withRetryPolicyRepo(t, func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository) {
		all, err := repo.GetAllRetryPolicies(ctx)
		require.NoError(t, err)
		assert.Empty(t, all)

		require.NoError(t, repo.CreateRetryPolicy(ctx, policyFixture("b")))
		require.NoError(t, repo.CreateRetryPolicy(ctx, policyFixture("a")))

		all, err = repo.GetAllRetryPolicies(ctx)
		require.NoError(t, err)
		require.Len(t, all, 2)
		assert.Equal(t, "a", all[0].Name)
		assert.Equal(t, "b", all[1].Name)
	})
}

func TestPostgresRetryPolicyRepository_DeleteIsIdempotent(t *testing.T) {
	withRetryPolicyRepo(t, func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository) {
		require.NoError(t, repo.CreateRetryPolicy(ctx, policyFixture("p1")))
		require.NoError(t, repo.DeleteRetryPolicy(ctx, "p1"))

		_, err := repo.GetRetryPolicy(ctx, "p1")
		assert.IsType(t, &ErrRetryPolicyNotFound{}, err)

		// Deleting an already-absent policy is a no-op, not an error.
		require.NoError(t, repo.DeleteRetryPolicy(ctx, "p1"))
	})
}
