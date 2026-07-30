package retrypolicy

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	serverqueue "github.com/armadaproject/armada/internal/server/queue"
	"github.com/armadaproject/armada/pkg/api"
	clientqueue "github.com/armadaproject/armada/pkg/client/queue"
)

func withRetryPolicyRepo(t *testing.T, action func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository)) {
	t.Helper()
	withRetryPolicyRepoAndDb(t, func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository, _ *pgxpool.Pool) {
		action(ctx, repo)
	})
}

func withRetryPolicyRepoAndDb(t *testing.T, action func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository, db *pgxpool.Pool)) {
	t.Helper()
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
	t.Cleanup(cancel)
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		action(ctx, NewPostgresRetryPolicyRepository(db), db)
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

// Operations against the wrong state return typed errors. One database serves
// all three, since each case uses a distinct policy name.
func TestPostgresRetryPolicyRepository_TypedErrors(t *testing.T) {
	withRetryPolicyRepo(t, func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository) {
		require.NoError(t, repo.CreateRetryPolicy(ctx, policyFixture("existing")))

		tests := map[string]struct {
			call    func() error
			wantErr any
		}{
			"getting a policy that does not exist": {
				call:    func() error { _, err := repo.GetRetryPolicy(ctx, "absent"); return err },
				wantErr: &ErrRetryPolicyNotFound{},
			},
			"updating a policy that does not exist": {
				call:    func() error { return repo.UpdateRetryPolicy(ctx, policyFixture("absent")) },
				wantErr: &ErrRetryPolicyNotFound{},
			},
		}
		for name, tc := range tests {
			t.Run(name, func(t *testing.T) {
				err := tc.call()
				require.Error(t, err)
				assert.IsType(t, tc.wantErr, err)
			})
		}
	})
}

// After these writes, a read returns exactly the stored policy.
func TestPostgresRetryPolicyRepository_WriteThenRead(t *testing.T) {
	updated := policyFixture("p1")
	updated.RetryLimit = 9

	tests := map[string]struct {
		write func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository) error
		want  *api.RetryPolicy
	}{
		"create round-trips every field": {
			write: func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository) error {
				return repo.CreateRetryPolicy(ctx, policyFixture("p1"))
			},
			want: policyFixture("p1"),
		},
		"creating over an existing name replaces it": {
			write: func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository) error {
				if err := repo.CreateRetryPolicy(ctx, policyFixture("p1")); err != nil {
					return err
				}
				return repo.CreateRetryPolicy(ctx, updated)
			},
			want: updated,
		},
		"update replaces the definition": {
			write: func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository) error {
				if err := repo.CreateRetryPolicy(ctx, policyFixture("p1")); err != nil {
					return err
				}
				return repo.UpdateRetryPolicy(ctx, updated)
			},
			want: updated,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			withRetryPolicyRepo(t, func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository) {
				require.NoError(t, tc.write(ctx, repo))

				got, err := repo.GetRetryPolicy(ctx, "p1")
				require.NoError(t, err)
				assert.Equal(t, tc.want, got)
			})
		})
	}
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
		detached, err := repo.DeleteRetryPolicy(ctx, "p1")
		require.NoError(t, err)
		assert.Empty(t, detached)

		_, err = repo.GetRetryPolicy(ctx, "p1")
		assert.IsType(t, &ErrRetryPolicyNotFound{}, err)

		detached, err = repo.DeleteRetryPolicy(ctx, "p1")
		require.NoError(t, err)
		assert.Empty(t, detached)
	})
}

func TestPostgresRetryPolicyRepository_DeleteDetachesReferencingQueues(t *testing.T) {
	withRetryPolicyRepoAndDb(t, func(ctx *armadacontext.Context, repo *PostgresRetryPolicyRepository, db *pgxpool.Pool) {
		queueRepo := serverqueue.NewPostgresQueueRepository(db)
		require.NoError(t, repo.CreateRetryPolicy(ctx, policyFixture("p1")))
		require.NoError(t, repo.CreateRetryPolicy(ctx, policyFixture("other")))
		require.NoError(t, queueRepo.CreateQueue(ctx, clientqueue.Queue{
			Name: "queue-a", PriorityFactor: 1, RetryPolicies: []string{"p1", "other"},
		}))
		require.NoError(t, queueRepo.CreateQueue(ctx, clientqueue.Queue{
			Name: "queue-b", PriorityFactor: 1, RetryPolicies: []string{"other"},
		}))

		detached, err := repo.DeleteRetryPolicy(ctx, "p1")
		require.NoError(t, err)
		assert.Equal(t, []string{"queue-a"}, detached)

		_, err = repo.GetRetryPolicy(ctx, "p1")
		assert.IsType(t, &ErrRetryPolicyNotFound{}, err)

		queueA, err := queueRepo.GetQueue(ctx, "queue-a")
		require.NoError(t, err)
		assert.Equal(t, []string{"other"}, queueA.RetryPolicies)

		queueB, err := queueRepo.GetQueue(ctx, "queue-b")
		require.NoError(t, err)
		assert.Equal(t, []string{"other"}, queueB.RetryPolicies)
	})
}
