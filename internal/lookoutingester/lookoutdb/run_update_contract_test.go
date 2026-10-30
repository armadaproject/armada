package lookoutdb

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/lookoutingester/model"
)

type runUpdatePath struct {
	name   string
	update func(*LookoutDb, *armadacontext.Context, []*model.UpdateJobRunInstruction) error
}

func runUpdatePaths() []runUpdatePath {
	return []runUpdatePath{
		{"batch", (*LookoutDb).UpdateJobRunsBatch},
		{"scalar", (*LookoutDb).UpdateJobRunsScalar},
		{"fallback", (*LookoutDb).UpdateJobRuns},
		{"store", func(l *LookoutDb, ctx *armadacontext.Context, updates []*model.UpdateJobRunInstruction) error {
			// One instruction per run in each call keeps conflation outside this contract.
			return l.Store(ctx, &model.InstructionSet{JobRunsToUpdate: updates})
		}},
	}
}

// This reader preserves SQL NULL versus empty bytea/jsonb and includes the
// identifying/creation fields that run updates must leave untouched.
type persistedRunContract struct {
	RunId       string
	JobId       string
	Cluster     string
	Pool        *string
	Leased      *time.Time
	Node        *string
	PodName     *string
	Pending     *time.Time
	Started     *time.Time
	Finished    *time.Time
	State       int32
	Error       []byte
	Debug       []byte
	ExitCode    *int32
	Ingress     map[int32]string
	Category    *string
	Subcategory *string
	Termination map[string]any
}

func readRunContract(t *testing.T, db *pgxpool.Pool, id string) persistedRunContract {
	t.Helper()
	var r persistedRunContract
	require.NoError(t, db.QueryRow(armadacontext.Background(), `SELECT
 run_id, job_id, cluster, pool, leased, node, pod_name, pending, started, finished,
 job_run_state, error, debug, exit_code, ingress_addresses, failure_category,
 failure_subcategory, scheduler_termination_reason FROM job_run WHERE run_id = $1`, id).Scan(
		&r.RunId, &r.JobId, &r.Cluster, &r.Pool, &r.Leased, &r.Node, &r.PodName,
		&r.Pending, &r.Started, &r.Finished, &r.State, &r.Error, &r.Debug, &r.ExitCode,
		&r.Ingress, &r.Category, &r.Subcategory, &r.Termination))
	return r
}

func withRunContractDb(t *testing.T, test func(*testing.T, *pgxpool.Pool, *LookoutDb)) {
	t.Helper()
	schema := 0
	require.NoError(t, lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		name := []string{"ordinary", "hot-cold"}[schema]
		schema++
		t.Run(name, func(t *testing.T) {
			test(t, db, NewLookoutDb(db, fatalErrors, m, 1, 0))
		})
		return nil
	}))
}

func seedRunContracts(t *testing.T, l *LookoutDb, count int) []persistedRunContract {
	t.Helper()
	ctx := armadacontext.Background()
	require.NoError(t, l.CreateJobsBatch(ctx, []*model.CreateJobInstruction{makeCreateJobInstruction(JobId)}))
	result := make([]persistedRunContract, count)
	for i := range result {
		id := fmt.Sprintf("123e4567-e89b-12d3-a456-%012d", i)
		require.NoError(t, l.CreateJobRunsBatch(ctx, []*model.CreateJobRunInstruction{{
			RunId: id, JobId: JobId, Cluster: "creation-cluster", Pool: "creation-pool",
			Leased: &baseTime, JobRunState: lookout.JobRunLeasedOrdinal,
		}}))
		result[i] = persistedRunContract{
			RunId: id, JobId: JobId, Cluster: "creation-cluster", Pool: pointer.String("creation-pool"),
			Leased: &baseTime, State: lookout.JobRunLeasedOrdinal,
		}
		require.Equal(t, result[i], readRunContract(t, l.db, id))
	}
	return result
}

func TestRunUpdatePersistenceContract(t *testing.T) {
	for _, path := range runUpdatePaths() {
		t.Run(path.name, func(t *testing.T) {
			withRunContractDb(t, func(t *testing.T, db *pgxpool.Pool, l *LookoutDb) {
				ctx := armadacontext.Background()
				expected := seedRunContracts(t, l, 1)[0]
				id := expected.RunId
				apply := func(update *model.UpdateJobRunInstruction) {
					t.Helper()
					require.NoError(t, path.update(l, ctx, []*model.UpdateJobRunInstruction{update}))
				}
				// Omitted values preserve existing SQL NULLs, including a NULL Pending.
				apply(&model.UpdateJobRunInstruction{RunId: id})
				require.Equal(t, expected, readRunContract(t, db, id))
				var nullObjects bool
				require.NoError(t, db.QueryRow(ctx, `SELECT ingress_addresses IS NULL AND
                    scheduler_termination_reason IS NULL FROM job_run WHERE run_id = $1`, id).Scan(&nullObjects))
				require.True(t, nullObjects, "initial JSON columns are SQL NULL, not JSON null or empty objects")

				pending := baseTime.Add(time.Second)
				started := baseTime.Add(2 * time.Second)
				finished := baseTime.Add(3 * time.Second)
				apply(&model.UpdateJobRunInstruction{
					RunId: id, Node: pointer.String("updated-node"), PodName: pointer.String("updated-pod"),
					Pending: &pending, Started: &started, Finished: &finished,
					JobRunState: pointer.Int32(lookout.JobRunFailedOrdinal),
					Error:       []byte{1, 2, 3}, Debug: []byte{4, 5}, ExitCode: pointer.Int32(27),
					IngressAddresses: map[int32]string{80: "first.example", 443: "second.example"},
					FailureCategory:  pointer.String("category"), FailureSubcategory: pointer.String("subcategory"),
					SchedulerTerminationReason: map[string]any{"message": "scheduler reason"},
				})
				// Expected values are specified independently of the instruction mapping.
				expected.Node = pointer.String("updated-node")
				expected.PodName = pointer.String("updated-pod")
				expected.Pending = &pending
				expected.Started = &started
				expected.Finished = &finished
				expected.State = lookout.JobRunFailedOrdinal
				expected.Error = []byte{1, 2, 3}
				expected.Debug = []byte{4, 5}
				expected.ExitCode = pointer.Int32(27)
				expected.Ingress = map[int32]string{80: "first.example", 443: "second.example"}
				expected.Category = pointer.String("category")
				expected.Subcategory = pointer.String("subcategory")
				expected.Termination = map[string]any{"message": "scheduler reason"}
				require.Equal(t, expected, readRunContract(t, db, id))

				apply(&model.UpdateJobRunInstruction{RunId: id})
				require.Equal(t, expected, readRunContract(t, db, id), "nil retains populated columns")

				// Current SQL accepts older incoming timestamps; it does not take a maximum.
				olderPending := baseTime.Add(-3 * time.Second)
				olderStarted := baseTime.Add(-2 * time.Second)
				olderFinished := baseTime.Add(-time.Second)
				sparse := &model.UpdateJobRunInstruction{
					RunId: id, Pending: &olderPending, Started: &olderStarted, Finished: &olderFinished,
					Node: pointer.String("later-node"),
				}
				expected.Pending = &olderPending
				expected.Started = &olderStarted
				expected.Finished = &olderFinished
				expected.Node = pointer.String("later-node")
				for i := 0; i < 2; i++ {
					apply(sparse)
					require.Equal(t, expected, readRunContract(t, db, id), "sequential overwrite and exact replay")
				}

				zeroTime := time.Time{}
				empty := &model.UpdateJobRunInstruction{
					RunId: id, Node: pointer.String(""), PodName: pointer.String(""),
					Pending: &zeroTime, Started: &zeroTime, Finished: &zeroTime,
					JobRunState: pointer.Int32(0), Error: []byte{}, Debug: []byte{}, ExitCode: pointer.Int32(0),
					IngressAddresses: map[int32]string{}, FailureCategory: pointer.String(""), FailureSubcategory: pointer.String(""),
					SchedulerTerminationReason: map[string]any{},
				}
				expected.Node = pointer.String("")
				expected.PodName = pointer.String("")
				expected.Pending = &zeroTime
				expected.Started = &zeroTime
				expected.Finished = &zeroTime
				expected.State = 0
				expected.Error = []byte{}
				expected.Debug = []byte{}
				expected.ExitCode = pointer.Int32(0)
				expected.Ingress = map[int32]string{}
				expected.Category = pointer.String("")
				expected.Subcategory = pointer.String("")
				expected.Termination = map[string]any{}
				apply(empty)
				require.Equal(t, expected, readRunContract(t, db, id), "non-nil empty and zero values replace stored values")
				var emptyBytes, emptyObjects bool
				require.NoError(t, db.QueryRow(ctx, `SELECT error = ''::bytea AND debug = ''::bytea,
     ingress_addresses = '{}'::jsonb AND scheduler_termination_reason = '{}'::jsonb
     FROM job_run WHERE run_id = $1`, id).Scan(&emptyBytes, &emptyObjects))
				require.True(t, emptyBytes)
				require.True(t, emptyObjects)
				apply(&model.UpdateJobRunInstruction{RunId: id})
				require.Equal(t, expected, readRunContract(t, db, id))

				require.NoError(t, path.update(l, ctx, nil))
				require.NoError(t, path.update(l, ctx, []*model.UpdateJobRunInstruction{}))
				apply(&model.UpdateJobRunInstruction{RunId: "123e4567-e89b-12d3-a456-999999999999", Node: pointer.String("missing")})
				require.Equal(t, expected, readRunContract(t, db, id))
				var count int
				require.NoError(t, db.QueryRow(ctx, "SELECT count(*) FROM job_run").Scan(&count))
				require.Equal(t, 1, count, "unmatched updates never create a run")
			})
		})
	}
}

func TestRunUpdateFailureContracts(t *testing.T) {
	for _, path := range runUpdatePaths() {
		t.Run(path.name, func(t *testing.T) {
			withRunContractDb(t, func(t *testing.T, db *pgxpool.Pool, l *LookoutDb) {
				ctx := armadacontext.Background()
				expected := seedRunContracts(t, l, 3)
				for i := range expected {
					oldNode := fmt.Sprintf("old-node-%d", i)
					_, err := db.Exec(ctx, "UPDATE job_run SET node = $1 WHERE run_id = $2", oldNode, expected[i].RunId)
					require.NoError(t, err)
					expected[i].Node = &oldNode
				}
				invalidNode := strings.Repeat("x", 513)
				// Establish a real assignment violation on an existing row, not an
				// unmatched identifier. SQLSTATE 22001 is configured as non-retryable.
				_, err := db.Exec(ctx, "UPDATE job_run SET node = $1 WHERE run_id = $2", invalidNode, expected[1].RunId)
				var pgErr *pgconn.PgError
				require.ErrorAs(t, err, &pgErr)
				require.Equal(t, "22001", pgErr.Code)
				require.Equal(t, expected[1], readRunContract(t, db, expected[1].RunId))

				updates := []*model.UpdateJobRunInstruction{
					{RunId: expected[0].RunId, Node: pointer.String("first-good")},
					{RunId: expected[1].RunId, Node: &invalidNode, PodName: pointer.String("must-not-persist")},
					{RunId: expected[2].RunId, Node: pointer.String("last-good")},
				}
				err = path.update(l, ctx, updates)
				if path.name == "batch" {
					require.Error(t, err)
				} else {
					require.NoError(t, err, "ordinary row errors are discarded, including after batch fallback")
					expected[0].Node = pointer.String("first-good")
					expected[2].Node = pointer.String("last-good")
				}
				// Scalar input is valid-invalid-valid. Store may reorder distinct runs;
				// its assertion is partial persistence, independent of iteration order.
				for _, run := range expected {
					require.Equal(t, run, readRunContract(t, db, run.RunId))
				}
			})
		})
	}
}

func TestRunUpdateCancelledContext(t *testing.T) {
	for _, path := range runUpdatePaths() {
		t.Run(path.name, func(t *testing.T) {
			withRunContractDb(t, func(t *testing.T, db *pgxpool.Pool, l *LookoutDb) {
				expected := seedRunContracts(t, l, 1)[0]
				ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
				cancel()
				err := path.update(l, ctx, []*model.UpdateJobRunInstruction{{RunId: expected.RunId, Node: pointer.String("cancelled")}})
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, expected, readRunContract(t, db, expected.RunId))
			})
		})
	}
}

func TestStoreRunUpdateForTerminalJob(t *testing.T) {
	withRunContractDb(t, func(t *testing.T, db *pgxpool.Pool, l *LookoutDb) {
		ctx := armadacontext.Background()
		expected := seedRunContracts(t, l, 1)[0]
		require.NoError(t, l.UpdateJobs(ctx, []*model.UpdateJobInstruction{{
			JobId: JobId, State: pointer.Int32(lookout.JobSucceededOrdinal),
		}}))
		job := getJob(t, db, JobId)
		require.Equal(t, int32(lookout.JobSucceededOrdinal), job.State)
		require.NoError(t, l.Store(ctx, &model.InstructionSet{
			JobsToUpdate:    []*model.UpdateJobInstruction{{JobId: JobId, State: pointer.Int32(lookout.JobRunningOrdinal)}},
			JobRunsToUpdate: []*model.UpdateJobRunInstruction{{RunId: expected.RunId, Node: pointer.String("late-node"), Finished: &finishedTime}},
		}))
		require.Equal(t, job, getJob(t, db, JobId), "late job state is filtered")
		expected.Node = pointer.String("late-node")
		expected.Finished = &finishedTime
		require.Equal(t, expected, readRunContract(t, db, expected.RunId), "run update is still accepted")
	})
}
