package scheduleringester

import (
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	f "github.com/armadaproject/armada/internal/common/ingest/testfixtures"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	"github.com/armadaproject/armada/internal/common/util"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestPgText(t *testing.T) {
	tests := map[string]struct {
		in   string
		want string
	}{
		"ascii unchanged":            {in: "job-set", want: "job-set"},
		"multi-byte UTF-8 unchanged": {in: "jöb-sét", want: "jöb-sét"},
		"NUL removed":                {in: "a\x00b", want: "ab"},
		"invalid UTF-8 replaced":     {in: "a\xffb", want: "a�b"},
		"both":                       {in: "a\x00\xffb", want: "a�b"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, pgText(tc.in))
		})
	}
}

func TestStorableText(t *testing.T) {
	assert.True(t, storableText("jöb-sét"))
	assert.False(t, storableText("a\x00b"))
	assert.False(t, storableText("a\xffb"))
}

// Strings that are only recorded are rewritten so that Postgres can store them.
func TestScrubRecordedStrings(t *testing.T) {
	es := &armadaevents.EventSequence{
		UserId: "user\x00",
		Events: []*armadaevents.EventSequence_Event{
			{Event: &armadaevents.EventSequence_Event_CancelJob{CancelJob: &armadaevents.CancelJob{JobId: "cancel", Reason: "reason\xff"}}},
			{Event: &armadaevents.EventSequence_Event_CancelJobSet{CancelJobSet: &armadaevents.CancelJobSet{Reason: "reason\x00"}}},
			{Event: &armadaevents.EventSequence_Event_JobPreemptionRequested{JobPreemptionRequested: &armadaevents.JobPreemptionRequested{JobId: "preempt", Reason: "reason\xff"}}},
		},
	}

	scrubRecordedStrings(es)

	assert.Equal(t, "user", es.UserId)
	assert.Equal(t, "reason�", es.Events[0].GetCancelJob().Reason)
	assert.Equal(t, "reason", es.Events[1].GetCancelJobSet().Reason)
	assert.Equal(t, "reason�", es.Events[2].GetJobPreemptionRequested().Reason)
}

// Names that pick which jobs an event acts on are never rewritten: an unstorable queue or job set
// drops the whole sequence, and an unstorable job id drops only its event.
func TestConvert_DropsEventsWithUnstorableTargets(t *testing.T) {
	cancelJob := func(jobId string) *armadaevents.EventSequence_Event {
		return &armadaevents.EventSequence_Event{
			Created: f.BaseTimeProto,
			Event:   &armadaevents.EventSequence_Event_CancelJob{CancelJob: &armadaevents.CancelJob{JobId: jobId}},
		}
	}
	cancelJobSet := &armadaevents.EventSequence_Event{
		Created: f.BaseTimeProto,
		Event:   &armadaevents.EventSequence_Event_CancelJobSet{CancelJobSet: &armadaevents.CancelJobSet{}},
	}
	goodJobId := util.NewULID()
	tests := map[string]struct {
		es      *armadaevents.EventSequence
		wantOps []DbOperation
	}{
		"job set with NUL byte": {
			es: &armadaevents.EventSequence{Queue: "q", JobSetName: "target\x00", Events: []*armadaevents.EventSequence_Event{cancelJobSet}},
		},
		"job set with invalid UTF-8": {
			es: &armadaevents.EventSequence{Queue: "q", JobSetName: "target\xff", Events: []*armadaevents.EventSequence_Event{cancelJobSet}},
		},
		"queue with NUL byte": {
			es: &armadaevents.EventSequence{Queue: "q\x00", JobSetName: "target", Events: []*armadaevents.EventSequence_Event{cancelJobSet}},
		},
		"job id with NUL byte drops only that event": {
			es: &armadaevents.EventSequence{
				Queue: "q", JobSetName: "target", UserId: "user",
				Events: []*armadaevents.EventSequence_Event{cancelJob(goodJobId + "\x00"), cancelJob(goodJobId)},
			},
			wantOps: []DbOperation{MarkJobsCancelRequested{
				cancelUser:   "user",
				cancelReason: cancelReasonUserInitiated,
				jobIds:       map[JobSetKey][]string{{queue: "q", jobSet: "target"}: {goodJobId}},
			}},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			converter, err := NewJobSetEventsInstructionConverter(m)
			require.NoError(t, err)
			ops := converter.Convert(armadacontext.Background(), &utils.EventsWithIds[*armadaevents.EventSequence]{
				Events: []*armadaevents.EventSequence{tc.es},
			})
			if tc.wantOps == nil {
				assert.Empty(t, ops.Ops)
			} else {
				assert.Equal(t, tc.wantOps, ops.Ops)
			}
		})
	}
}

// Postgres rejects NUL bytes and invalid UTF-8 in text columns, and SchedulerDb.Store retries a
// failed batch without bound. Before the converter sanitised user strings, one event carrying such
// a string stalled the whole batch, so unrelated events from other queues were never ingested.
// This test runs the real converter and the real sink against Postgres (started by
// `mage tests`, as for the package's other DB tests) and checks that the batch commits and that
// the unrelated job in it is stored.
func TestStore_UnstorableUserStringDoesNotBlockBatch(t *testing.T) {
	tests := map[string]*armadaevents.EventSequence{
		"cancel reason with NUL byte":      cancelJobSequence("a\x00b"),
		"cancel reason with invalid UTF-8": cancelJobSequence("a\xffb"),
		"submit job set name with NUL byte": func() *armadaevents.EventSequence {
			es := submitJobSequence(util.NewULID())
			es.JobSetName = "js\x00"
			return es
		}(),
	}
	for name, badSequence := range tests {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, schedulerdb.WithTestDb(func(_ *schedulerdb.Queries, db *pgxpool.Pool) error {
				unrelatedJobId := util.NewULID()
				batch := []*armadaevents.EventSequence{submitJobSequence(unrelatedJobId), badSequence}
				require.NoError(t, storeOnce(t, db, batch), "batch must commit")
				require.True(t, jobExists(t, db, unrelatedJobId), "unrelated job in the same batch must be ingested")
				return nil
			}))
		})
	}
}

// A job set cancel for "jobset-b\x00" must not cancel the jobs of the valid job set "jobset-b".
// The old API accepted such names, so these events can still be in Pulsar.
func TestStore_UnstorableJobSetDoesNotCancelOtherJobSet(t *testing.T) {
	require.NoError(t, schedulerdb.WithTestDb(func(_ *schedulerdb.Queries, db *pgxpool.Pool) error {
		jobId := util.NewULID()
		require.NoError(t, storeOnce(t, db, []*armadaevents.EventSequence{submitJobSequence(jobId)}))

		cancel := &armadaevents.EventSequence{
			Queue: "queue-b", JobSetName: "jobset-b\x00", UserId: "user-b",
			Events: []*armadaevents.EventSequence_Event{{
				Created: f.BaseTimeProto,
				Event:   &armadaevents.EventSequence_Event_CancelJobSet{CancelJobSet: &armadaevents.CancelJobSet{}},
			}},
		}
		require.NoError(t, storeOnce(t, db, []*armadaevents.EventSequence{cancel}))

		var cancelRequested bool
		require.NoError(t, db.QueryRow(armadacontext.Background(),
			"SELECT cancel_by_jobset_requested FROM jobs WHERE job_id = $1", jobId).Scan(&cancelRequested))
		assert.False(t, cancelRequested, "job in jobset-b must not be cancelled")
		return nil
	}))
}

func submitJobSequence(jobId string) *armadaevents.EventSequence {
	ev := proto.Clone(f.Submit).(*armadaevents.EventSequence_Event)
	ev.GetSubmitJob().JobId = jobId
	return &armadaevents.EventSequence{
		Queue: "queue-b", JobSetName: "jobset-b", UserId: "user-b", Groups: []string{"group-b"},
		Events: []*armadaevents.EventSequence_Event{ev},
	}
}

func cancelJobSequence(reason string) *armadaevents.EventSequence {
	return &armadaevents.EventSequence{
		Queue: "queue-a", JobSetName: "jobset-a", UserId: "user-a", Groups: []string{"group-a"},
		Events: []*armadaevents.EventSequence_Event{{
			Created: f.BaseTimeProto,
			Event: &armadaevents.EventSequence_Event_CancelJob{
				CancelJob: &armadaevents.CancelJob{JobId: util.NewULID(), Reason: reason},
			},
		}},
	}
}

// storeOnce converts seqs with the real converter and writes the result with the real sink in a
// single transaction, as one attempt of SchedulerDb.Store does. Store itself retries a failed batch
// forever and ignores cancellation, so calling it here would leave a stuck retry loop running
// after a failing test; one attempt surfaces the error instead.
func storeOnce(t *testing.T, db *pgxpool.Pool, seqs []*armadaevents.EventSequence) error {
	ctx := armadacontext.Background()
	converter, err := NewJobSetEventsInstructionConverter(m)
	require.NoError(t, err)
	ops := converter.Convert(ctx, &utils.EventsWithIds[*armadaevents.EventSequence]{Events: seqs})
	sink := NewSchedulerDb(db, metrics.NewMetricsWithRegistry("sanitize_test_", prometheus.NewRegistry()), 10*time.Millisecond, 50*time.Millisecond, 5*time.Second)
	return pgx.BeginTxFunc(ctx, db, pgx.TxOptions{}, func(tx pgx.Tx) error {
		for _, op := range ops.Ops {
			if err := sink.WriteDbOp(ctx, tx, op); err != nil {
				return err
			}
		}
		return nil
	})
}

func jobExists(t *testing.T, db *pgxpool.Pool, jobId string) bool {
	var n int
	require.NoError(t, db.QueryRow(armadacontext.Background(), "SELECT count(*) FROM jobs WHERE job_id = $1", jobId).Scan(&n))
	return n == 1
}
