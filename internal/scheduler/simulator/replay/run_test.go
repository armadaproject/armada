package replay_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/scheduler/simulator/replay"
)

func TestRunObservesHalfOpenHorizon(t *testing.T) {
	ctx := context.Background()
	arrivals := []replay.Job{
		{ID: "excluded", SubmittedSeconds: 99, UsefulSeconds: 120},
		{ID: "complete", SubmittedSeconds: 100, UsefulSeconds: 120},
		{ID: "running", SubmittedSeconds: 100, UsefulSeconds: 200},
		{ID: "queued", SubmittedSeconds: 101, UsefulSeconds: 100},
		{ID: "after", SubmittedSeconds: 300, UsefulSeconds: 10},
	}
	results := &observations{}
	scheduler := &scriptedScheduler{
		rounds: map[int64]replay.Decisions{
			100: {Allocate: []replay.Placement{{JobID: "complete"}, {JobID: "running"}}},
		},
	}
	stats, err := replay.Run(ctx, replay.Window{
		WarmupSeconds: 100, StartSeconds: 150, EndSeconds: 300, CycleSeconds: 10,
	}, readJobs(arrivals), scheduler, results)
	require.NoError(t, err)
	require.Equal(t, replay.Stats{BeforeWarmup: 1, AtOrAfterEnd: 1, Admitted: 3, PeakQueued: 2, PeakRunning: 2, PeakCompletionEvents: 1, PeakLookahead: 1}, stats)
	require.Equal(t, []string{"complete", "running", "queued"}, scheduler.admitted)
	require.Equal(t, []string{"complete"}, scheduler.completed)
	for i := range results.jobs {
		results.jobs[i].Offered = replay.Job{}
		results.jobs[i].FirstAllocationSeconds = nil
		results.jobs[i].FirstUsefulSeconds = nil
	}
	for i := range results.attempts {
		results.attempts[i].Offered = replay.Job{}
	}
	require.Equal(t, []replay.JobResult{
		{JobID: "complete", State: replay.Completed, Attempts: 1, ObservedSeconds: 220, DurableSeconds: 120},
		{JobID: "queued", State: replay.Queued, ObservedSeconds: 300, RemainingSeconds: 100},
		{JobID: "running", State: replay.Running, Attempts: 1, ObservedSeconds: 300, ProvisionalSeconds: 200},
	}, results.jobs)
	require.Equal(t, []replay.AttemptResult{
		{
			JobID: "complete", Ordinal: 1, Placement: replay.Placement{JobID: "complete"},
			AllocatedSeconds: 100, EndSeconds: 220, State: replay.Completed, RetainedSeconds: 120,
		},
		{
			JobID: "running", Ordinal: 1, Placement: replay.Placement{JobID: "running"},
			AllocatedSeconds: 100, EndSeconds: 300, State: replay.Running, ProvisionalSeconds: 200,
		},
	}, results.attempts)
}

func readJobs(jobs []replay.Job) func() (replay.Job, error) {
	return func() (replay.Job, error) {
		if len(jobs) == 0 {
			return replay.Job{}, io.EOF
		}
		job := jobs[0]
		jobs = jobs[1:]
		return job, nil
	}
}

type scriptedScheduler struct {
	rounds    map[int64]replay.Decisions
	admitted  []string
	completed []string
}

func (s *scriptedScheduler) Admit(_ context.Context, _ int64, job replay.Job) (string, error) {
	s.admitted = append(s.admitted, job.ID)
	return "", nil
}

func (s *scriptedScheduler) Complete(_ context.Context, _ int64, id string) error {
	s.completed = append(s.completed, id)
	return nil
}

func (s *scriptedScheduler) Schedule(_ context.Context, now int64) (replay.Decisions, error) {
	return s.rounds[now], nil
}

type observations struct {
	jobs     []replay.JobResult
	attempts []replay.AttemptResult
}

func (o *observations) Queue(_ replay.QueueInterval) error { return nil }

func (o *observations) Job(row replay.JobResult) error {
	o.jobs = append(o.jobs, row)
	return nil
}

func (o *observations) Attempt(row replay.AttemptResult) error {
	o.attempts = append(o.attempts, row)
	return nil
}

func TestPreemptionKeepsOnlyCompletedCheckpoints(t *testing.T) {
	for _, tc := range []struct {
		name                               string
		preemptAt, startup, retained, lost int64
	}{
		{"during startup", 1, 1, 0, 0},
		{"before checkpoint", 4, 2, 0, 2},
		{"exactly at checkpoint", 5, 2, 3, 0},
		{"after checkpoint", 6, 2, 3, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := replay.Job{ID: "job", UsefulSeconds: 8, StartupSeconds: 2, CheckpointSeconds: 3}
			out := &observations{}
			scheduler := &scriptedScheduler{rounds: map[int64]replay.Decisions{
				0: {Allocate: []replay.Placement{{JobID: job.ID, RunID: "first"}}},
				tc.preemptAt: {
					Preempt:  []replay.Preemption{{JobID: job.ID, Reason: "urgency"}},
					Allocate: []replay.Placement{{JobID: job.ID, RunID: "replacement"}},
				},
			}}
			_, err := replay.Run(t.Context(), replay.Window{EndSeconds: 20, CycleSeconds: 1}, readJobs([]replay.Job{job}), scheduler, out)
			require.NoError(t, err)
			require.Equal(t, []string{job.ID}, scheduler.completed)
			require.Len(t, out.attempts, 2)
			first, restart := out.attempts[0], out.attempts[1]
			require.Equal(t, replay.Preempted, first.State)
			require.Equal(t, "urgency", first.Reason)
			require.Equal(t, tc.startup, first.StartupSeconds)
			require.Equal(t, tc.retained, first.RetainedSeconds)
			require.Equal(t, tc.lost, first.LostSeconds)
			require.Equal(t, replay.Completed, restart.State)
			require.Equal(t, tc.preemptAt+2+8-tc.retained, restart.EndSeconds)
			require.Equal(t, int64(2), restart.StartupSeconds)
			require.Equal(t, job.UsefulSeconds, out.jobs[0].DurableSeconds)
		})
	}
}

func TestReplayCutoffDoesNotFinalizeCheckpointAtEnd(t *testing.T) {
	job := replay.Job{ID: "checkpoint", UsefulSeconds: 30, CheckpointSeconds: 10}
	out := &observations{}
	scheduler := &scriptedScheduler{rounds: map[int64]replay.Decisions{
		0: {Allocate: []replay.Placement{{JobID: job.ID}}},
	}}
	_, err := replay.Run(t.Context(), replay.Window{EndSeconds: 20, CycleSeconds: 1}, readJobs([]replay.Job{job}), scheduler, out)
	require.NoError(t, err)
	require.Empty(t, scheduler.completed)
	require.Len(t, out.attempts, 1)
	require.Equal(t, int64(10), out.attempts[0].RetainedSeconds)
	require.Equal(t, int64(10), out.attempts[0].ProvisionalSeconds)
}
