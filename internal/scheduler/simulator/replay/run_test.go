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
		{ID: "excluded", SubmittedUS: 99, UsefulUS: 120},
		{ID: "complete", SubmittedUS: 100, UsefulUS: 120},
		{ID: "running", SubmittedUS: 100, UsefulUS: 200},
		{ID: "queued", SubmittedUS: 101, UsefulUS: 100},
		{ID: "after", SubmittedUS: 300, UsefulUS: 10},
	}
	results := &observations{}
	scheduler := &scriptedScheduler{
		rounds: map[int64]replay.Decisions{
			100: {Allocate: []replay.Placement{{JobID: "complete"}, {JobID: "running"}}},
		},
	}
	stats, err := replay.Run(ctx, replay.Window{
		WarmupUS: 100, StartUS: 150, EndUS: 300, CycleUS: 10,
	}, readJobs(arrivals), scheduler, results)
	require.NoError(t, err)
	require.Equal(t, replay.Stats{BeforeWarmup: 1, AtOrAfterEnd: 1, Admitted: 3, PeakQueued: 2, PeakRunning: 2, PeakCompletionEvents: 1, PeakLookahead: 1}, stats)
	require.Equal(t, []string{"complete", "running", "queued"}, scheduler.admitted)
	require.Equal(t, []string{"complete"}, scheduler.completed)
	for i := range results.jobs {
		results.jobs[i].Offered = replay.Job{}
		results.jobs[i].FirstAllocationUS = nil
		results.jobs[i].FirstUsefulUS = nil
	}
	for i := range results.attempts {
		results.attempts[i].Offered = replay.Job{}
	}
	require.Equal(t, []replay.JobResult{
		{JobID: "complete", State: replay.Completed, Attempts: 1, ObservedUS: 220, DurableUS: 120},
		{JobID: "queued", State: replay.Queued, ObservedUS: 300, RemainingUS: 100},
		{JobID: "running", State: replay.Running, Attempts: 1, ObservedUS: 300, ProvisionalUS: 200},
	}, results.jobs)
	require.Equal(t, []replay.AttemptResult{
		{
			JobID: "complete", Ordinal: 1, Placement: replay.Placement{JobID: "complete"},
			AllocatedUS: 100, EndUS: 220, State: replay.Completed, RetainedUS: 120,
		},
		{
			JobID: "running", Ordinal: 1, Placement: replay.Placement{JobID: "running"},
			AllocatedUS: 100, EndUS: 300, State: replay.Running, ProvisionalUS: 200,
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
			job := replay.Job{ID: "job", UsefulUS: 8, StartupUS: 2, CheckpointUS: 3}
			out := &observations{}
			scheduler := &scriptedScheduler{rounds: map[int64]replay.Decisions{
				0: {Allocate: []replay.Placement{{JobID: job.ID, RunID: "first"}}},
				tc.preemptAt: {
					Preempt:  []replay.Preemption{{JobID: job.ID, Reason: "urgency"}},
					Allocate: []replay.Placement{{JobID: job.ID, RunID: "replacement"}},
				},
			}}
			_, err := replay.Run(t.Context(), replay.Window{EndUS: 20, CycleUS: 1}, readJobs([]replay.Job{job}), scheduler, out)
			require.NoError(t, err)
			require.Equal(t, []string{job.ID}, scheduler.completed)
			require.Len(t, out.attempts, 2)
			first, restart := out.attempts[0], out.attempts[1]
			require.Equal(t, replay.Preempted, first.State)
			require.Equal(t, "urgency", first.Reason)
			require.Equal(t, tc.startup, first.StartupUS)
			require.Equal(t, tc.retained, first.RetainedUS)
			require.Equal(t, tc.lost, first.LostUS)
			require.Equal(t, replay.Completed, restart.State)
			require.Equal(t, tc.preemptAt+2+8-tc.retained, restart.EndUS)
			require.Equal(t, int64(2), restart.StartupUS)
			require.Equal(t, job.UsefulUS, out.jobs[0].DurableUS)
		})
	}
}

func TestReplayCutoffDoesNotFinalizeCheckpointAtEnd(t *testing.T) {
	job := replay.Job{ID: "checkpoint", UsefulUS: 30, CheckpointUS: 10}
	out := &observations{}
	scheduler := &scriptedScheduler{rounds: map[int64]replay.Decisions{
		0: {Allocate: []replay.Placement{{JobID: job.ID}}},
	}}
	_, err := replay.Run(t.Context(), replay.Window{EndUS: 20, CycleUS: 1}, readJobs([]replay.Job{job}), scheduler, out)
	require.NoError(t, err)
	require.Empty(t, scheduler.completed)
	require.Len(t, out.attempts, 1)
	require.Equal(t, int64(10), out.attempts[0].RetainedUS)
	require.Equal(t, int64(10), out.attempts[0].ProvisionalUS)
}
