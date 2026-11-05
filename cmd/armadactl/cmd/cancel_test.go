package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/armadactl"
	"github.com/armadaproject/armada/pkg/api"
)

func TestCancelExecutor(t *testing.T) {
	tests := map[string]struct {
		flags []flag
		want  executorCall
	}{
		// Omitting priority-classes means all priority classes, which the
		// executor API represents as an empty slice. An unnarrowed queue
		// selection expands to every queue.
		"without priority-classes": {
			flags: nil,
			want: executorCall{
				executor:        "test-executor",
				queues:          []string{"queue-a", "queue-b"},
				priorityClasses: []string{},
				pools:           []string{},
			},
		},
		"with a single priority class": {
			flags: []flag{{"priority-classes", "armada-default"}},
			want: executorCall{
				executor:        "test-executor",
				queues:          []string{"queue-a", "queue-b"},
				priorityClasses: []string{"armada-default"},
				pools:           []string{},
			},
		},
		"with multiple priority classes": {
			flags: []flag{{"priority-classes", "armada-default,armada-preemptible"}},
			want: executorCall{
				executor:        "test-executor",
				queues:          []string{"queue-a", "queue-b"},
				priorityClasses: []string{"armada-default", "armada-preemptible"},
				pools:           []string{},
			},
		},
		"with queues and pools": {
			flags: []flag{{"queues", "queue-a"}, {"pools", "pool-1,pool-2"}},
			want: executorCall{
				executor:        "test-executor",
				queues:          []string{"queue-a"},
				priorityClasses: []string{},
				pools:           []string{"pool-1", "pool-2"},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			a := armadactl.New()
			cmd := cancelExecutorCmd(a)

			var got []executorCall
			withFakeAPIs(t, a, cmd, func() {
				a.Params.QueueAPI.GetAll = func() ([]*api.Queue, error) { return testQueues(), nil }
				a.Params.ExecutorAPI.CancelOnExecutor = func(executor string, queues, priorityClasses, pools []string) error {
					got = append(got, executorCall{executor, queues, priorityClasses, pools})
					return nil
				}
			})

			cmd.SetArgs([]string{"test-executor"})
			for _, f := range tc.flags {
				require.NoError(t, cmd.Flags().Set(f.name, f.value))
			}

			require.NoError(t, cmd.Execute())
			require.Equal(t, []executorCall{tc.want}, got)
		})
	}
}

func TestCancelNode(t *testing.T) {
	tests := map[string]struct {
		flags []flag
		want  nodeCall
	}{
		// Omitting priority-classes means all priority classes, which the node
		// API represents as an empty slice.
		"without priority-classes": {
			flags: []flag{{"executor", "test-executor"}},
			want: nodeCall{
				node:            "test-node",
				executor:        "test-executor",
				queues:          []string{"queue-a", "queue-b"},
				priorityClasses: []string{},
			},
		},
		"with a single priority class": {
			flags: []flag{{"executor", "test-executor"}, {"priority-classes", "armada-default"}},
			want: nodeCall{
				node:            "test-node",
				executor:        "test-executor",
				queues:          []string{"queue-a", "queue-b"},
				priorityClasses: []string{"armada-default"},
			},
		},
		"with multiple priority classes": {
			flags: []flag{{"executor", "test-executor"}, {"priority-classes", "armada-default,armada-preemptible"}},
			want: nodeCall{
				node:            "test-node",
				executor:        "test-executor",
				queues:          []string{"queue-a", "queue-b"},
				priorityClasses: []string{"armada-default", "armada-preemptible"},
			},
		},
		"with queues": {
			flags: []flag{{"executor", "test-executor"}, {"queues", "queue-a"}},
			want: nodeCall{
				node:            "test-node",
				executor:        "test-executor",
				queues:          []string{"queue-a"},
				priorityClasses: []string{},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			a := armadactl.New()
			cmd := cancelNodeCmd(a)

			var got []nodeCall
			withFakeAPIs(t, a, cmd, func() {
				a.Params.QueueAPI.GetAll = func() ([]*api.Queue, error) { return testQueues(), nil }
				a.Params.NodeAPI.CancelOnNode = func(node, executor string, queues, priorityClasses []string) error {
					got = append(got, nodeCall{node, executor, queues, priorityClasses})
					return nil
				}
			})

			cmd.SetArgs([]string{"test-node"})
			for _, f := range tc.flags {
				require.NoError(t, cmd.Flags().Set(f.name, f.value))
			}

			require.NoError(t, cmd.Execute())
			require.Equal(t, []nodeCall{tc.want}, got)
		})
	}
}

func TestCancelQueues(t *testing.T) {
	// job-states is required by this command, so every case sets it.
	tests := map[string]struct {
		args  []string
		flags []flag
		want  []queueCall
	}{
		// Omitting priority-classes means all priority classes, which the
		// queue API represents as an empty slice.
		"without priority-classes": {
			args:  []string{"queue-a"},
			flags: []flag{{"job-states", "queued"}},
			want: []queueCall{
				{
					queue:           "queue-a",
					priorityClasses: []string{},
					jobStates:       []api.JobState{api.JobState_QUEUED},
					pools:           []string{},
				},
			},
		},
		"with a single priority class": {
			args:  []string{"queue-a"},
			flags: []flag{{"job-states", "queued"}, {"priority-classes", "armada-default"}},
			want: []queueCall{
				{
					queue:           "queue-a",
					priorityClasses: []string{"armada-default"},
					jobStates:       []api.JobState{api.JobState_QUEUED},
					pools:           []string{},
				},
			},
		},
		"with multiple priority classes": {
			args:  []string{"queue-a"},
			flags: []flag{{"job-states", "queued"}, {"priority-classes", "armada-default,armada-preemptible"}},
			want: []queueCall{
				{
					queue:           "queue-a",
					priorityClasses: []string{"armada-default", "armada-preemptible"},
					jobStates:       []api.JobState{api.JobState_QUEUED},
					pools:           []string{},
				},
			},
		},
		"with multiple job states and pools": {
			args:  []string{"queue-a"},
			flags: []flag{{"job-states", "queued,running"}, {"pools", "pool-1"}},
			want: []queueCall{
				{
					queue:           "queue-a",
					priorityClasses: []string{},
					jobStates:       []api.JobState{api.JobState_QUEUED, api.JobState_RUNNING},
					pools:           []string{"pool-1"},
				},
			},
		},
		"cancels each selected queue": {
			args:  []string{"queue-a", "queue-b"},
			flags: []flag{{"job-states", "queued"}},
			want: []queueCall{
				{
					queue:           "queue-a",
					priorityClasses: []string{},
					jobStates:       []api.JobState{api.JobState_QUEUED},
					pools:           []string{},
				},
				{
					queue:           "queue-b",
					priorityClasses: []string{},
					jobStates:       []api.JobState{api.JobState_QUEUED},
					pools:           []string{},
				},
			},
		},
		// dry-run reports what would happen without calling the API.
		"dry-run calls nothing": {
			args:  []string{"queue-a"},
			flags: []flag{{"job-states", "queued"}, {"dry-run", "true"}},
			want:  nil,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			a := armadactl.New()
			cmd := cancelQueueCmd(a)

			var got []queueCall
			withFakeAPIs(t, a, cmd, func() {
				a.Params.QueueAPI.GetAll = func() ([]*api.Queue, error) { return testQueues(), nil }
				a.Params.QueueAPI.Cancel = func(queue string, priorityClasses []string, jobStates []api.JobState, pools []string) error {
					got = append(got, queueCall{queue, priorityClasses, jobStates, pools})
					return nil
				}
			})

			cmd.SetArgs(tc.args)
			for _, f := range tc.flags {
				require.NoError(t, cmd.Flags().Set(f.name, f.value))
			}

			require.NoError(t, cmd.Execute())
			require.Equal(t, tc.want, got)
		})
	}
}

func TestCancelQueuesRequiresQueueSelection(t *testing.T) {
	// Guards against accidentally cancelling every queue: selection must be
	// narrowed by name or by label.
	a := armadactl.New()
	cmd := cancelQueueCmd(a)

	called := false
	withFakeAPIs(t, a, cmd, func() {
		a.Params.QueueAPI.GetAll = func() ([]*api.Queue, error) { return testQueues(), nil }
		a.Params.QueueAPI.Cancel = func(queue string, priorityClasses []string, jobStates []api.JobState, pools []string) error {
			called = true
			return nil
		}
	})

	// Must be non-nil: cobra falls back to os.Args[1:] when args are nil.
	cmd.SetArgs([]string{})
	require.NoError(t, cmd.Flags().Set("job-states", "queued"))
	cmd.SilenceUsage = true

	require.Error(t, cmd.Execute())
	require.False(t, called, "no queue should be cancelled without a selection")
}
