package cmd

import (
	"io"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/armadactl"
	"github.com/armadaproject/armada/pkg/api"
)

// executorCall records the arguments armadactl passes to the executor API.
type executorCall struct {
	executor        string
	queues          []string
	priorityClasses []string
	pools           []string
}

// nodeCall records the arguments armadactl passes to the node API.
type nodeCall struct {
	node            string
	executor        string
	queues          []string
	priorityClasses []string
}

// queueCall records the arguments armadactl passes to the queue API.
type queueCall struct {
	queue           string
	priorityClasses []string
	jobStates       []api.JobState
	pools           []string
}

// testQueues is the set of queues the faked GetAll returns. Commands that are
// not narrowed by queue expand to all of them.
func testQueues() []*api.Queue {
	return []*api.Queue{{Name: "queue-a"}, {Name: "queue-b"}}
}

// withFakeAPIs runs the command's real PreRunE and then replaces the API
// functions that initParams just installed, so the command executes its real
// RunE all the way to the API boundary without making network calls.
//
// installFakes must overwrite the APIs *after* initParams has run, since
// initParams assigns every function pointer in Params.
func withFakeAPIs(t *testing.T, a *armadactl.App, cmd *cobra.Command, installFakes func()) {
	t.Helper()
	a.Out = io.Discard
	realPreRunE := cmd.PreRunE
	require.NotNil(t, realPreRunE, "expected the command to define a PreRunE")
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if err := realPreRunE(cmd, args); err != nil {
			return err
		}
		installFakes()
		return nil
	}
}

func TestPreemptExecutor(t *testing.T) {
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
			cmd := preemptExecutorCmd(a)

			var got []executorCall
			withFakeAPIs(t, a, cmd, func() {
				a.Params.QueueAPI.GetAll = func() ([]*api.Queue, error) { return testQueues(), nil }
				a.Params.ExecutorAPI.PreemptOnExecutor = func(executor string, queues, priorityClasses, pools []string) error {
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

func TestPreemptNode(t *testing.T) {
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
			cmd := preemptNodeCmd(a)

			var got []nodeCall
			withFakeAPIs(t, a, cmd, func() {
				a.Params.QueueAPI.GetAll = func() ([]*api.Queue, error) { return testQueues(), nil }
				a.Params.NodeAPI.PreemptOnNode = func(node, executor string, queues, priorityClasses []string) error {
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

func TestPreemptQueues(t *testing.T) {
	tests := map[string]struct {
		args  []string
		flags []flag
		want  []queueCall
	}{
		// Omitting priority-classes means all priority classes, which the
		// queue API represents as an empty slice.
		"without priority-classes": {
			args:  []string{"queue-a"},
			flags: nil,
			want: []queueCall{
				{queue: "queue-a", priorityClasses: []string{}, pools: []string{}},
			},
		},
		"with a single priority class": {
			args:  []string{"queue-a"},
			flags: []flag{{"priority-classes", "armada-default"}},
			want: []queueCall{
				{queue: "queue-a", priorityClasses: []string{"armada-default"}, pools: []string{}},
			},
		},
		"with multiple priority classes": {
			args:  []string{"queue-a"},
			flags: []flag{{"priority-classes", "armada-default,armada-preemptible"}},
			want: []queueCall{
				{queue: "queue-a", priorityClasses: []string{"armada-default", "armada-preemptible"}, pools: []string{}},
			},
		},
		"with pools": {
			args:  []string{"queue-a"},
			flags: []flag{{"pools", "pool-1"}},
			want: []queueCall{
				{queue: "queue-a", priorityClasses: []string{}, pools: []string{"pool-1"}},
			},
		},
		"preempts each selected queue": {
			args:  []string{"queue-a", "queue-b"},
			flags: nil,
			want: []queueCall{
				{queue: "queue-a", priorityClasses: []string{}, pools: []string{}},
				{queue: "queue-b", priorityClasses: []string{}, pools: []string{}},
			},
		},
		// dry-run reports what would happen without calling the API.
		"dry-run calls nothing": {
			args:  []string{"queue-a"},
			flags: []flag{{"dry-run", "true"}},
			want:  nil,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			a := armadactl.New()
			cmd := preemptQueuesCmd(a)

			var got []queueCall
			withFakeAPIs(t, a, cmd, func() {
				a.Params.QueueAPI.GetAll = func() ([]*api.Queue, error) { return testQueues(), nil }
				a.Params.QueueAPI.Preempt = func(queue string, priorityClasses, pools []string) error {
					got = append(got, queueCall{queue: queue, priorityClasses: priorityClasses, pools: pools})
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

func TestPreemptQueuesRequiresQueueSelection(t *testing.T) {
	a := armadactl.New()
	cmd := preemptQueuesCmd(a)

	called := false
	withFakeAPIs(t, a, cmd, func() {
		a.Params.QueueAPI.GetAll = func() ([]*api.Queue, error) { return testQueues(), nil }
		a.Params.QueueAPI.Preempt = func(queue string, priorityClasses, pools []string) error {
			called = true
			return nil
		}
	})

	// Must be non-nil: cobra falls back to os.Args[1:] when args are nil.
	cmd.SetArgs([]string{})
	cmd.SilenceUsage = true

	require.Error(t, cmd.Execute())
	require.False(t, called, "no queue should be preempted without a selection")
}
