package categorizer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/errormatch"
)

func TestClassify(t *testing.T) {
	tests := map[string]struct {
		configs    []CategoryConfig
		pod        *v1.Pod
		categories []string
	}{
		"OOM condition matches": {
			configs: []CategoryConfig{
				{Name: "oom", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
			},
			pod:        podWithTerminatedContainer(137, errormatch.ConditionOOMKilled, ""),
			categories: []string{"oom"},
		},
		"exit code In matches": {
			configs: []CategoryConfig{
				{Name: "cuda_error", Rules: []CategoryRule{
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{74, 75}}},
				}},
			},
			pod:        podWithTerminatedContainer(74, "Error", ""),
			categories: []string{"cuda_error"},
		},
		"exit code NotIn matches": {
			configs: []CategoryConfig{
				{Name: "unexpected", Rules: []CategoryRule{
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorNotIn, Values: []int32{1, 2}}},
				}},
			},
			pod:        podWithTerminatedContainer(42, "Error", ""),
			categories: []string{"unexpected"},
		},
		"termination message regex matches": {
			configs: []CategoryConfig{
				{Name: "gpu_error", Rules: []CategoryRule{
					{OnTerminationMessage: &errormatch.RegexMatcher{Pattern: "(?i)cuda.*error"}},
				}},
			},
			pod:        podWithTerminatedContainer(1, "Error", "CUDA memory error on device 0"),
			categories: []string{"gpu_error"},
		},
		"multiple categories can match": {
			configs: []CategoryConfig{
				{Name: "oom", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
				{Name: "high_exit", Rules: []CategoryRule{
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{137}}},
				}},
			},
			pod:        podWithTerminatedContainer(137, errormatch.ConditionOOMKilled, ""),
			categories: []string{"oom", "high_exit"},
		},
		"no match returns nil": {
			configs: []CategoryConfig{
				{Name: "oom", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
			},
			pod:        podWithTerminatedContainer(1, "Error", "normal failure"),
			categories: nil,
		},
		"nil pod returns nil": {
			configs: []CategoryConfig{
				{Name: "oom", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
			},
			pod:        nil,
			categories: nil,
		},
		"init container is checked": {
			configs: []CategoryConfig{
				{Name: "init_fail", Rules: []CategoryRule{
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{1}}},
				}},
			},
			pod: &v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
					InitContainerStatuses: []v1.ContainerStatus{
						{
							Name: "init",
							State: v1.ContainerState{
								Terminated: &v1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"},
							},
						},
					},
				},
			},
			categories: []string{"init_fail"},
		},
		"evicted condition matches": {
			configs: []CategoryConfig{
				{Name: "evicted", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionEvicted}},
				}},
			},
			pod: &v1.Pod{
				Status: v1.PodStatus{
					Phase:  v1.PodFailed,
					Reason: errormatch.ConditionEvicted,
				},
			},
			categories: []string{"evicted"},
		},
		"deadline exceeded condition matches": {
			configs: []CategoryConfig{
				{Name: "timeout", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionDeadlineExceeded}},
				}},
			},
			pod: &v1.Pod{
				Status: v1.PodStatus{
					Phase:  v1.PodFailed,
					Reason: errormatch.ConditionDeadlineExceeded,
				},
			},
			categories: []string{"timeout"},
		},
		"rules within category are ORed": {
			configs: []CategoryConfig{
				{Name: "infra", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{137}}},
				}},
			},
			pod:        podWithTerminatedContainer(137, "Error", ""),
			categories: []string{"infra"},
		},
		"exit code 0 container is skipped": {
			configs: []CategoryConfig{
				{Name: "msg_match", Rules: []CategoryRule{
					{OnTerminationMessage: &errormatch.RegexMatcher{Pattern: "success"}},
				}},
			},
			pod: &v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "sidecar",
							State: v1.ContainerState{
								Terminated: &v1.ContainerStateTerminated{
									ExitCode: 0,
									Reason:   "Completed",
									Message:  "success message",
								},
							},
						},
						{
							Name: "main",
							State: v1.ContainerState{
								Terminated: &v1.ContainerStateTerminated{
									ExitCode: 1,
									Reason:   "Error",
									Message:  "actual failure",
								},
							},
						},
					},
				},
			},
			categories: nil,
		},
		"containerName targets specific container": {
			configs: []CategoryConfig{
				{Name: "main_oom", Rules: []CategoryRule{
					{ContainerName: "main", OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
			},
			pod: &v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
					ContainerStatuses: []v1.ContainerStatus{
						{Name: "sidecar", State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{ExitCode: 137, Reason: errormatch.ConditionOOMKilled},
						}},
						{Name: "main", State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"},
						}},
					},
				},
			},
			categories: nil, // sidecar OOMs but main does not, rule targets main only
		},
		"containerName matches when container matches": {
			configs: []CategoryConfig{
				{Name: "main_error", Rules: []CategoryRule{
					{ContainerName: "main", OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{42}}},
				}},
			},
			pod: &v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
					ContainerStatuses: []v1.ContainerStatus{
						{Name: "main", State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{ExitCode: 42, Reason: "Error"},
						}},
					},
				},
			},
			categories: []string{"main_error"},
		},
		"no containerName matches any container": {
			configs: []CategoryConfig{
				{Name: "any_oom", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
			},
			pod: &v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
					ContainerStatuses: []v1.ContainerStatus{
						{Name: "sidecar", State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{ExitCode: 137, Reason: errormatch.ConditionOOMKilled},
						}},
					},
				},
			},
			categories: []string{"any_oom"}, // no containerName, matches sidecar
		},
		"empty config returns nil": {
			configs:    nil,
			pod:        podWithTerminatedContainer(1, "Error", ""),
			categories: nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			classifier, err := NewClassifier(tc.configs)
			require.NoError(t, err)
			result := classifier.Classify(tc.pod)
			assert.Equal(t, tc.categories, result)
		})
	}
}

func TestNewClassifier_ValidationErrors(t *testing.T) {
	tests := map[string]struct {
		configs     []CategoryConfig
		errContains string
	}{
		"empty name": {
			configs: []CategoryConfig{
				{Name: "", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
			},
			errContains: "must have a name",
		},
		"empty rule": {
			configs:     []CategoryConfig{{Name: "bad", Rules: []CategoryRule{{}}}},
			errContains: "must specify one of",
		},
		"multiple matchers in rule": {
			configs: []CategoryConfig{
				{Name: "bad", Rules: []CategoryRule{
					{
						OnConditions: []string{errormatch.ConditionOOMKilled},
						OnExitCodes:  &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{137}},
					},
				}},
			},
			errContains: "must specify only one of",
		},
		"unknown condition": {
			configs: []CategoryConfig{
				{Name: "bad", Rules: []CategoryRule{
					{OnConditions: []string{"NotARealCondition"}},
				}},
			},
			errContains: "unknown condition",
		},
		"invalid exit code operator": {
			configs: []CategoryConfig{
				{Name: "bad", Rules: []CategoryRule{
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: "Equals", Values: []int32{1}}},
				}},
			},
			errContains: "invalid exit code operator",
		},
		"empty exit code values": {
			configs: []CategoryConfig{
				{Name: "bad", Rules: []CategoryRule{
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: nil}},
				}},
			},
			errContains: "requires at least one value",
		},
		"invalid regex": {
			configs: []CategoryConfig{
				{Name: "bad", Rules: []CategoryRule{
					{OnTerminationMessage: &errormatch.RegexMatcher{Pattern: "[invalid"}},
				}},
			},
			errContains: "invalid regex",
		},
		"empty rules": {
			configs:     []CategoryConfig{{Name: "empty", Rules: nil}},
			errContains: "must have at least one rule",
		},
		"duplicate category name": {
			configs: []CategoryConfig{
				{Name: "oom", Rules: []CategoryRule{
					{OnConditions: []string{errormatch.ConditionOOMKilled}},
				}},
				{Name: "oom", Rules: []CategoryRule{
					{OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{137}}},
				}},
			},
			errContains: "duplicate category name",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := NewClassifier(tc.configs)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errContains)
		})
	}
}

func podWithTerminatedContainer(exitCode int32, reason, message string) *v1.Pod {
	return &v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodFailed,
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: "main",
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{
							ExitCode: exitCode,
							Reason:   reason,
							Message:  message,
						},
					},
				},
			},
		},
	}
}
