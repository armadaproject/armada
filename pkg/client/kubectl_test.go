package client

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestGetKubectlCommandForPod(t *testing.T) {
	tests := []struct {
		name     string
		template string
		want     string
	}{
		{
			name: "the default template names the pod",
			want: "kubectl --context cluster-1 -n ns logs armada-run-1",
		},
		{
			name:     "a custom template gets the pod name",
			template: "kubectl --kubeconfig {{cluster}} -n {{namespace}} {{cmd}} pod/{{pod}}",
			want:     "kubectl --kubeconfig cluster-1 -n ns logs pod/armada-run-1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			viper.Set("kubectlCommandTemplate", tc.template)
			t.Cleanup(func() { viper.Set("kubectlCommandTemplate", "") })

			assert.Equal(t, tc.want, GetKubectlCommandForPod("cluster-1", "ns", "armada-run-1", "logs"))
		})
	}
}

func TestGetKubectlCommand_BuildsTheJobScopedName(t *testing.T) {
	assert.Equal(t, "kubectl --context cluster-1 -n ns logs armada-job-1-0", GetKubectlCommand("cluster-1", "ns", "job-1", 0, "logs"))
}
