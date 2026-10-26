package client

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestGetKubectlCommandForPod_CustomTemplate(t *testing.T) {
	viper.Set("kubectlCommandTemplate", "kubectl --kubeconfig {{cluster}} -n {{namespace}} {{cmd}} pod/{{pod}}")
	t.Cleanup(func() { viper.Set("kubectlCommandTemplate", "") })

	assert.Equal(t, "kubectl --kubeconfig cluster-1 -n ns logs pod/armada-run-1", GetKubectlCommandForPod("cluster-1", "ns", "armada-run-1", "logs"))
}
