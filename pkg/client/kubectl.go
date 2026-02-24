package client

import (
	"strings"

	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
)

// GetKubectlCommand constructs a kubectl command for interacting with a pod.
// runIndex is optional - if nil, uses legacy pod name format for backward compatibility.
func GetKubectlCommand(cluster string, namespace string, jobId string, podNumber int, runIndex *int, cmd string) string {
	t := viper.GetString("kubectlCommandTemplate")
	if t == "" {
		t = "kubectl --context {{cluster}} -n {{namespace}} {{cmd}} {{pod}}"
	}

	podName := common.BuildPodName(jobId, podNumber, runIndex)

	r := strings.NewReplacer(
		"{{cluster}}",
		cluster,
		"{{namespace}}",
		namespace,
		"{{cmd}}",
		cmd,
		"{{pod}}",
		podName,
	)
	command := r.Replace(t)

	return command
}
