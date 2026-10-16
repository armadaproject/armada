package client

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
)

// GetKubectlCommand returns a kubectl command for the pod armada-<jobId>-<podNumber>. This name is correct only for a
// job-scoped pod with the default prefix.
//
// Deprecated: run-scoped pod names and another prefix give the pod another name. Use GetKubectlCommandForPod with the
// pod name from the job events.
func GetKubectlCommand(cluster string, namespace string, jobId string, podNumber int, cmd string) string {
	return GetKubectlCommandForPod(cluster, namespace, fmt.Sprintf("%s%s-%d", common.PodNamePrefix, jobId, podNumber), cmd)
}

// GetKubectlCommandForPod returns a kubectl command for one pod, from the kubectlCommandTemplate setting.
func GetKubectlCommandForPod(cluster string, namespace string, podName string, cmd string) string {
	t := viper.GetString("kubectlCommandTemplate")
	if t == "" {
		t = "kubectl --context {{cluster}} -n {{namespace}} {{cmd}} {{pod}}"
	}
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
