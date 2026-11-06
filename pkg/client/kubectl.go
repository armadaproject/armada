package client

import (
	"strings"

	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common/podname"
)

// GetKubectlCommand returns a kubectl command for the job-scoped pod with the default prefix. The executor makes one
// pod for each run, so the pod number is always 0, and the command ignores podNumber.
//
// Deprecated: run-scoped pod names and another prefix give the pod another name. Use GetKubectlCommandForPod with the
// pod name from the job events.
func GetKubectlCommand(cluster string, namespace string, jobId string, podNumber int, cmd string) string {
	return GetKubectlCommandForPod(cluster, namespace, podname.Default(jobId), cmd)
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
