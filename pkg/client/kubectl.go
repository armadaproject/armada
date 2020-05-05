package client

import (
	"strings"

	"github.com/spf13/viper"

	"github.com/G-Research/armada/internal/common"
)

func GetKubectlCommand(cluster string, namespace string, jobId string, cmd string) string {
	t := viper.GetString("kubectlCommandTemplate")
	if t == "" {
		t = "kubectl --context {{cluster}} -n {{namespace}} {{cmd}} {{pod}}"
	}
	r := strings.NewReplacer("{{cluster}}", cluster, "{{namespace}}", namespace, "{{cmd}}", cmd, "{{pod}}", common.PodNamePrefix+jobId)
	command := r.Replace(t)

	return command
}
