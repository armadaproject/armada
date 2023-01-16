package client

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
)

func GetKubectlCommand(cluster string, namespace string, jobId string, podNumber int, cmd string) string {
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
		fmt.Sprintf("%s%s-%d", common.PodNamePrefix, jobId, podNumber),
	)
	command := r.Replace(t)

	return command
}
