package client

import (
	"strings"

	"github.com/spf13/viper"
)

func GetKubectlCommand(cluster string, namespace string, cmd string) string {
	t := viper.GetString("kubectlCommandTemplate")
	if t == "" {
		t = "kubectl --context {{cluster}} -n {{namespace}} {{cmd}}"
	}
	r := strings.NewReplacer("{{cluster}}", cluster, "{{namespace}}", namespace, "{{cmd}}", cmd)
	command := r.Replace(t)

	return command
}
