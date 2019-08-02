package main

import (
	"github.com/G-Research/k8s-batch/cmd/armadactl/cmd"
	"github.com/G-Research/k8s-batch/internal/common"
)

func main() {

	common.ConfigureLogging()
	cmd.Execute()
}
