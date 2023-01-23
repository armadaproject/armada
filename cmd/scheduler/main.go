package main

import (
	"github.com/armadaproject/armada/cmd/scheduler/cmd"
	"github.com/armadaproject/armada/internal/common"
)

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()
	_ = cmd.RootCmd().Execute()
}
