package main

import (
	"os"

	"github.com/armadaproject/armada/cmd/scheduler/cmd"
	"github.com/armadaproject/armada/internal/common"
)

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()
	err := cmd.RootCmd().Execute()
	if err != nil {
		os.Exit(1)
	}
}
