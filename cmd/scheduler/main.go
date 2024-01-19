package main

import (
	_ "net/http/pprof"
	"os"

	"github.com/armadaproject/armada/cmd/scheduler/cmd"
	"github.com/armadaproject/armada/internal/common"
)

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()
	if err := cmd.RootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
