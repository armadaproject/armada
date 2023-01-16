package main

import (
	"os"

	"github.com/armadaproject/armada/cmd/armadactl/cmd"
	"github.com/armadaproject/armada/internal/common"
)

// Config is handled by cmd/params.go
func main() {
	common.ConfigureCommandLineLogging()
	root := cmd.RootCmd()
	if err := root.Execute(); err != nil {
		// We don't need to log the error here because cobra has already done this for us
		os.Exit(1)
	}
}
