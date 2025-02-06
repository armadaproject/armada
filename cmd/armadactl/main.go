package main

import (
	"os"

	"github.com/armadaproject/armada/cmd/armadactl/cmd"
	"github.com/armadaproject/armada/internal/common/logging"
)

// Config is handled by cmd/params.go
func main() {
	logging.ConfigureCliLogging()
	root := cmd.RootCmd()
	if err := root.Execute(); err != nil {
		// We don't need to log the error here because cobra has already done this for us
		os.Exit(1)
	}
}
