package main

import (
	"os"

	"github.com/armadaproject/armada/cmd/testsuite/cmd"
	"github.com/armadaproject/armada/internal/common/logging"
)

// Config is handled by cmd/params.go
func main() {
	logging.ConfigureCliLogging()
	err := cmd.RootCmd().Execute()
	if err != nil {
		os.Exit(1)
	}
}
