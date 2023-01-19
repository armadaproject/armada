package main

import (
	"github.com/armadaproject/armada/cmd/testsuite/cmd"
	"github.com/armadaproject/armada/internal/common"
)

// Config is handled by cmd/params.go
func main() {
	common.ConfigureCommandLineLogging()
	_ = cmd.RootCmd().Execute()
}
