package main

import (
	"github.com/G-Research/armada/cmd/testsuite/cmd"
	"github.com/G-Research/armada/internal/common"
)

// Config is handled by cmd/params.go
func main() {
	common.ConfigureCommandLineLogging()
	_ = cmd.RootCmd().Execute()
}
