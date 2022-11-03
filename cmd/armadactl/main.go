package main

import (
	"github.com/G-Research/armada/cmd/armadactl/cmd"
	"github.com/G-Research/armada/internal/common"
)

// Config is handled by cmd/params.go
func main() {
	common.ConfigureCommandLineLogging()
	root := cmd.RootCmd()
	// root.execute returns an error, but we can ignore this as the error will already be
	// printed to stderr by cobra
	root.Execute()
}
