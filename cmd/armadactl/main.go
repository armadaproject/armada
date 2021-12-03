package main

import (
	"github.com/G-Research/armada/cmd/armadactl/cmd"
	"github.com/G-Research/armada/internal/common"
)

// Config is handled by cmd/params.go
func main() {
	common.ConfigureCommandLineLogging() // TODO: can be removed once all of armadactl prints to its internal io.Writer
	cmd.Execute()
}
