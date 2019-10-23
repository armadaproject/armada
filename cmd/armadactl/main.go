package main

import (
	"github.com/G-Research/armada/cmd/armadactl/cmd"
	"github.com/G-Research/armada/internal/common"
)

func main() {
	common.ConfigureCommandLineLogging()
	cmd.Execute()
}
