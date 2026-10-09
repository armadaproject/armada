package main

import (
	"github.com/armadaproject/armada/cmd/regatta/cmd"
	"github.com/armadaproject/armada/internal/common/logging"
)

func main() {
	logging.ConfigureCliLogging()
	cmd.Execute()
}
