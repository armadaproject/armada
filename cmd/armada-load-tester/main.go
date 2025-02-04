package main

import (
	"github.com/armadaproject/armada/cmd/armada-load-tester/cmd"
	"github.com/armadaproject/armada/internal/common/logging"
)

func main() {
	logging.ConfigureCliLogging()
	cmd.Execute()
}
