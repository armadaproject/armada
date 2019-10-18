package main

import (
	"github.com/G-Research/armada/cmd/armada-load-tester/cmd"
	"github.com/G-Research/armada/internal/common"
)

func main() {
	common.ConfigureLogging()
	cmd.Execute()
}
