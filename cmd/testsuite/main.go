package main

import (
	"log"

	"github.com/G-Research/armada/cmd/testsuite/cmd"
	"github.com/G-Research/armada/internal/common"
)

// Config is handled by cmd/params.go
func main() {
	common.ConfigureCommandLineLogging()
	root := cmd.RootCmd()
	if err := root.Execute(); err != nil {
		log.Fatal(err)
	}
}
