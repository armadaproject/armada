package main

import (
	"os"

	"github.com/armadaproject/armada/cmd/simulator/cmd"
)

func main() {
	if err := cmd.RootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
