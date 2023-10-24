package main

import (
	"fmt"
	"os"

	"github.com/armadaproject/armada/cmd/simulator/cmd"
)

func main() {
	if err := cmd.RootCmd().Execute(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
