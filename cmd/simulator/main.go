package main

import (
	"fmt"
	"github.com/armadaproject/armada/cmd/simulator/cmd"
	"os"
)

func main() {
	if err := cmd.RootCmd().Execute(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
