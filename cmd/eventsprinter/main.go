package main

import (
	"fmt"

	"github.com/armadaproject/armada/cmd/eventsprinter/cmd"
)

func main() {
	root := cmd.RootCmd()
	if err := root.Execute(); err != nil {
		fmt.Println(err)
	}
}
