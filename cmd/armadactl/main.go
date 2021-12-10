package main

import (
	"log"

	"github.com/G-Research/armada/cmd/armadactl/cmd"
)

// Config is handled by cmd/params.go
func main() {
	root := cmd.RootCmd()
	if err := root.Execute(); err != nil {
		log.Fatal(err)
	}
}
