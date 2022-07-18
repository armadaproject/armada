package main

import (
	"log"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/cmd/jobservice/cmd"
)

const CustomConfigLocation string = "config"

func main() {
	common.ConfigureCommandLineLogging()
	root := cmd.RootCmd()
	if err := root.Execute(); err != nil {
		log.Fatal(err)
	}
}
