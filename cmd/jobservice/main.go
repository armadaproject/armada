package main

import (
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/cmd/jobservice/cmd"
	"github.com/G-Research/armada/internal/common"
)

func main() {
	common.ConfigureCommandLineLogging()
	root := cmd.RootCmd()
	if err := root.Execute(); err != nil {
		log.Fatal(err)
	}
}
