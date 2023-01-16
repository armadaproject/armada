package main

import (
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/cmd/jobservice/cmd"
	"github.com/armadaproject/armada/internal/common"
)

func main() {
	common.ConfigureCommandLineLogging()
	root := cmd.RootCmd()
	if err := root.Execute(); err != nil {
		log.Fatal(err)
	}
}
