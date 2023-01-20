package main

import (
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/cmd/pulsartest/cmd"
	"github.com/armadaproject/armada/internal/common"
)

// Config is handled by cmd/params.go
func main() {
	common.ConfigureCommandLineLogging()
	root := cmd.RootCmd()
	if err := root.Execute(); err != nil {
		log.Fatal(err)
	}
}
