package main

import (
	"github.com/G-Research/armada/cmd/armadactl/cmd"
	"github.com/G-Research/armada/internal/common"
)

// Config is handled by cmd/params.go
func main() {
	common.ConfigureCommandLineLogging() // TODO: can be removed once all of armadactl prints to its internal io.Writer
	cmd.Execute()
}

// package main

// import (
// 	log "github.com/sirupsen/logrus"

// 	"github.com/G-Research/armada/cmd/armadactl/cmd"
// 	"github.com/G-Research/armada/internal/common"
// )

// func main() {
// 	common.ConfigureCommandLineLogging()
// 	root := cmd.RootCmd()
// 	if err := root.Execute(); err != nil {
// 		log.Fatal(err)
// 	}
// }
