package cmd

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/pkg/client"
)

func init() {
	cobra.OnInitialize(initConfig)
	client.AddArmadaApiConnectionCommandlineArgs(rootCmd)
}

var rootCmd = &cobra.Command{
	Use:   "armada-load-tester command",
	Short: "Command line utility to submit many jobs to armada",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Error(err)
		os.Exit(1)
	}
}

var cfgFile string

func initConfig() {
	if err := client.LoadCommandlineArgsFromConfigFile(cfgFile); err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
