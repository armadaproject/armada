package cmd

import (
	"os"

	"github.com/spf13/cobra"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/pkg/client"
)

func init() {
	cobra.OnInitialize(initConfig)
	client.AddArmadaApiConnectionCommandlineArgs(rootCmd)
}

var rootCmd = &cobra.Command{
	Use:   "regatta command",
	Short: "Armada performance-testing harness",
	Long: `
Regatta drives benchmarking tests against Armada, optionally standing up KWOK
fake nodes so scenarios can be run on simulated hardware.

Persistent armada config can be passed in using --config argument or picked from $HOME/.armadactl.yaml.`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
}

var cfgFile string

func initConfig() {
	if err := client.LoadCommandlineArgsFromConfigFile(cfgFile); err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
}
