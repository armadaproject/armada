package cmd

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/G-Research/armada/pkg/client"
)

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.armadactl.yaml)")
	client.AddArmadaApiConnectionCommandlineArgs(rootCmd)
}

var rootCmd = &cobra.Command{
	Use:   "armadactl",
	Short: "armadactl controls the Armada batch job queueing system.",
	Long: `
armadactl controls the Armada batch job queueing system.

Persistent config can be saved in a config file so it doesn't have to be specified every command.

Example structure:
armadaUrl: localhost:50051
basicAuth:
  username: user1
  password: password123

The location of this file can be passed in using --config argument or picked from $HOME/.armadactl.yaml.
`,
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
	client.LoadCommandlineArgsFromConfigFile(cfgFile)
}

func exitWithError(e error) {
	log.Error(e)
	os.Exit(1)
}
