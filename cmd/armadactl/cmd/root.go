package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/G-Research/armada/internal/client"
)

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.armadactl.yaml)")
	client.AddArmadaApiConnectionCommandlineArgs(rootCmd)
}

var rootCmd = &cobra.Command{
	Use:   "armadactl",
	Short: "Command line utility to manage armada",
	Long: `
Command line utility to manage armada

Persistent config can be saved in a config file so it doesn't have to be specified every command.

Example structure:
armadaUrl: localhost:50051
username: user1
password: password123

The location of this file can be passed in using --config argument or picked from $HOME/.armadactl.yaml.
`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var cfgFile string

func initConfig() {
	client.LoadCommandlineArgsFromConfigFile(cfgFile)
}
