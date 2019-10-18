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
	Use:   "armadaclt command",
	Short: "Command line utility to manage armada",
	Long:  ``,
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
