package cmd

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/client"
	"github.com/spf13/cobra"
	"os"
)

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.armadactl.yaml)")
	client.AddArmadaApiConnectionCommandlineArgs(rootCmd)
}

var rootCmd = &cobra.Command{
	Use:   "armada-load-tester command",
	Short: "Command line utility to submit many jobs to armada",
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
