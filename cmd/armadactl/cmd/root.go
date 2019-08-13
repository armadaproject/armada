package cmd

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"os"
)

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.armadactl.yaml)")
	rootCmd.PersistentFlags().String("armadaUrl", "localhost:50051", "specify armada server url")
	rootCmd.PersistentFlags().String("username", "", "username to connect to armada server")
	rootCmd.PersistentFlags().String("password", "", "password to connect to armada server")
	viper.BindPFlag("armadaUrl", rootCmd.PersistentFlags().Lookup("armadaUrl"))
	viper.BindPFlag("username", rootCmd.PersistentFlags().Lookup("username"))
	viper.BindPFlag("password", rootCmd.PersistentFlags().Lookup("password"))
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

func withConnection(action func(*grpc.ClientConn)) {
	url := viper.GetString("armadaUrl")
	username := viper.GetString("username")
	password := viper.GetString("password")

	conn, err := createConnection(url, username, password)

	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	action(conn)
}

func createConnection(url string, username string, password string) (*grpc.ClientConn, error) {
	if username == "" || password == "" {
		return grpc.Dial(url, grpc.WithInsecure())
	} else {
		return grpc.Dial(
			url,
			grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")),
			grpc.WithPerRPCCredentials(&common.LoginCredentials{
				Username: username,
				Password: password,
			}))
	}
}

var cfgFile string

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		viper.AddConfigPath(home)
		viper.SetConfigName(".armadactl")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	err := viper.ReadInConfig()

	if err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	} else {
		fmt.Println("Can't read config:", err)
		os.Exit(1)
	}
}
