package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/jobservice"
	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/pkg/client"
)

func runCmd(app *jobservice.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:  "run",
		RunE: runCmdE(app),
	}

	cmd.Flags().String("config", "", "Configuration")

	return cmd
}

func runCmdE(app *jobservice.App) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		g, ctx := errgroup.WithContext(context.Background())
		var config configuration.JobServiceConfiguration

		configValue, configErr := cmd.Flags().GetString("config")
		if configErr != nil {
			log.Warnf("Error Parsing Config in Startup %v", configErr)
		}
		configArray := strings.Split(configValue, " ")
		common.LoadConfig(&config, "./config/jobservice", configArray)
		config.ApiConnection = *client.ExtractCommandlineArmadaApiConnectionDetails()

		err := app.StartUp(ctx, &config)
		if err != nil {
			panic(err)
		}

		// Cancel the errgroup context on SIGINT and SIGTERM,
		// which shuts everything down gracefully.
		stopSignal := make(chan os.Signal, 1)
		signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)
		g.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case sig := <-stopSignal:
				ctx.Err()
				return fmt.Errorf("received signal %v", sig)
			}
		})
		g.Wait()
		return nil
	}
}
