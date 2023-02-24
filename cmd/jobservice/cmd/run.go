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

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/jobservice"
	"github.com/armadaproject/armada/internal/jobservice/configuration"
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
			log.Warnf("error parsing config in startup %v", configErr)
		}
		configArray := strings.Split(configValue, " ")
		common.LoadConfig(&config, "./config/jobservice", configArray)

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
				if err := ctx.Err(); err != nil {
					log.Warnf("error from stopping %v", err)
				}
				return fmt.Errorf("received signal %v", sig)
			}
		})
		return g.Wait()
	}
}
