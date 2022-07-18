package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/jobservice"
	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/pkg/client"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func runCmd(app *jobservice.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test",
		Short: "Test an Armada deployment by submitting jobs and watching for expected events.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, app)
		},
		RunE: runCmdE(app),
	}

	return cmd
}
func runCmdE(app *jobservice.App) func(cmd *cobra.Command, args []string) error {	common.ConfigureLogging()
	common.BindCommandlineArguments()
	g, ctx := errgroup.WithContext(context.Background())

	var config configuration.JobServiceConfiguration
	config.ApiConnection = *client.ExtractCommandlineArmadaApiConnectionDetails()
	common.LoadConfig(&config, "./config/jobservice", []string{})

	shutdown, wg := app.StartUp()

	// Cancel the errgroup context on SIGINT and SIGTERM,
	// which shuts everything down gracefully.
	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)
	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		case sig := <-stopSignal:
			wg.Done()
			shutdown()
			return fmt.Errorf("received signal %v", sig)
		}
	})

	wg.Wait()
	return nil
}
