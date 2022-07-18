package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/jobservice"
	"github.com/G-Research/armada/pkg/client"
)

func runCmd(app *jobservice.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test",
		Short: "Test an Armada deployment by submitting jobs and watching for expected events.",
		RunE:  runCmdE(app),
	}

	return cmd
}
func runCmdE(app *jobservice.App) func(cmd *cobra.Command, args []string) error {
	g, ctx := errgroup.WithContext(context.Background())
	app.Config.ApiConnection = *client.ExtractCommandlineArmadaApiConnectionDetails()

	err := app.StartUp(ctx)
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
