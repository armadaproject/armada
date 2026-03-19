package cmd

import (
	"context"
	"os"
	"fmt"
	"errors"
	"time"

	introspectionapi "github.com/armadaproject/armada/pkg/api/introspection"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func kubectlCacheDescribeNodeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use: "node <job-id|run-id>",
		Short: "Describe node for a job (job id by default; run id via --run-id)",
		Args:  cobra.MaximumNArgs(1),
		RunE: runKubectlCacheDescribeNode,
	}

	cmd.Flags().StringVar(&describeAddr, "addr", "", "armada api address (env ARMADA_API_ADDR used if empty)")
	cmd.Flags().IntVar(&describeTimeout, "timeout", 10, "rpc timeout seconds")

	cmd.Flags().StringVarP(&describeJobID, "job-id", "j", "", "job id (optional; positional argument is treated as job id if no flags are set)")

	cmd.Flags().StringVarP(&describeRunID, "run-id", "r", "", "job run id (optional; takes precedence over positional argument)")

	return cmd
}

func runKubectlCacheDescribeNode(cmd *cobra.Command, args []string) error {
	addr := describeAddr
	if addr == "" {
		addr = os.Getenv("ARMADA_API_ADDR")
	}
	if addr == "" {
		// 2) fall back to global --armadaUrl (default is usually localhost:50051)
		if v, err := cmd.Flags().GetString("armadaUrl"); err == nil && v != "" {
			addr = v
		}
		if addr == "" {
			if v, err := cmd.InheritedFlags().GetString("armadaUrl"); err == nil && v != "" {
				addr = v
			}
		}
	}
	if addr == "" {
		addr = "localhost:50051"
	}

	runID := describeRunID
	jobID := describeJobID

	if runID == "" && jobID == "" && len(args) > 0 {
		jobID = args[0]
	}

	if runID == "" && jobID == "" {
		return errors.New("must provide either --job-id/positional arg, or --run-id")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(describeTimeout)*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	client := introspectionapi.NewIntrospectionClient(conn)

	var resp *introspectionapi.CachedKubectlDescribeResponse
		resp, err = client.CachedKubectlDescribeNode(ctx, &introspectionapi.CachedKubectlDescribeNodeRequest{
			JobId: jobID,
			RunId: runID,
		})
	if err != nil {
		return err
	}

	fmt.Printf("Cached at: %s\n\n%s\n", time.Unix(resp.CachedAt.Seconds, int64(resp.CachedAt.Nanos)).Format(time.RFC3339), resp.Output)
	return nil
}