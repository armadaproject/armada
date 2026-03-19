package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	introspectionapi "github.com/armadaproject/armada/pkg/api/introspection"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func kubectlCacheDescribePodCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use: "pod <job-id|run-id>",
		Short: "Describe pod for specific job (job id by default: run id via --run-id)",
		Args: cobra.MaximumNArgs(1),
		RunE: runKubectlCacheDescribePod,
	}

	cmd.Flags().StringVar(&describeAddr, "addr", "", "armada api address (env ARMADA_API_ADDR used if empty)")
	cmd.Flags().IntVar(&describeTimeout, "timeout", 10, "rpc timeout seconds")

	cmd.Flags().StringVarP(&describeJobID, "job-id", "j", "", "job id (optional; positional argument is treated as job id if no flags are set)")

	cmd.Flags().StringVarP(&describeRunID, "run-id", "r", "", "job run id (optional; takes precedence over positional argument)")
	
	cmd.Flags().StringVar(&describePodCluster, "cluster", "", "cluster name (optional; derived from run details if omitted)")

	cmd.Flags().BoolVar(&describeIncludeEvents, "include-events", false, "include events in pod summary")

	cmd.Flags().BoolVar(&describePodIncludeRaw, "include-raw", false, "include raw pod JSON in response")
	return cmd
}

func runKubectlCacheDescribePod(cmd *cobra.Command, args []string) error {
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

	jobID := describeJobID
	if jobID == "" {
		return errors.New("must provide --job-id or positional argument")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(describeTimeout)*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	client := introspectionapi.NewIntrospectionClient(conn)
	req := &introspectionapi.CachedKubectlDescribeJobPodRequest{
		JobId: jobID,
	}

	resp, err := client.CachedKubectlDescribePod(ctx, req)
	if err!= nil {
		return err
	}

	fmt.Printf("Cached at: %s\n\n%s\n", time.Unix(resp.CachedAt.Seconds, int64(resp.CachedAt.Nanos)).Format(time.RFC3339), resp.Output)
	return nil
}