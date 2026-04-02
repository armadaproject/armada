package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	introspectionapi "github.com/armadaproject/armada/pkg/api/introspection"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var describePodCluster string
var describePodIncludeRaw bool
var describeIncludeEvents bool

func describePodCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pod <job-id>",
		Short: "Describe pod for a job (job id by default; run id via --run-id)",
		Args:  cobra.MaximumNArgs(1),
		RunE:  runDescribePod,
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

func runDescribePod(cmd *cobra.Command, args []string) error {
	addr := describeAddr
	if addr == "" {
		addr = os.Getenv("ARMADA_API_ADDR")
	}
	if addr == "" {
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
	req := &introspectionapi.DescribeJobPodRequest{
		JobId:         jobID,
		RunId:         runID,
		Cluster:       describePodCluster,
		IncludeRaw:    describePodIncludeRaw,
		IncludeEvents: describeIncludeEvents,
	}
	resp, err := client.KubeDescribeJobPod(ctx, req)
	if err != nil {
		return err
	}

	out, _ := json.MarshalIndent(resp, "", "  ")
	fmt.Println(string(out))
	return nil
}
