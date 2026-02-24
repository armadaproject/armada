package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	introspectionapi "github.com/armadaproject/armada/pkg/api/introspection"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var describeAddr string
var describeTimeout int

func describeCmd() *cobra.Command {
    cmd := &cobra.Command{
        Use:   "describe job <run-id>",
        Short: "Describe node for job run",
        Args:  cobra.ExactArgs(1),
        RunE:  runDescribeJob,
    }

    cmd.Flags().StringVar(&describeAddr, "addr", "", "armada api address (env ARMADA_API_ADDR used if empty)")
    cmd.Flags().IntVar(&describeTimeout, "timeout", 10, "rpc timeout seconds")

    return cmd
}

func runDescribeJob(cmd *cobra.Command, args []string) error {
	addr := describeAddr
	if addr == "" {
		if env := cmd.Flags().Lookup("addr"); env == nil {
			// fallback to env var if you like
			addr = "localhost:7464"
		}
	}
	runId := args[0]

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(describeTimeout)*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(
    	addr,
    	grpc.WithTransportCredentials(insecure.NewCredentials()),
	)	
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	client := introspectionapi.NewIntrospectionClient(conn)
	resp, err := client.DescribeNodeByJobRun(ctx, &introspectionapi.DescribeNodeByJobRunRequest{RunId: runId})
	if err != nil {
		return err
	}
	out, _ := json.MarshalIndent(resp, "", "  ")
	fmt.Println(string(out))
	return nil
}