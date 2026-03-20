package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func nativeDescribePodCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use: "pod <job-id>",
		Short: "Describe pod for specific job using internal Armada data (job id)",
		Args: cobra.MaximumNArgs(1),
		RunE: runNativeDescribePod,
	}

	cmd.Flags().StringVar(&describeAddr, "addr", "", "armada api address (env ARMADA_API_ADDR used if empty)")
	cmd.Flags().IntVar(&describeTimeout, "timeout", 10, "rpc timeout seconds")

	cmd.Flags().StringVarP(&describeJobID, "job-id", "j", "", "job id (optional; positional argument is treated as job id if no flags are set)")

	return cmd
}

func runNativeDescribePod(cmd *cobra.Command, args []string) error {
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

	jobID := describeJobID

	if jobID == "" && len(args) > 0 {
		jobID = args[0]
	}

	if jobID == "" {
		return errors.New("must provide --job-id/positional")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(describeTimeout)*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	client := api.NewJobsClient(conn)
	resp, err := client.GetJobDetails(ctx, &api.JobDetailsRequest{
		JobIds:       []string{jobID},
		ExpandJobRun: true,
	})
	if err != nil {
		return err
	}

	details := resp.JobDetails[jobID]
	if details == nil {
		return fmt.Errorf("job %q not found", jobID)
	}

	errResp, err := client.GetJobErrors(ctx, &api.JobErrorsRequest{JobIds: []string{jobID}})
	if err != nil {
		return err
	}

	out, _ := json.MarshalIndent(details, "", "  ")
	fmt.Println(string(out))
	fmt.Printf("state:  %s\n", api.JobState_name[int32(details.State)])
	if reason, ok := errResp.JobErrors[jobID]; ok && reason != "" {
		fmt.Printf("reason: %s\n", reason)
	}

	return nil
}