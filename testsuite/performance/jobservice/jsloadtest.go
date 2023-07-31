package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	jsgrpc "github.com/armadaproject/armada/pkg/api/jobservice"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// TODO: Add arguments to control how the load is applied.
func main() {
	ctx := context.Background()
	wg := sync.WaitGroup{}

	// Launch a jobservice client to query jobservice about a jobset
	conn, err := grpc.Dial("localhost:2000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	client := jsgrpc.NewJobServiceClient(conn)
	healthResp, err := client.Health(ctx, &types.Empty{})
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(healthResp.Status.String())

	prefix := rand.Intn(10000)

	maxJob := 1000
	wg.Add(maxJob)

	for i := 0; i < maxJob; i++ {
		go func(n int) {
			err := queryJobStatus(ctx, conn, n, prefix)
			if err != nil {
				fmt.Printf("Error querying job status: %v\n", err)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
}

func queryJobStatus(ctx context.Context, conn *grpc.ClientConn, n int, prefix int) error {
	client := jsgrpc.NewJobServiceClient(conn)

	resp, err := client.GetJobStatus(ctx, &jsgrpc.JobServiceRequest{
		JobId:    "fake_job_id",
		JobSetId: fmt.Sprintf("%d_new_fake_job_set_id_%d", prefix, n),
		Queue:    "fake_queue",
	})
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	fmt.Printf("%s - %d\n", resp.State.String(), n)
	return nil
}
