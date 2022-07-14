package armada

import (
	"context"
	"log"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	authConfiguration "github.com/G-Research/armada/internal/common/auth/configuration"
	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/internal/common/health"
	"github.com/G-Research/armada/pkg/api"
)

func TestSubmitJob_EmptyPodSpec(t *testing.T) {
	withRunningServer(func(client api.SubmitClient, leaseClient api.AggregatedQueueClient, ctx context.Context) {
		_, err := client.CreateQueue(ctx, &api.Queue{
			Name:           "test",
			PriorityFactor: 1,
		})
		if !assert.NoError(t, err) {
			return
		}

		request := &api.JobSubmitRequest{
			JobRequestItems: []*api.JobSubmitRequestItem{{}},
			Queue:           "test",
			JobSetId:        "set",
		}
		_, err = client.SubmitJobs(ctx, request)
		assert.Error(t, err)
	})
}

func TestSubmitJob(t *testing.T) {
	withRunningServer(func(client api.SubmitClient, leaseClient api.AggregatedQueueClient, ctx context.Context) {

		_, err := client.CreateQueue(ctx, &api.Queue{
			Name:           "test",
			PriorityFactor: 1,
		})
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		cpu, _ := resource.ParseQuantity("1")
		memory, _ := resource.ParseQuantity("512Mi")

		jobId := SubmitJob(client, ctx, cpu, memory, t)

		leasedResponse, err := leaseJobs(leaseClient, ctx, common.ComputeResources{"cpu": cpu, "memory": memory})
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		if !assert.Equal(t, 1, len(leasedResponse.Job)) {
			t.FailNow()
		}
		if !assert.Equal(t, jobId, leasedResponse.Job[0].Id) {
			t.FailNow()
		}
	})
}

func TestAutomQueueCreation(t *testing.T) {
	withRunningServer(func(client api.SubmitClient, leaseClient api.AggregatedQueueClient, ctx context.Context) {

		cpu, _ := resource.ParseQuantity("1")
		memory, _ := resource.ParseQuantity("512Mi")

		jobId := SubmitJob(client, ctx, cpu, memory, t)
		leasedResponse, err := leaseJobs(leaseClient, ctx, common.ComputeResources{"cpu": cpu, "memory": memory})
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		assert.Equal(t, 1, len(leasedResponse.Job))
		assert.Equal(t, jobId, leasedResponse.Job[0].Id)

		info, err := client.GetQueueInfo(ctx, &api.QueueInfoRequest{Name: "test"})
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		assert.Equal(t, "set", info.ActiveJobSets[0].Name)
	})
}

func TestCancelJob(t *testing.T) {
	withRunningServer(func(client api.SubmitClient, leaseClient api.AggregatedQueueClient, ctx context.Context) {

		_, err := client.CreateQueue(ctx, &api.Queue{
			Name:           "test",
			PriorityFactor: 1,
		})
		if ok := assert.NoError(t, err); !ok {
			t.FailNow()
		}

		cpu, _ := resource.ParseQuantity("1")
		memory, _ := resource.ParseQuantity("512Mi")

		SubmitJob(client, ctx, cpu, memory, t)
		SubmitJob(client, ctx, cpu, memory, t)

		leasedResponse, err := leaseJobs(leaseClient, ctx, common.ComputeResources{"cpu": cpu, "memory": memory})
		if ok := assert.NoError(t, err); !ok {
			t.FailNow()
		}
		assert.Equal(t, 1, len(leasedResponse.Job))

		cancelResult, err := client.CancelJobs(ctx, &api.JobCancelRequest{JobSetId: "set", Queue: "test"})
		if ok := assert.NoError(t, err); !ok {
			t.FailNow()
		}
		assert.Equal(t, 2, len(cancelResult.CancelledIds))

		renewed, err := leaseClient.RenewLease(ctx, &api.RenewLeaseRequest{
			ClusterId: "test-cluster",
			Ids:       []string{leasedResponse.Job[0].Id},
		})
		if ok := assert.NoError(t, err); !ok {
			t.FailNow()
		}

		assert.Equal(t, 0, len(renewed.Ids))
	})
}

func leaseJobs(leaseClient api.AggregatedQueueClient, ctx context.Context, availableResource common.ComputeResources) (*api.JobLease, error) {
	nodeResources := common.ComputeResources{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")}
	return leaseClient.LeaseJobs(ctx, &api.LeaseRequest{
		ClusterId: "test-cluster",
		Resources: availableResource,
		Nodes:     []api.NodeInfo{{Name: "testNode", AllocatableResources: nodeResources, AvailableResources: nodeResources}},
	})
}

func SubmitJob(client api.SubmitClient, ctx context.Context, cpu resource.Quantity, memory resource.Quantity, t *testing.T) string {
	request := &api.JobSubmitRequest{
		JobRequestItems: []*api.JobSubmitRequestItem{
			{
				PodSpec: &v1.PodSpec{
					Containers: []v1.Container{{
						Name:  "Container1",
						Image: "index.docker.io/library/ubuntu:latest",
						Args:  []string{"sleep", "10s"},
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
							Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
						},
					},
					},
				},
				Priority: 0,
			},
		},
		Queue:    "test",
		JobSetId: "set",
	}
	response, err := client.SubmitJobs(ctx, request)
	assert.NoError(t, err)
	return response.JobResponseItems[0].JobId
}

func withRunningServer(action func(client api.SubmitClient, leaseClient api.AggregatedQueueClient, ctx context.Context)) {
	minidb, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	// minidb.Close hangs indefinitely (likely due to a bug in miniredis)
	// defer minidb.Close()

	// cleanup prometheus in case there are registered metrics already present
	prometheus.DefaultRegisterer = prometheus.NewRegistry()

	// get free port
	l, _ := net.Listen("tcp", ":0")
	l.Close()
	freeAddress := l.Addr().String()
	parts := strings.Split(freeAddress, ":")
	port, _ := strconv.Atoi(parts[len(parts)-1])

	healthChecks := health.NewMultiChecker()

	ctx, shutdown := context.WithCancel(context.Background())
	defer shutdown()
	go func() {
		err := Serve(
			ctx,
			&configuration.ArmadaConfig{
				Auth: authConfiguration.AuthConfig{
					AnonymousAuth: true,
					PermissionGroupMapping: map[permission.Permission][]string{
						permissions.ExecuteJobs:    {authorization.EveryoneGroup},
						permissions.SubmitJobs:     {authorization.EveryoneGroup},
						permissions.SubmitAnyJobs:  {authorization.EveryoneGroup},
						permissions.CreateQueue:    {authorization.EveryoneGroup},
						permissions.CancelJobs:     {authorization.EveryoneGroup},
						permissions.CancelAnyJobs:  {authorization.EveryoneGroup},
						permissions.WatchEvents:    {authorization.EveryoneGroup},
						permissions.WatchAllEvents: {authorization.EveryoneGroup},
					},
				},
				GrpcPort: uint16(port),
				Redis: redis.UniversalOptions{
					Addrs: []string{minidb.Addr()},
					DB:    0,
				},
				CancelJobsBatchSize: 200,
				Scheduling: configuration.SchedulingConfig{
					QueueLeaseBatchSize:          100,
					MaximumLeasePayloadSizeBytes: 7 * 1024 * 1024,
					MaximumJobsToSchedule:        1000,
					Lease: configuration.LeaseSettings{
						ExpireAfter:        time.Minute * 15,
						ExpiryLoopInterval: time.Second * 5,
					},
					MaxPodSpecSizeBytes: 65535,
				},
				QueueManagement: configuration.QueueManagementConfig{
					AutoCreateQueues:      true,
					DefaultPriorityFactor: 1000,
				},
			},
			healthChecks,
		)
		if err != nil {
			log.Fatalf("failed to start server: %v", err)
		}
	}()

	// Wait for the server to come up
	err = healthChecks.Check()
	for err != nil {
		time.Sleep(100 * time.Millisecond)
		err = healthChecks.Check()
	}

	conn, err := grpc.Dial(freeAddress, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	setupServer(conn)
	client := api.NewSubmitClient(conn)
	leaseClient := api.NewAggregatedQueueClient(conn)

	action(client, leaseClient, context.Background())
}

func setupServer(conn *grpc.ClientConn) {
	ctx := context.Background()
	usageReport := &api.ClusterUsageReport{
		ClusterId:                "test-cluster",
		ReportTime:               time.Now(),
		Queues:                   []*api.QueueReport{},
		ClusterCapacity:          map[string]resource.Quantity{"cpu": resource.MustParse("100"), "memory": resource.MustParse("100Gi")},
		ClusterAvailableCapacity: map[string]resource.Quantity{"cpu": resource.MustParse("100"), "memory": resource.MustParse("100Gi")},
	}
	usageClient := api.NewUsageClient(conn)
	_, err := usageClient.ReportUsage(ctx, usageReport)
	if err != nil {
		panic(err)
	}
	//Make initial lease request to populate cluster node info
	leaseClient := api.NewAggregatedQueueClient(conn)
	_, err = leaseJobs(leaseClient, ctx, common.ComputeResources{})
	if err != nil {
		panic(err)
	}
}
