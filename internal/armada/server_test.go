package armada

import (
	"context"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/encoding/gzip"
	"io"
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

		if !assert.Equal(t, 1, len(leasedResponse)) {
			t.FailNow()
		}
		if !assert.Equal(t, jobId, leasedResponse[0].Job.Id) {
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

		assert.Equal(t, 1, len(leasedResponse))
		assert.Equal(t, jobId, leasedResponse[0].Job.Id)

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
		assert.Equal(t, 1, len(leasedResponse))

		cancelResult, err := client.CancelJobs(ctx, &api.JobCancelRequest{JobSetId: "set", Queue: "test"})
		if ok := assert.NoError(t, err); !ok {
			t.FailNow()
		}
		assert.Equal(t, 2, len(cancelResult.CancelledIds))

		renewed, err := leaseClient.RenewLease(ctx, &api.RenewLeaseRequest{
			ClusterId: "test-cluster",
			Ids:       []string{leasedResponse[0].Job.Id},
		})
		if ok := assert.NoError(t, err); !ok {
			t.FailNow()
		}

		assert.Equal(t, 0, len(renewed.Ids))
	})
}

func TestCancelJobSet(t *testing.T) {
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
		assert.Equal(t, 1, len(leasedResponse))

		queuedCount, leasedCount := getQueueStateSummary(ctx, t, client, "test")
		assert.Equal(t, queuedCount, 1)
		assert.Equal(t, leasedCount, 1)

		_, err = client.CancelJobSet(ctx,
			&api.JobSetCancelRequest{
				JobSetId: "set",
				Queue:    "test",
				Filter: &api.JobSetFilter{
					States: []api.JobState{api.JobState_QUEUED},
				},
			})
		assert.NoError(t, err)

		queuedCount, leasedCount = getQueueStateSummary(ctx, t, client, "test")
		assert.Equal(t, queuedCount, 0)
		assert.Equal(t, leasedCount, 1)
	})
}

func TestValidatePreemeptionConfig_AllOk(t *testing.T) {
	config := configuration.PreemptionConfig{
		Enabled: true,
		PriorityClasses: map[string]configuration.PriorityClass{
			"pc1": {
				Priority: 1,
				MaximalResourceFractionPerQueue: map[string]float64{
					"cpu": 1.0,
				},
			},
			"pc2": {
				Priority: 2,
				MaximalResourceFractionPerQueue: map[string]float64{
					"cpu": 0.1,
				},
			},
		},
		DefaultPriorityClass: "pc1",
	}
	assert.NoError(t, validatePreemptionConfig(config))
}

func TestValidatePreemeptionConfig_Failures(t *testing.T) {
	invalidPriorityClass := configuration.PreemptionConfig{
		Enabled: true,
		PriorityClasses: map[string]configuration.PriorityClass{
			"pc1": {
				Priority: 1,
				MaximalResourceFractionPerQueue: map[string]float64{
					"cpu": 1.0,
				},
			},
			"pc2": {
				Priority: 2,
				MaximalResourceFractionPerQueue: map[string]float64{
					"cpu": 0.1,
				},
			},
		},
		DefaultPriorityClass: "this doesn't exist",
	}
	assert.Error(t, validatePreemptionConfig(invalidPriorityClass))

	invalidFesourceFraction := configuration.PreemptionConfig{
		Enabled: true,
		PriorityClasses: map[string]configuration.PriorityClass{
			"pc1": {
				Priority: 1,
				MaximalResourceFractionPerQueue: map[string]float64{
					"cpu": 0.1,
				},
			},
			"pc2": {
				Priority: 2,
				MaximalResourceFractionPerQueue: map[string]float64{
					"cpu": 0.3,
				},
			},
		},
		DefaultPriorityClass: "pc1",
	}
	assert.Error(t, validatePreemptionConfig(invalidFesourceFraction))

	extraResourceType1 := configuration.PreemptionConfig{
		Enabled: true,
		PriorityClasses: map[string]configuration.PriorityClass{
			"pc1": {
				Priority: 1,
				MaximalResourceFractionPerQueue: map[string]float64{
					"cpu": 1.0,
					"gpu": 0.3,
				},
			},
			"pc2": {
				Priority: 2,
				MaximalResourceFractionPerQueue: map[string]float64{
					"cpu": 0.1,
				},
			},
		},
		DefaultPriorityClass: "pc1",
	}
	assert.Error(t, validatePreemptionConfig(extraResourceType1))

	extraResourceType2 := configuration.PreemptionConfig{
		Enabled: true,
		PriorityClasses: map[string]configuration.PriorityClass{
			"pc1": {
				Priority: 1,
				MaximalResourceFractionPerQueue: map[string]float64{
					"cpu": 1.0,
				},
			},
			"pc2": {
				Priority: 2,
				MaximalResourceFractionPerQueue: map[string]float64{
					"cpu": 0.1,
					"gpu": 0.3,
				},
			},
		},
		DefaultPriorityClass: "pc1",
	}
	assert.Error(t, validatePreemptionConfig(extraResourceType2))
}

func leaseJobs(leaseClient api.AggregatedQueueClient, ctx context.Context, availableResource common.ComputeResources) ([]*api.StreamingJobLease, error) {
	nodeResources := common.ComputeResources{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")}

	// Setup a bidirectional gRPC stream.
	// The server sends jobs over this stream.
	// The executor sends back acks to indicate which jobs were successfully received.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := leaseClient.StreamingLeaseJobs(ctx, grpc_retry.Disable(), grpc.UseCompressor(gzip.Name))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// The first message sent over the stream includes all information necessary
	// for the server to choose jobs to lease.
	// Subsequent messages only include ids of received jobs.
	err = stream.Send(&api.StreamingLeaseRequest{
		ClusterId: "test-cluster",
		Resources: availableResource,
		Nodes:     []api.NodeInfo{{Name: "testNode", AllocatableResources: nodeResources, AvailableResources: nodeResources}},
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Goroutine receiving jobs from the server.
	// Also recevies ack confirmations from the server.
	// Send leases on ch to another goroutine responsible for sending back acks.
	// Give the channel a small buffer to allow for some asynchronicity.
	var numServerAcks uint32
	var numJobs uint32
	jobs := make([]*api.StreamingJobLease, 0)
	ch := make(chan *api.StreamingJobLease, 10)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		// Close channel to ensure sending goroutine exits.
		defer close(ch)

		// Exit when until all acks have been confirmed.
		for numServerAcks == 0 || numServerAcks < numJobs {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					return nil
				} else {
					return ctx.Err()
				}
			default:
				res, err := stream.Recv()
				if err == io.EOF {
					return nil
				} else if err != nil {
					return err
				}
				numJobs = res.GetNumJobs()
				numServerAcks = res.GetNumAcked()
				if res.Job != nil {
					jobs = append(jobs, res)
				}
				ch <- res
			}
		}
		return nil
	})

	// Get received jobs on the channel and send back acks.
	g.Go(func() error {
		defer stream.CloseSend()
		for {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					return nil
				} else {
					return ctx.Err()
				}
			case res := <-ch:
				if res == nil {
					return nil // Channel closed.
				}

				// Send ack back to the server.
				if res.Job != nil {
					err := stream.Send(&api.StreamingLeaseRequest{
						ReceivedJobIds: []string{res.Job.Id},
					})
					if err == io.EOF {
						return nil
					} else if err != nil {
						return err
					}
				}
			}
		}
	})

	// Wait for receiver to exit.
	err = g.Wait()
	if err != nil {
		return nil, err
	}

	// If we received confirmation on the ack, we know the server is aware we received the job.
	// For the remaining jobs, return any leases.
	receivedJobs := jobs[:numServerAcks]

	return receivedJobs, nil
}

func getQueueStateSummary(ctx context.Context, t *testing.T, client api.SubmitClient, queue string) (queuedCount int, leasedCount int) {
	queueInfo, err := client.GetQueueInfo(ctx, &api.QueueInfoRequest{Name: queue})
	assert.NoError(t, err)
	queuedCount = 0
	leasedCount = 0

	for _, jobSetInfo := range queueInfo.ActiveJobSets {
		queuedCount += int(jobSetInfo.QueuedJobs)
		leasedCount += int(jobSetInfo.LeasedJobs)
	}
	return queuedCount, leasedCount
}

func SubmitJob(client api.SubmitClient, ctx context.Context, cpu resource.Quantity, memory resource.Quantity, t *testing.T) string {
	request := &api.JobSubmitRequest{
		JobRequestItems: []*api.JobSubmitRequestItem{
			{
				PodSpec: &v1.PodSpec{
					Containers: []v1.Container{
						{
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
	// Make initial lease request to populate cluster node info
	leaseClient := api.NewAggregatedQueueClient(conn)
	_, err = leaseJobs(leaseClient, ctx, common.ComputeResources{})
	if err != nil {
		panic(err)
	}
}
