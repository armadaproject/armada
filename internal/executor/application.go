package executor

import (
	"os"
	"sync"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/oidc"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/metrics"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/service"
)

func StartUp(config configuration.ExecutorConfiguration) (func(), *sync.WaitGroup) {
	kubernetesClient, err := CreateKubernetesClient(&config.Kubernetes)
	if err != nil {
		log.Errorf("Failed to connect to kubernetes because %s", err)
		os.Exit(-1)
	}

	conn, err := createConnectionToApi(config)
	if err != nil {
		log.Errorf("Failed to connect to API because: %s", err)
		os.Exit(-1)
	}

	queueClient := api.NewAggregatedQueueClient(conn)
	usageClient := api.NewUsageClient(conn)
	eventClient := api.NewEventClient(conn)

	clusterContext := context.NewClusterContext(
		config.Application.ClusterId,
		2*time.Minute,
		kubernetesClient,
	)

	eventReporter := reporter.NewJobEventReporter(
		clusterContext,
		eventClient)

	jobLeaseService := service.NewJobLeaseService(
		clusterContext,
		queueClient)

	clusterUtilisationService := service.NewClusterUtilisationService(
		clusterContext,
		usageClient)

	stuckPodDetector := service.NewPodProgressMonitorService(
		clusterContext,
		eventReporter,
		jobLeaseService)

	clusterAllocationService := service.NewClusterAllocationService(
		clusterContext,
		jobLeaseService,
		clusterUtilisationService)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	tasks := make([]chan bool, 0)
	tasks = append(tasks, scheduleBackgroundTask(clusterUtilisationService.ReportClusterUtilisation, config.Task.UtilisationReportingInterval, "utilisation_reporting", wg))
	tasks = append(tasks, scheduleBackgroundTask(clusterAllocationService.AllocateSpareClusterCapacity, config.Task.AllocateSpareClusterCapacityInterval, "job_lease_request", wg))
	tasks = append(tasks, scheduleBackgroundTask(jobLeaseService.ManageJobLeases, config.Task.JobLeaseRenewalInterval, "job_lease_renewal", wg))
	tasks = append(tasks, scheduleBackgroundTask(eventReporter.ReportMissingJobEvents, config.Task.MissingJobEventReconciliationInterval, "event_reconciliation", wg))
	tasks = append(tasks, scheduleBackgroundTask(stuckPodDetector.HandleStuckPods, config.Task.StuckPodScanInterval, "stuck_pod", wg))
	tasks = append(tasks, scheduleBackgroundTask(clusterContext.ProcessPodsToDelete, config.Task.PodDeletionInterval, "pod_deletion", wg))

	return func() {
		stopTasks(tasks)
		clusterContext.Stop()
		conn.Close()
		wg.Done()
		if waitForShutdownCompletion(wg, 2*time.Second) {
			log.Warnf("Graceful shutdown timed out")
		}
		log.Infof("Shutdown complete")
	}, wg
}

func createConnectionToApi(config configuration.ExecutorConfiguration) (*grpc.ClientConn, error) {
	dialOpts := make([]grpc.DialOption, 0, 10)

	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(300 * time.Millisecond)),
		grpc_retry.WithMax(3),
	}

	defaultCallOptions := grpc.WithDefaultCallOptions(grpc.WaitForReady(true))

	unuaryInterceptors := grpc.WithChainUnaryInterceptor(
		grpc_prometheus.UnaryClientInterceptor,
		grpc_retry.UnaryClientInterceptor(retryOpts...),
	)

	streamInterceptors := grpc.WithChainStreamInterceptor(
		grpc_prometheus.StreamClientInterceptor,
		grpc_retry.StreamClientInterceptor(retryOpts...),
	)

	dialOpts = append(dialOpts, defaultCallOptions, unuaryInterceptors, streamInterceptors)

	if config.BasicAuth.EnableAuthentication {
		dialOpts = append(dialOpts,
			grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")),
			grpc.WithPerRPCCredentials(&common.LoginCredentials{
				Username: config.BasicAuth.Username,
				Password: config.BasicAuth.Password,
			}),
		)
	} else if config.OpenIdPasswordAuth.ProviderUrl != "" {
		tokenCredentials, err := oidc.AuthenticateWithPassword(config.OpenIdPasswordAuth)
		if err != nil {
			return nil, err
		}
		dialOpts = append(dialOpts,
			grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")),
			grpc.WithPerRPCCredentials(tokenCredentials),
		)
	} else if config.OpenIdClientCredentialsAuth.ProviderUrl != "" {
		tokenCredentials, err := oidc.AuthenticateWithClientCredentials(config.OpenIdClientCredentialsAuth)
		if err != nil {
			return nil, err
		}
		dialOpts = append(dialOpts,
			grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")),
			grpc.WithPerRPCCredentials(tokenCredentials),
		)
	} else {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	}

	return grpc.Dial(config.Armada.Url, dialOpts...)
}

func waitForShutdownCompletion(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func scheduleBackgroundTask(task func(), interval time.Duration, metricName string, wg *sync.WaitGroup) chan bool {
	stop := make(chan bool)

	var taskDurationHistogram = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    metrics.ArmadaExecutorMetricsPrefix + metricName + "_latency_seconds",
			Help:    "Background loop " + metricName + " latency in seconds",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
		})

	wg.Add(1)
	go func() {
		start := time.Now()
		task()
		duration := time.Since(start)
		taskDurationHistogram.Observe(duration.Seconds())

		for {
			select {
			case <-time.After(interval):
			case <-stop:
				wg.Done()
				return
			}
			innerStart := time.Now()
			task()
			innerDuration := time.Since(innerStart)
			taskDurationHistogram.Observe(innerDuration.Seconds())
		}
	}()

	return stop
}

func stopTasks(taskChannels []chan bool) {
	for _, channel := range taskChannels {
		channel <- true
	}
}
