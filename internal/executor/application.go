package executor

import (
	"os"
	"sync"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/common/client"
	"github.com/G-Research/armada/internal/executor/cluster"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/metrics"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/service"
)

func StartUp(config configuration.ExecutorConfiguration) (func(), *sync.WaitGroup) {

	kubernetesClientProvider, err := cluster.NewKubernetesClientProvider(&config.Kubernetes)

	if err != nil {
		log.Errorf("Failed to connect to kubernetes because %s", err)
		os.Exit(-1)
	}

	clusterContext := context.NewClusterContext(
		config.Application.ClusterId,
		2*time.Minute,
		kubernetesClientProvider)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopSignals := make([]chan bool, 0)
	stopSignals = append(stopSignals, scheduleBackgroundTask(clusterContext.ProcessPodsToDelete, config.Task.PodDeletionInterval, "pod_deletion", wg))

	return StartUpWithContext(config, clusterContext, stopSignals, wg)
}

func StartUpWithContext(config configuration.ExecutorConfiguration, clusterContext context.ClusterContext, stopSignals []chan bool, wg *sync.WaitGroup) (func(), *sync.WaitGroup) {

	conn, err := createConnectionToApi(config)
	if err != nil {
		log.Errorf("Failed to connect to API because: %s", err)
		os.Exit(-1)
	}

	queueClient := api.NewAggregatedQueueClient(conn)
	usageClient := api.NewUsageClient(conn)
	eventClient := api.NewEventClient(conn)

	eventReporter := reporter.NewJobEventReporter(
		clusterContext,
		eventClient)

	jobLeaseService := service.NewJobLeaseService(
		clusterContext,
		queueClient)

	clusterUtilisationService := service.NewClusterUtilisationService(
		clusterContext,
		usageClient,
		config.Kubernetes.TrackedNodeLabels)

	stuckPodDetector := service.NewPodProgressMonitorService(
		clusterContext,
		eventReporter,
		jobLeaseService)

	clusterAllocationService := service.NewClusterAllocationService(
		clusterContext,
		eventReporter,
		jobLeaseService,
		clusterUtilisationService)

	stopSignals = append(stopSignals, scheduleBackgroundTask(clusterUtilisationService.ReportClusterUtilisation, config.Task.UtilisationReportingInterval, "utilisation_reporting", wg))
	stopSignals = append(stopSignals, scheduleBackgroundTask(clusterAllocationService.AllocateSpareClusterCapacity, config.Task.AllocateSpareClusterCapacityInterval, "job_lease_request", wg))
	stopSignals = append(stopSignals, scheduleBackgroundTask(jobLeaseService.ManageJobLeases, config.Task.JobLeaseRenewalInterval, "job_lease_renewal", wg))
	stopSignals = append(stopSignals, scheduleBackgroundTask(eventReporter.ReportMissingJobEvents, config.Task.MissingJobEventReconciliationInterval, "event_reconciliation", wg))
	stopSignals = append(stopSignals, scheduleBackgroundTask(stuckPodDetector.HandleStuckPods, config.Task.StuckPodScanInterval, "stuck_pod", wg))

	return func() {
		stopTasks(stopSignals)
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
	return client.CreateApiConnection(&config.ApiConnection,
		grpc.WithChainUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(grpc_prometheus.StreamClientInterceptor))
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
