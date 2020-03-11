package executor

import (
	"os"
	"sync"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	metrics_server "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/G-Research/armada/internal/common/task"
	"github.com/G-Research/armada/internal/executor/cluster"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/metrics"
	"github.com/G-Research/armada/internal/executor/metrics/pod_metrics"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/service"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
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

	taskManager := task.NewBackgroundTaskManager(metrics.ArmadaExecutorMetricsPrefix)
	taskManager.Register(clusterContext.ProcessPodsToDelete, config.Task.PodDeletionInterval, "pod_deletion")

	return StartUpWithContext(config, clusterContext, kubernetesClientProvider, taskManager, wg)
}

func StartUpWithContext(config configuration.ExecutorConfiguration, clusterContext context.ClusterContext, kubernetesClientProvider cluster.KubernetesClientProvider, taskManager *task.BackgroundTaskManager, wg *sync.WaitGroup) (func(), *sync.WaitGroup) {

	conn, err := createConnectionToApi(config)
	if err != nil {
		log.Errorf("Failed to connect to API because: %s", err)
		os.Exit(-1)
	}

	var metricsServerClient *metrics_server.Clientset
	if config.Metric.ExposeQueueUsageMetrics && kubernetesClientProvider != nil {
		metricsServerClient, err = metrics_server.NewForConfig(kubernetesClientProvider.ClientConfig())
		if err != nil {
			log.Errorf("Failed to connect to metrics server because: %s", err)
			os.Exit(-1)
		}
	}

	queueClient := api.NewAggregatedQueueClient(conn)
	usageClient := api.NewUsageClient(conn)
	eventClient := api.NewEventClient(conn)

	eventReporter, stopReporter := reporter.NewJobEventReporter(
		clusterContext,
		eventClient)

	jobLeaseService := service.NewJobLeaseService(
		clusterContext,
		queueClient,
		config.Kubernetes.MinimumPodAge,
		config.Kubernetes.FailedPodExpiry)

	queueUtilisationService := service.NewMetricsServerQueueUtilisationService(
		clusterContext,
		metricsServerClient)

	clusterUtilisationService := service.NewClusterUtilisationService(
		clusterContext,
		queueUtilisationService,
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

	contextMetrics := pod_metrics.NewClusterContextMetrics(clusterContext, clusterUtilisationService, queueUtilisationService)

	taskManager.Register(clusterUtilisationService.ReportClusterUtilisation, config.Task.UtilisationReportingInterval, "utilisation_reporting")
	taskManager.Register(clusterAllocationService.AllocateSpareClusterCapacity, config.Task.AllocateSpareClusterCapacityInterval, "job_lease_request")
	taskManager.Register(jobLeaseService.ManageJobLeases, config.Task.JobLeaseRenewalInterval, "job_lease_renewal")
	taskManager.Register(eventReporter.ReportMissingJobEvents, config.Task.MissingJobEventReconciliationInterval, "event_reconciliation")
	taskManager.Register(stuckPodDetector.HandleStuckPods, config.Task.StuckPodScanInterval, "stuck_pod")
	taskManager.Register(contextMetrics.UpdateMetrics, config.Task.PodMetricsInterval, "pod_metrics")

	if config.Metric.ExposeQueueUsageMetrics {
		taskManager.Register(queueUtilisationService.RefreshUtilisationData, config.Task.QueueUsageDataRefreshInterval, "pod_usage_data_refresh")
	}

	return func() {
		stopReporter <- true
		clusterContext.Stop()
		conn.Close()
		if taskManager.StopAll(2 * time.Second) {
			log.Warnf("Graceful shutdown timed out")
		}
		log.Infof("Shutdown complete")
		wg.Done()
	}, wg
}

func createConnectionToApi(config configuration.ExecutorConfiguration) (*grpc.ClientConn, error) {
	return client.CreateApiConnection(&config.ApiConnection,
		grpc.WithChainUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(grpc_prometheus.StreamClientInterceptor))
}
