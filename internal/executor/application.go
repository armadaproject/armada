package executor

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/common/cluster"
	"github.com/G-Research/armada/internal/common/task"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/job"
	"github.com/G-Research/armada/internal/executor/metrics"
	"github.com/G-Research/armada/internal/executor/metrics/pod_metrics"
	"github.com/G-Research/armada/internal/executor/node"
	"github.com/G-Research/armada/internal/executor/podchecks"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/service"
	"github.com/G-Research/armada/internal/executor/utilisation"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

func StartUp(config configuration.ExecutorConfiguration) (func(), *sync.WaitGroup) {

	err := validateConfig(config)
	if err != nil {
		log.Errorf("Invalid config: %s", err)
		os.Exit(-1)
	}

	kubernetesClientProvider, err := cluster.NewKubernetesClientProvider(config.Kubernetes.ImpersonateUsers)

	if err != nil {
		log.Errorf("Failed to connect to kubernetes because %s", err)
		os.Exit(-1)
	}

	clusterContext := context.NewClusterContext(
		config.Application,
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

	queueClient := api.NewAggregatedQueueClient(conn)
	usageClient := api.NewUsageClient(conn)
	eventClient := api.NewEventClient(conn)

	eventReporter, stopReporter := reporter.NewJobEventReporter(
		clusterContext,
		eventClient)

	jobLeaseService := service.NewJobLeaseService(
		clusterContext,
		queueClient,
		config.Kubernetes.MinimumJobSize,
		config.Kubernetes.AvoidNodeLabelsOnRetry,
	)

	if config.Kubernetes.PendingPodChecks == nil {
		log.Error("Config error: Missing pending pod checks")
		os.Exit(-1)
	}
	pendingPodChecker, err := podchecks.NewPodChecks(*config.Kubernetes.PendingPodChecks)
	if err != nil {
		log.Errorf("Config error in pending pod checks: %s", err)
		os.Exit(-1)
	}

	jobContext := job.NewClusterJobContext(clusterContext, pendingPodChecker, config.Kubernetes.StuckTerminatingPodExpiry)
	submitter := job.NewSubmitter(clusterContext, config.Kubernetes.PodDefaults)

	nodeInfoService := node.NewKubernetesNodeInfoService(clusterContext, config.Kubernetes.ToleratedTaints)
	queueUtilisationService := utilisation.NewMetricsServerQueueUtilisationService(
		clusterContext, nodeInfoService)
	clusterUtilisationService := utilisation.NewClusterUtilisationService(
		clusterContext,
		queueUtilisationService,
		nodeInfoService,
		usageClient,
		config.Kubernetes.TrackedNodeLabels)

	clusterAllocationService := service.NewClusterAllocationService(
		clusterContext,
		eventReporter,
		jobLeaseService,
		clusterUtilisationService,
		submitter)

	jobManager := service.NewJobManager(
		clusterContext,
		jobContext,
		eventReporter,
		jobLeaseService,
		config.Kubernetes.MinimumPodAge,
		config.Kubernetes.FailedPodExpiry)

	job.RunIngressCleanup(clusterContext)

	pod_metrics.ExposeClusterContextMetrics(clusterContext, clusterUtilisationService, queueUtilisationService, nodeInfoService)

	taskManager.Register(clusterUtilisationService.ReportClusterUtilisation, config.Task.UtilisationReportingInterval, "utilisation_reporting")
	taskManager.Register(eventReporter.ReportMissingJobEvents, config.Task.MissingJobEventReconciliationInterval, "event_reconciliation")
	taskManager.Register(clusterAllocationService.AllocateSpareClusterCapacity, config.Task.AllocateSpareClusterCapacityInterval, "job_lease_request")
	taskManager.Register(jobManager.ManageJobLeases, config.Task.JobLeaseRenewalInterval, "job_management")

	if config.Metric.ExposeQueueUsageMetrics {
		taskManager.Register(queueUtilisationService.RefreshUtilisationData, config.Task.QueueUsageDataRefreshInterval, "pod_usage_data_refresh")

		if config.Task.UtilisationEventReportingInterval > 0 {
			podUtilisationReporter := utilisation.NewUtilisationEventReporter(
				clusterContext,
				queueUtilisationService,
				eventReporter,
				config.Task.UtilisationEventReportingInterval)
			taskManager.Register(podUtilisationReporter.ReportUtilisationEvents, config.Task.UtilisationEventProcessingInterval, "pod_utilisation_event_reporting")
		}
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
	return client.CreateApiConnectionWithCallOptions(&config.ApiConnection,
		[]grpc.CallOption{grpc.MaxCallRecvMsgSize(config.Client.MaxMessageSizeBytes)},
		grpc.WithChainUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(grpc_prometheus.StreamClientInterceptor))
}

func validateConfig(config configuration.ExecutorConfiguration) error {
	missing := util.SubtractStringList(config.Kubernetes.AvoidNodeLabelsOnRetry, config.Kubernetes.TrackedNodeLabels)
	if len(missing) > 0 {
		return fmt.Errorf("These labels were in avoidNodeLabelsOnRetry but not trackedNodeLabels: %s", strings.Join(missing, ", "))
	}
	return nil
}
