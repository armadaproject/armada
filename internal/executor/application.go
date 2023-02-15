package executor

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/armadaproject/armada/internal/common/cluster"
	"github.com/armadaproject/armada/internal/common/task"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	executor_context "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/healthmonitor"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/metrics"
	"github.com/armadaproject/armada/internal/executor/metrics/pod_metrics"
	"github.com/armadaproject/armada/internal/executor/node"
	"github.com/armadaproject/armada/internal/executor/podchecks"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/service"
	"github.com/armadaproject/armada/internal/executor/utilisation"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/executorapi"
)

func StartUp(config configuration.ExecutorConfiguration) (func(), *sync.WaitGroup) {
	err := validateConfig(config)
	if err != nil {
		log.Errorf("Invalid config: %s", err)
		os.Exit(-1)
	}

	kubernetesClientProvider, err := cluster.NewKubernetesClientProvider(
		config.Kubernetes.ImpersonateUsers,
		config.Kubernetes.QPS,
		config.Kubernetes.Burst,
	)
	if err != nil {
		log.Errorf("Failed to connect to kubernetes because %s", err)
		os.Exit(-1)
	}

	var etcdHealthMonitor healthmonitor.EtcdLimitHealthMonitor
	if len(config.Kubernetes.Etcd.MetricUrls) > 0 {
		log.Info("etcd URLs provided; monitoring etcd health enabled")

		etcdHealthMonitor, err = healthmonitor.NewEtcdHealthMonitor(config.Kubernetes.Etcd, nil)
		if err != nil {
			panic(err)
		}
	} else {
		log.Info("no etcd URLs provided; etcd health isn't monitored")
	}

	clusterContext := executor_context.NewClusterContext(
		config.Application,
		2*time.Minute,
		kubernetesClientProvider,
		etcdHealthMonitor,
		config.Kubernetes.PodKillTimeout,
	)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	taskManager := task.NewBackgroundTaskManager(metrics.ArmadaExecutorMetricsPrefix)
	taskManager.Register(clusterContext.ProcessPodsToDelete, config.Task.PodDeletionInterval, "pod_deletion")

	return StartUpWithContext(config, clusterContext, etcdHealthMonitor, taskManager, wg)
}

func StartUpWithContext(
	config configuration.ExecutorConfiguration,
	clusterContext executor_context.ClusterContext,
	etcdHealthMonitor healthmonitor.EtcdLimitHealthMonitor,
	taskManager *task.BackgroundTaskManager,
	wg *sync.WaitGroup,
) (func(), *sync.WaitGroup) {
	conn, err := createConnectionToApi(config)
	if err != nil {
		log.Errorf("Failed to connect to API because: %s", err)
		os.Exit(-1)
	}

	var executorApiClient executorapi.ExecutorApiClient
	var usageClient api.UsageClient
	var eventSender reporter.EventSender
	var queueClient api.AggregatedQueueClient

	if config.Application.UseExecutorApi {
		executorApiClient = executorapi.NewExecutorApiClient(conn)
		eventSender = reporter.NewExecutorApiEventSender(executorApiClient, 4*1024*1024)
	} else {
		usageClient = api.NewUsageClient(conn)
		queueClient = api.NewAggregatedQueueClient(conn)
		eventClient := api.NewEventClient(conn)
		eventSender = reporter.NewLegacyApiEventSender(eventClient)
	}

	eventReporter, stopReporter := reporter.NewJobEventReporter(
		clusterContext,
		eventSender)

	if config.Kubernetes.PendingPodChecks == nil {
		log.Error("Config error: Missing pending pod checks")
		os.Exit(-1)
	}
	pendingPodChecker, err := podchecks.NewPodChecks(*config.Kubernetes.PendingPodChecks)
	if err != nil {
		log.Errorf("Config error in pending pod checks: %s", err)
		os.Exit(-1)
	}

	jobContext := job.NewClusterJobContext(
		clusterContext,
		pendingPodChecker,
		config.Kubernetes.StuckTerminatingPodExpiry,
		config.Application.UpdateConcurrencyLimit)
	submitter := job.NewSubmitter(
		clusterContext,
		config.Kubernetes.PodDefaults,
		config.Application.SubmitConcurrencyLimit,
		config.Kubernetes.FatalPodSubmissionErrors,
	)

	nodeInfoService := node.NewKubernetesNodeInfoService(clusterContext, config.Kubernetes.ToleratedTaints)
	podUtilisationService := utilisation.NewPodUtilisationService(
		clusterContext,
		nodeInfoService,
		config.Metric.CustomUsageMetrics,
		&http.Client{Timeout: 15 * time.Second},
	)
	clusterUtilisationService := utilisation.NewClusterUtilisationService(
		clusterContext,
		podUtilisationService,
		nodeInfoService,
		usageClient,
		config.Kubernetes.TrackedNodeLabels,
		config.Kubernetes.NodeIdLabel,
		config.Kubernetes.NodeReservedResources,
		config.Kubernetes.NodeReservedResourcesPriority,
	)

	var clusterAllocationService service.ClusterAllocator

	if config.Application.UseExecutorApi {
		leaseRequester := service.NewJobLeaseRequester(
			executorApiClient, clusterContext, config.Kubernetes.MinimumJobSize)
		clusterAllocationService = service.NewClusterAllocationService(
			clusterContext,
			eventReporter,
			leaseRequester,
			clusterUtilisationService,
			submitter,
			config.Kubernetes.PodDefaults,
			etcdHealthMonitor)
		podIssueService := service.NewPodIssueService(
			clusterContext,
			eventReporter,
			pendingPodChecker,
			config.Kubernetes.StuckTerminatingPodExpiry)
		taskManager.Register(podIssueService.HandlePodIssues, config.Task.PodIssueHandlingInterval, "pod_issue_handling")
	} else {
		jobLeaseService := service.NewJobLeaseService(
			clusterContext,
			queueClient,
			config.Kubernetes.MinimumJobSize,
			config.Kubernetes.AvoidNodeLabelsOnRetry,
		)
		clusterAllocationService = service.NewLegacyClusterAllocationService(
			clusterContext,
			eventReporter,
			jobLeaseService,
			clusterUtilisationService,
			submitter,
			etcdHealthMonitor,
			config.Kubernetes.NodeReservedResources,
		)
		jobManager := service.NewJobManager(
			clusterContext,
			jobContext,
			eventReporter,
			jobLeaseService)
		taskManager.Register(jobManager.ManageJobLeases, config.Task.JobLeaseRenewalInterval, "job_management")
		taskManager.Register(clusterUtilisationService.ReportClusterUtilisation, config.Task.UtilisationReportingInterval, "utilisation_reporting")
	}

	resourceCleanupService := service.NewResourceCleanupService(clusterContext, config.Kubernetes)

	pod_metrics.ExposeClusterContextMetrics(clusterContext, clusterUtilisationService, podUtilisationService, nodeInfoService)

	taskManager.Register(eventReporter.ReportMissingJobEvents, config.Task.MissingJobEventReconciliationInterval, "event_reconciliation")
	taskManager.Register(clusterAllocationService.AllocateSpareClusterCapacity, config.Task.AllocateSpareClusterCapacityInterval, "job_lease_request")
	taskManager.Register(resourceCleanupService.CleanupResources, config.Task.ResourceCleanupInterval, "resource_cleanup")

	if config.Metric.ExposeQueueUsageMetrics {
		taskManager.Register(podUtilisationService.RefreshUtilisationData, config.Task.QueueUsageDataRefreshInterval, "pod_usage_data_refresh")

		if config.Task.UtilisationEventReportingInterval > 0 {
			podUtilisationReporter := utilisation.NewUtilisationEventReporter(
				clusterContext,
				podUtilisationService,
				eventReporter,
				config.Task.UtilisationEventReportingInterval)
			taskManager.Register(
				podUtilisationReporter.ReportUtilisationEvents,
				config.Task.UtilisationEventProcessingInterval,
				"pod_utilisation_event_reporting",
			)
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
	grpc_prometheus.EnableClientHandlingTimeHistogram()
	return client.CreateApiConnectionWithCallOptions(
		&config.ApiConnection,
		[]grpc.CallOption{grpc.MaxCallRecvMsgSize(config.Client.MaxMessageSizeBytes)},
		grpc.WithChainUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(grpc_prometheus.StreamClientInterceptor),
		grpc.WithKeepaliveParams(config.GRPC),
	)
}

func validateConfig(config configuration.ExecutorConfiguration) error {
	missing := util.SubtractStringList(config.Kubernetes.AvoidNodeLabelsOnRetry, config.Kubernetes.TrackedNodeLabels)
	if len(missing) > 0 {
		return fmt.Errorf("These labels were in avoidNodeLabelsOnRetry but not trackedNodeLabels: %s", strings.Join(missing, ", "))
	}
	if config.Application.SubmitConcurrencyLimit <= 0 {
		return fmt.Errorf("SubmitConcurrencyLimit was %d, must be greater or equal to 1", config.Application.SubmitConcurrencyLimit)
	}
	if config.Application.UpdateConcurrencyLimit <= 0 {
		return fmt.Errorf("UpdateConcurrencyLimit was %d, must be greater or equal to 1", config.Application.UpdateConcurrencyLimit)
	}
	if config.Application.DeleteConcurrencyLimit <= 0 {
		return fmt.Errorf("DeleteConcurrencyLimit was %d, must be greater or equal to 1", config.Application.DeleteConcurrencyLimit)
	}
	if config.Kubernetes.Etcd.FractionOfStorageInUseSoftLimit <= 0 || config.Kubernetes.Etcd.FractionOfStorageInUseSoftLimit > 1 {
		return fmt.Errorf("EtcdFractionOfStorageInUseSoftLimit must be in (0, 1]")
	}
	if config.Kubernetes.Etcd.FractionOfStorageInUseHardLimit <= 0 || config.Kubernetes.Etcd.FractionOfStorageInUseHardLimit > 1 {
		return fmt.Errorf("EtcdFractionOfStorageInUseHardLimit must be in (0, 1]")
	}
	return nil
}
