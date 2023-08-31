package executor

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/caarlos0/log"
	"github.com/go-playground/validator/v10"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/armadaproject/armada/internal/common/cluster"
	"github.com/armadaproject/armada/internal/common/etcdhealth"
	"github.com/armadaproject/armada/internal/common/healthmonitor"
	common_metrics "github.com/armadaproject/armada/internal/common/metrics"
	"github.com/armadaproject/armada/internal/common/task"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	executor_context "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/job/processors"
	"github.com/armadaproject/armada/internal/executor/metrics"
	"github.com/armadaproject/armada/internal/executor/metrics/pod_metrics"
	"github.com/armadaproject/armada/internal/executor/metrics/runstate"
	"github.com/armadaproject/armada/internal/executor/node"
	"github.com/armadaproject/armada/internal/executor/podchecks"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/service"
	"github.com/armadaproject/armada/internal/executor/utilisation"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/executorapi"
)

func StartUp(ctx context.Context, log *logrus.Entry, config configuration.ExecutorConfiguration) (func(), *sync.WaitGroup) {
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

	// Create an errgroup to run services in.
	g, ctx := errgroup.WithContext(ctx)

	// Setup etcd health monitoring.
	etcdClusterHealthMonitoringByName := make(map[string]healthmonitor.HealthMonitor, len(config.Kubernetes.Etcd.EtcdClustersHealthMonitoring))
	for _, etcdClusterHealthMonitoring := range config.Kubernetes.Etcd.EtcdClustersHealthMonitoring {
		etcdReplicaHealthMonitorsByUrl := make(map[string]healthmonitor.HealthMonitor, len(etcdClusterHealthMonitoring.MetricUrls))
		for _, metricsUrl := range etcdClusterHealthMonitoring.MetricUrls {
			etcdReplicaHealthMonitorsByUrl[metricsUrl] = etcdhealth.NewEtcdReplicaHealthMonitor(
				metricsUrl,
				etcdClusterHealthMonitoring.FractionOfStorageInUseLimit,
				etcdClusterHealthMonitoring.FractionOfStorageLimit,
				etcdClusterHealthMonitoring.ReplicaTimeout,
				etcdClusterHealthMonitoring.ScrapeInterval,
				etcdClusterHealthMonitoring.ScrapeDelayBucketsStart,
				etcdClusterHealthMonitoring.ScrapeDelayBucketsFactor,
				etcdClusterHealthMonitoring.ScrapeDelayBucketsCount,
				common_metrics.NewHttpMetricsProvider(metricsUrl, http.DefaultClient),
			).WithMetricsPrefix(metrics.ArmadaExecutorMetricsPrefix)
		}
		etcdClusterHealthMonitoringByName[etcdClusterHealthMonitoring.Name] = healthmonitor.NewMultiHealthMonitor(
			etcdClusterHealthMonitoring.Name,
			etcdReplicaHealthMonitorsByUrl,
		).WithMinimumReplicasAvailable(
			etcdClusterHealthMonitoring.MinimumReplicasAvailable,
		).WithMetricsPrefix(
			metrics.ArmadaExecutorMetricsPrefix,
		)
	}
	var etcdClustersHealthMonitoring healthmonitor.HealthMonitor
	if len(etcdClusterHealthMonitoringByName) > 0 {
		log.Info("etcd URLs provided; monitoring etcd health enabled")
		etcdClustersHealthMonitoring = healthmonitor.NewMultiHealthMonitor(
			"overall_etcd",
			etcdClusterHealthMonitoringByName,
		).WithMetricsPrefix(
			metrics.ArmadaExecutorMetricsPrefix,
		)
		g.Go(func() error { return etcdClustersHealthMonitoring.Run(ctx, log) })
		prometheus.MustRegister(etcdClustersHealthMonitoring)
	} else {
		log.Info("no etcd URLs provided; etcd health isn't monitored")
	}

	clusterContext := executor_context.NewClusterContext(
		config.Application,
		2*time.Minute,
		kubernetesClientProvider,
		config.Kubernetes.PodKillTimeout,
	)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	taskManager := task.NewBackgroundTaskManager(metrics.ArmadaExecutorMetricsPrefix)
	taskManager.Register(clusterContext.ProcessPodsToDelete, config.Task.PodDeletionInterval, "pod_deletion")

	return StartUpWithContext(log, config, clusterContext, etcdClustersHealthMonitoring, taskManager, wg)
}

func StartUpWithContext(
	log *logrus.Entry,
	config configuration.ExecutorConfiguration,
	clusterContext executor_context.ClusterContext,
	clusterHealthMonitor healthmonitor.HealthMonitor,
	taskManager *task.BackgroundTaskManager,
	wg *sync.WaitGroup,
) (func(), *sync.WaitGroup) {
	nodeInfoService := node.NewKubernetesNodeInfoService(clusterContext, config.Kubernetes.ToleratedTaints)
	podUtilisationService := utilisation.NewPodUtilisationService(
		clusterContext,
		nodeInfoService,
		config.Metric.CustomUsageMetrics,
		&http.Client{Timeout: 15 * time.Second},
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

	stopServerApiComponents := setupServerApiComponents(config, clusterContext, clusterHealthMonitor, taskManager, pendingPodChecker, nodeInfoService, podUtilisationService)
	stopExecutorApiComponents := setupExecutorApiComponents(config, clusterContext, clusterHealthMonitor, taskManager, pendingPodChecker, nodeInfoService, podUtilisationService)

	resourceCleanupService := service.NewResourceCleanupService(clusterContext, config.Kubernetes)
	taskManager.Register(resourceCleanupService.CleanupResources, config.Task.ResourceCleanupInterval, "resource_cleanup")

	if config.Metric.ExposeQueueUsageMetrics {
		taskManager.Register(podUtilisationService.RefreshUtilisationData, config.Task.QueueUsageDataRefreshInterval, "pod_usage_data_refresh")
	}

	return func() {
		clusterContext.Stop()
		stopServerApiComponents()
		stopExecutorApiComponents()
		if taskManager.StopAll(10 * time.Second) {
			log.Warnf("Graceful shutdown timed out")
		}
		log.Infof("Shutdown complete")
		wg.Done()
	}, wg
}

func setupExecutorApiComponents(
	config configuration.ExecutorConfiguration,
	clusterContext executor_context.ClusterContext,
	clusterHealthMonitor healthmonitor.HealthMonitor,
	taskManager *task.BackgroundTaskManager,
	pendingPodChecker *podchecks.PodChecks,
	nodeInfoService node.NodeInfoService,
	podUtilisationService utilisation.PodUtilisationService,
) func() {
	if !config.Application.UseExecutorApi {
		return func() {}
	}
	conn, err := createConnectionToApi(config.ExecutorApiConnection, config.Client.MaxMessageSizeBytes, config.GRPC)
	if err != nil {
		log.Errorf("Failed to connect to Executor API because: %s", err)
		os.Exit(-1)
	}

	executorApiClient := executorapi.NewExecutorApiClient(conn)
	eventSender := reporter.NewExecutorApiEventSender(executorApiClient, 4*1024*1024)
	jobRunState := job.NewJobRunStateStore(clusterContext)

	clusterUtilisationService := utilisation.NewClusterUtilisationService(
		clusterContext,
		podUtilisationService,
		nodeInfoService,
		nil,
		config.Kubernetes.TrackedNodeLabels,
		config.Kubernetes.NodeIdLabel,
		config.Kubernetes.MinimumResourcesMarkedAllocatedToNonArmadaPodsPerNode,
		config.Kubernetes.MinimumResourcesMarkedAllocatedToNonArmadaPodsPerNodePriority,
	)

	eventReporter, stopReporter := reporter.NewJobEventReporter(
		clusterContext,
		jobRunState,
		eventSender)

	submitter := job.NewSubmitter(
		clusterContext,
		config.Kubernetes.PodDefaults,
		config.Application.SubmitConcurrencyLimit,
		config.Kubernetes.FatalPodSubmissionErrors,
	)

	leaseRequester := service.NewJobLeaseRequester(
		executorApiClient, clusterContext, config.Kubernetes.MinimumJobSize)
	preemptRunProcessor := processors.NewRunPreemptedProcessor(clusterContext, jobRunState, eventReporter)
	removeRunProcessor := processors.NewRemoveRunProcessor(clusterContext, jobRunState)

	jobRequester := service.NewJobRequester(
		clusterContext,
		eventReporter,
		leaseRequester,
		jobRunState,
		clusterUtilisationService,
		config.Kubernetes.PodDefaults,
		config.Application.MaxLeasedJobs,
	)
	clusterAllocationService := service.NewClusterAllocationService(
		clusterContext,
		eventReporter,
		jobRunState,
		submitter,
		clusterHealthMonitor,
	)
	podIssueService := service.NewIssueHandler(
		jobRunState,
		clusterContext,
		eventReporter,
		config.Kubernetes.StateChecks,
		pendingPodChecker,
		config.Kubernetes.StuckTerminatingPodExpiry,
	)

	taskManager.Register(podIssueService.HandlePodIssues, config.Task.PodIssueHandlingInterval, "pod_issue_handling")
	taskManager.Register(preemptRunProcessor.Run, config.Task.StateProcessorInterval, "preempt_runs")
	taskManager.Register(removeRunProcessor.Run, config.Task.StateProcessorInterval, "remove_runs")
	taskManager.Register(jobRequester.RequestJobsRuns, config.Task.AllocateSpareClusterCapacityInterval, "request_runs")
	taskManager.Register(clusterAllocationService.AllocateSpareClusterCapacity, config.Task.AllocateSpareClusterCapacityInterval, "submit_runs")
	taskManager.Register(eventReporter.ReportMissingJobEvents, config.Task.MissingJobEventReconciliationInterval, "event_reconciliation")
	pod_metrics.ExposeClusterContextMetrics(clusterContext, clusterUtilisationService, podUtilisationService, nodeInfoService)
	runStateMetricsCollector := runstate.NewJobRunStateStoreMetricsCollector(jobRunState)
	prometheus.MustRegister(runStateMetricsCollector)

	if config.Metric.ExposeQueueUsageMetrics && config.Task.UtilisationEventReportingInterval > 0 {
		podUtilisationReporter := utilisation.NewUtilisationEventReporter(
			clusterContext,
			podUtilisationService,
			eventReporter,
			config.Task.UtilisationEventReportingInterval,
			false)
		taskManager.Register(
			podUtilisationReporter.ReportUtilisationEvents,
			config.Task.UtilisationEventProcessingInterval,
			"pod_utilisation_event_reporting",
		)
	}

	return func() {
		stopReporter <- true
		conn.Close()
	}
}

func setupServerApiComponents(
	config configuration.ExecutorConfiguration,
	clusterContext executor_context.ClusterContext,
	clusterHealthMonitor healthmonitor.HealthMonitor,
	taskManager *task.BackgroundTaskManager,
	pendingPodChecker *podchecks.PodChecks,
	nodeInfoService node.NodeInfoService,
	podUtilisationService utilisation.PodUtilisationService,
) func() {
	if !config.Application.UseLegacyApi {
		return func() {}
	}
	conn, err := createConnectionToApi(config.ApiConnection, config.Client.MaxMessageSizeBytes, config.GRPC)
	if err != nil {
		log.Errorf("Failed to connect to API because: %s", err)
		os.Exit(-1)
	}

	usageClient := api.NewUsageClient(conn)
	queueClient := api.NewAggregatedQueueClient(conn)
	eventClient := api.NewEventClient(conn)
	eventSender := reporter.NewLegacyApiEventSender(eventClient)

	eventReporter, stopReporter := reporter.NewJobEventReporter(
		clusterContext,
		nil,
		eventSender)

	jobContext := job.NewClusterJobContext(
		clusterContext,
		pendingPodChecker,
		config.Kubernetes.StuckTerminatingPodExpiry,
		config.Application.UpdateConcurrencyLimit)

	clusterUtilisationService := utilisation.NewClusterUtilisationService(
		clusterContext,
		podUtilisationService,
		nodeInfoService,
		usageClient,
		config.Kubernetes.TrackedNodeLabels,
		config.Kubernetes.NodeIdLabel,
		config.Kubernetes.MinimumResourcesMarkedAllocatedToNonArmadaPodsPerNode,
		config.Kubernetes.MinimumResourcesMarkedAllocatedToNonArmadaPodsPerNodePriority,
	)

	jobLeaseService := service.NewJobLeaseService(
		clusterContext,
		queueClient,
		config.Kubernetes.MinimumJobSize,
		config.Kubernetes.AvoidNodeLabelsOnRetry,
		config.Application.JobLeaseRequestTimeout,
	)

	submitter := job.NewSubmitter(
		clusterContext,
		config.Kubernetes.PodDefaults,
		config.Application.SubmitConcurrencyLimit,
		config.Kubernetes.FatalPodSubmissionErrors,
	)

	clusterAllocationService := service.NewLegacyClusterAllocationService(
		clusterContext,
		eventReporter,
		jobLeaseService,
		clusterUtilisationService,
		submitter,
		clusterHealthMonitor,
	)

	jobManager := service.NewJobManager(
		clusterContext,
		jobContext,
		eventReporter,
		jobLeaseService)
	taskManager.Register(jobManager.ManageJobLeases, config.Task.JobLeaseRenewalInterval, "job_management")
	taskManager.Register(clusterUtilisationService.ReportClusterUtilisation, config.Task.UtilisationReportingInterval, "utilisation_reporting")
	taskManager.Register(eventReporter.ReportMissingJobEvents, config.Task.MissingJobEventReconciliationInterval, "event_reconciliation_legacy")

	if config.Metric.ExposeQueueUsageMetrics && config.Task.UtilisationEventReportingInterval > 0 {
		podUtilisationReporter := utilisation.NewUtilisationEventReporter(
			clusterContext,
			podUtilisationService,
			eventReporter,
			config.Task.UtilisationEventReportingInterval,
			true)
		taskManager.Register(
			podUtilisationReporter.ReportUtilisationEvents,
			config.Task.UtilisationEventProcessingInterval,
			"pod_utilisation_event_reporting",
		)
	}

	if !config.Application.UseExecutorApi {
		taskManager.Register(clusterAllocationService.AllocateSpareClusterCapacity, config.Task.AllocateSpareClusterCapacityInterval, "job_lease_request")
		pod_metrics.ExposeClusterContextMetrics(clusterContext, clusterUtilisationService, podUtilisationService, nodeInfoService)

	}

	return func() {
		stopReporter <- true
		conn.Close()
	}
}

func createConnectionToApi(connectionDetails client.ApiConnectionDetails, maxMessageSizeBytes int, grpcConfig keepalive.ClientParameters) (*grpc.ClientConn, error) {
	grpc_prometheus.EnableClientHandlingTimeHistogram()
	return client.CreateApiConnectionWithCallOptions(
		&connectionDetails,
		[]grpc.CallOption{grpc.MaxCallRecvMsgSize(maxMessageSizeBytes)},
		grpc.WithChainUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(grpc_prometheus.StreamClientInterceptor),
		grpc.WithKeepaliveParams(grpcConfig),
	)
}

func validateConfig(config configuration.ExecutorConfiguration) error {
	validator := validator.New()
	if err := validator.Struct(config); err != nil {
		return err
	}
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
	return nil
}
