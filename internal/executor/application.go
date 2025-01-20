package executor

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
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
	"github.com/armadaproject/armada/internal/executor/podchecks/failedpodchecks"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/service"
	"github.com/armadaproject/armada/internal/executor/utilisation"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/executorapi"
)

func StartUp(ctx *armadacontext.Context, config configuration.ExecutorConfiguration) (func(), *sync.WaitGroup) {
	err := validateConfig(config)
	if err != nil {
		ctx.Errorf("Invalid config: %s", err)
		os.Exit(-1)
	}

	kubernetesClientProvider, err := cluster.NewKubernetesClientProvider(
		config.Kubernetes.ImpersonateUsers,
		config.Kubernetes.QPS,
		config.Kubernetes.Burst,
	)
	if err != nil {
		ctx.Errorf("Failed to connect to kubernetes because %s", err)
		os.Exit(-1)
	}

	// Create an errgroup to run services in.
	g, ctx := armadacontext.ErrGroup(ctx)

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
		ctx.Info("etcd URLs provided; monitoring etcd health enabled")
		etcdClustersHealthMonitoring = healthmonitor.NewMultiHealthMonitor(
			"overall_etcd",
			etcdClusterHealthMonitoringByName,
		).WithMetricsPrefix(
			metrics.ArmadaExecutorMetricsPrefix,
		)
		g.Go(func() error { return etcdClustersHealthMonitoring.Run(ctx) })
		prometheus.MustRegister(etcdClustersHealthMonitoring)
	} else {
		ctx.Info("no etcd URLs provided; etcd health isn't monitored")
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

	return StartUpWithContext(ctx, config, clusterContext, etcdClustersHealthMonitoring, taskManager, wg)
}

func StartUpWithContext(
	ctx *armadacontext.Context,
	config configuration.ExecutorConfiguration,
	clusterContext executor_context.ClusterContext,
	clusterHealthMonitor healthmonitor.HealthMonitor,
	taskManager *task.BackgroundTaskManager,
	wg *sync.WaitGroup,
) (func(), *sync.WaitGroup) {
	nodeInfoService := node.NewKubernetesNodeInfoService(clusterContext, config.Kubernetes.NodeTypeLabel, config.Kubernetes.NodePoolLabel, config.Kubernetes.ToleratedTaints)
	podUtilisationService := utilisation.NewPodUtilisationService(
		clusterContext,
		nodeInfoService,
		config.Metric.CustomUsageMetrics,
		&http.Client{Timeout: 15 * time.Second},
	)

	if config.Kubernetes.PendingPodChecks == nil {
		ctx.Fatalf("Config error: Missing pending pod checks")
	}
	pendingPodChecker, err := podchecks.NewPodChecks(*config.Kubernetes.PendingPodChecks)
	if err != nil {
		ctx.Fatalf("Config error in pending pod checks: %s", err)
	}

	stopExecutorApiComponents := setupExecutorApiComponents(ctx, config, clusterContext, clusterHealthMonitor, taskManager, pendingPodChecker, nodeInfoService, podUtilisationService)

	resourceCleanupService, err := service.NewResourceCleanupService(clusterContext, config.Kubernetes)
	if err != nil {
		ctx.Fatalf("Error creating resource cleanup service: %s", err)
	}
	taskManager.Register(resourceCleanupService.CleanupResources, config.Task.ResourceCleanupInterval, "resource_cleanup")

	if config.Metric.ExposeQueueUsageMetrics {
		taskManager.Register(podUtilisationService.RefreshUtilisationData, config.Task.QueueUsageDataRefreshInterval, "pod_usage_data_refresh")
	}

	return func() {
		clusterContext.Stop()
		stopExecutorApiComponents()
		if taskManager.StopAll(10 * time.Second) {
			ctx.Warnf("Graceful shutdown timed out")
		}
		ctx.Infof("Shutdown complete")
		wg.Done()
	}, wg
}

func setupExecutorApiComponents(
	ctx *armadacontext.Context,
	config configuration.ExecutorConfiguration,
	clusterContext executor_context.ClusterContext,
	clusterHealthMonitor healthmonitor.HealthMonitor,
	taskManager *task.BackgroundTaskManager,
	pendingPodChecker *podchecks.PodChecks,
	nodeInfoService node.NodeInfoService,
	podUtilisationService utilisation.PodUtilisationService,
) func() {
	conn, err := createConnectionToApi(config.ExecutorApiConnection, config.Client.MaxMessageSizeBytes, config.GRPC)
	if err != nil {
		ctx.Fatalf("Failed to connect to Executor API because: %s", err)
	}

	executorApiClient := executorapi.NewExecutorApiClient(conn)
	eventSender := reporter.NewExecutorApiEventSender(executorApiClient, 4*1024*1024)
	jobRunState := job.NewJobRunStateStore(clusterContext)

	clusterUtilisationService := utilisation.NewClusterUtilisationService(
		clusterContext,
		podUtilisationService,
		nodeInfoService,
		config.Kubernetes.TrackedNodeLabels,
		config.Kubernetes.NodeIdLabel,
		config.Kubernetes.MinimumResourcesMarkedAllocatedToNonArmadaPodsPerNode,
		config.Kubernetes.MinimumResourcesMarkedAllocatedToNonArmadaPodsPerNodePriority,
	)

	failedPodChecker, err := failedpodchecks.NewPodRetryChecker(config.Kubernetes.FailedPodChecks)
	if err != nil {
		ctx.Fatalf("Config error in failed pod checks: %s", err)
	}

	eventReporter, stopReporter := reporter.NewJobEventReporter(eventSender, clock.RealClock{}, 200)

	submitter := job.NewSubmitter(
		clusterContext,
		config.Kubernetes.PodDefaults,
		config.Application.SubmitConcurrencyLimit,
		config.Kubernetes.FatalPodSubmissionErrors,
	)

	leaseRequester := service.NewJobLeaseRequester(executorApiClient, clusterContext)
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
		config.Application.JobLeaseRequestTimeout,
	)
	clusterAllocationService := service.NewClusterAllocationService(
		clusterContext,
		eventReporter,
		jobRunState,
		submitter,
		clusterHealthMonitor,
	)
	podIssueService, err := service.NewPodIssuerHandler(
		jobRunState,
		clusterContext,
		eventReporter,
		config.Kubernetes.StateChecks,
		pendingPodChecker,
		failedPodChecker,
		config.Kubernetes.StuckTerminatingPodExpiry,
	)
	if err != nil {
		ctx.Fatalf("Failed to create pod issue service: %s", err)
	}

	jobStateReporter, err := service.NewJobStateReporter(
		clusterContext,
		eventReporter,
		podIssueService,
	)
	if err != nil {
		ctx.Fatalf("Failed to create job state reporter: %s", err)
	}

	taskManager.Register(podIssueService.HandlePodIssues, config.Task.PodIssueHandlingInterval, "pod_issue_handling")
	taskManager.Register(preemptRunProcessor.Run, config.Task.StateProcessorInterval, "preempt_runs")
	taskManager.Register(removeRunProcessor.Run, config.Task.StateProcessorInterval, "remove_runs")
	taskManager.Register(jobRequester.RequestJobsRuns, config.Task.JobLeaseRenewalInterval, "request_runs")
	taskManager.Register(clusterAllocationService.AllocateSpareClusterCapacity, config.Task.AllocateSpareClusterCapacityInterval, "submit_runs")
	taskManager.Register(jobStateReporter.ReportMissingJobEvents, config.Task.MissingJobEventReconciliationInterval, "event_reconciliation")
	_, err = pod_metrics.ExposeClusterContextMetrics(clusterContext, clusterUtilisationService, podUtilisationService, nodeInfoService)
	if err != nil {
		ctx.Fatalf("Failed to setup cluster context metrics: %s", err)
	}
	runStateMetricsCollector := runstate.NewJobRunStateStoreMetricsCollector(jobRunState)
	prometheus.MustRegister(runStateMetricsCollector)

	if config.Metric.ExposeQueueUsageMetrics && config.Task.UtilisationEventReportingInterval > 0 {
		podUtilisationReporter, err := utilisation.NewUtilisationEventReporter(
			clusterContext,
			podUtilisationService,
			eventReporter,
			config.Task.UtilisationEventReportingInterval)
		if err != nil {
			ctx.Fatalf("Failed to pod utilisation reporter: %s", err)
		}
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
