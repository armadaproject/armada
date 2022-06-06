package executor

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/common/cluster"
	"github.com/G-Research/armada/internal/common/task"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/etcdhealthmonitor"
	"github.com/G-Research/armada/internal/executor/configuration"
	executor_context "github.com/G-Research/armada/internal/executor/context"
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

	kubernetesClientProvider, err := cluster.NewKubernetesClientProvider(
		config.Kubernetes.ImpersonateUsers,
		config.Kubernetes.QPS,
		config.Kubernetes.Burst,
	)
	if err != nil {
		log.Errorf("Failed to connect to kubernetes because %s", err)
		os.Exit(-1)
	}

	var etcdHealthMonitor *etcdhealthmonitor.EtcdHealthMonitor
	if len(config.Kubernetes.EtcdMetricUrls) != 0 {
		log.Info("etcd URLs provided; monitoring etcd health")

		if config.Kubernetes.EtcdFractionOfStorageInUseSoftLimit <= 0 || config.Kubernetes.EtcdFractionOfStorageInUseSoftLimit > 1 {
			panic("EtcdFractionOfStorageInUseSoftLimit must be in (0, 1]")
		}

		if config.Kubernetes.EtcdFractionOfStorageInUseHardLimit <= 0 || config.Kubernetes.EtcdFractionOfStorageInUseHardLimit > 1 {
			panic("EtcdFractionOfStorageInUseHardLimit must be in (0, 1]")
		}

		etcdHealthMonitor, err = etcdhealthmonitor.New(config.Kubernetes.EtcdMetricUrls, nil)
		if err != nil {
			panic(err)
		}

		// Run the etcd health monitor in the background.
		// TODO: Run services in an errgroup
		ctx, _ := context.WithCancel(context.Background())
		go etcdHealthMonitor.Run(ctx)

		// Give it up to 10 seconds to start collecting metrics.
		start := time.Now()
		etcdFractionOfStorageInUse, err := etcdHealthMonitor.MaxFractionOfStorageInUse()
		for err != nil {
			time.Sleep(time.Second)
			etcdFractionOfStorageInUse, err = etcdHealthMonitor.MaxFractionOfStorageInUse()
			if time.Since(start) > 10*time.Second {
				err := errors.WithMessage(err, "EtcdHealthMonitor failed to start in time")
				panic(err)
			}
		}
		log.Infof("%f percent of etcd storage is in use", etcdFractionOfStorageInUse)
	} else {
		log.Info("no etcd URLs provided; etcd health isn't monitored")
	}

	clusterContext := executor_context.NewClusterContext(
		config.Application,
		2*time.Minute,
		kubernetesClientProvider,
	)
	clusterContext.EtcdHealthMonitor = etcdHealthMonitor
	clusterContext.EtcdMaxFractionOfStorageInUse = config.Kubernetes.EtcdFractionOfStorageInUseHardLimit

	wg := &sync.WaitGroup{}
	wg.Add(1)

	taskManager := task.NewBackgroundTaskManager(metrics.ArmadaExecutorMetricsPrefix)
	taskManager.Register(clusterContext.ProcessPodsToDelete, config.Task.PodDeletionInterval, "pod_deletion")

	return StartUpWithContext(config, clusterContext, etcdHealthMonitor, taskManager, wg)
}

func StartUpWithContext(config configuration.ExecutorConfiguration, clusterContext executor_context.ClusterContext, etcdHealthMonitor *etcdhealthmonitor.EtcdHealthMonitor, taskManager *task.BackgroundTaskManager, wg *sync.WaitGroup) (func(), *sync.WaitGroup) {

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
	jobLeaseService.EtcdHealthMonitor = etcdHealthMonitor
	jobLeaseService.EtcdMaxFractionOfStorageInUse = config.Kubernetes.EtcdFractionOfStorageInUseSoftLimit

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
	submitter := job.NewSubmitter(clusterContext, config.Kubernetes.PodDefaults, config.Application.SubmitConcurrencyLimit)

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
		jobLeaseService)

	resourceCleanupService := service.NewResourceCleanupService(clusterContext, config.Kubernetes)

	pod_metrics.ExposeClusterContextMetrics(clusterContext, clusterUtilisationService, queueUtilisationService, nodeInfoService)

	taskManager.Register(clusterUtilisationService.ReportClusterUtilisation, config.Task.UtilisationReportingInterval, "utilisation_reporting")
	taskManager.Register(eventReporter.ReportMissingJobEvents, config.Task.MissingJobEventReconciliationInterval, "event_reconciliation")
	taskManager.Register(clusterAllocationService.AllocateSpareClusterCapacity, config.Task.AllocateSpareClusterCapacityInterval, "job_lease_request")
	taskManager.Register(jobManager.ManageJobLeases, config.Task.JobLeaseRenewalInterval, "job_management")
	taskManager.Register(resourceCleanupService.CleanupResources, config.Task.ResourceCleanupInterval, "resource_cleanup")

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
