package scheduler

import (
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/app"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth"
	dbcommon "github.com/armadaproject/armada/internal/common/database"
	grpcCommon "github.com/armadaproject/armada/internal/common/grpc"
	"github.com/armadaproject/armada/internal/common/health"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/optimisation/descent"
	"github.com/armadaproject/armada/internal/common/optimisation/nesterov"
	"github.com/armadaproject/armada/internal/common/profiling"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/serve"
	"github.com/armadaproject/armada/internal/common/types"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/failureestimator"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/leader"
	"github.com/armadaproject/armada/internal/scheduler/metrics"
	"github.com/armadaproject/armada/internal/scheduler/quarantine"
	"github.com/armadaproject/armada/internal/scheduler/reports"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/executorapi"
)

// Run sets up a Scheduler application and runs it until a SIGTERM is received
func Run(config schedulerconfig.Configuration) error {
	g, ctx := armadacontext.ErrGroup(app.CreateContextWithShutdown())

	// ////////////////////////////////////////////////////////////////////////
	// Profiling
	// ////////////////////////////////////////////////////////////////////////
	if config.PprofPort != nil {
		pprofServer := profiling.SetupPprofHttpServer(*config.PprofPort)
		g.Go(func() error {
			return serve.ListenAndServe(ctx, pprofServer)
		})
	}

	// ////////////////////////////////////////////////////////////////////////
	// Health Checks
	// ////////////////////////////////////////////////////////////////////////
	mux := http.NewServeMux()

	startupCompleteCheck := health.NewStartupCompleteChecker()
	healthChecks := health.NewMultiChecker(startupCompleteCheck)
	health.SetupHttpMux(mux, healthChecks)
	shutdownHttpServer := common.ServeHttp(uint16(config.Http.Port), mux)
	defer shutdownHttpServer()

	// List of services to run concurrently.
	// Because we want to start services only once all input validation has been completed,
	// we add all services to a slice and start them together at the end of this function.
	var services []func() error

	// ////////////////////////////////////////////////////////////////////////
	// Database setup (postgres and redis)
	// ////////////////////////////////////////////////////////////////////////
	ctx.Infof("Setting up database connections")
	db, err := dbcommon.OpenPgxPool(config.Postgres)
	if err != nil {
		return errors.WithMessage(err, "Error opening connection to postgres")
	}
	defer db.Close()
	jobRepository := database.NewPostgresJobRepository(db, int32(config.DatabaseFetchSize))
	executorRepository := database.NewPostgresExecutorRepository(db)

	redisClient := redis.NewUniversalClient(config.Redis.AsUniversalOptions())
	defer func() {
		err := redisClient.Close()
		if err != nil {
			logging.
				WithStacktrace(ctx, err).
				Warnf("Redis client didn't close down cleanly")
		}
	}()
	queueRepository := repository.NewRedisQueueRepository(redisClient)
	legacyExecutorRepository := database.NewRedisExecutorRepository(redisClient, "pulsar")

	// ////////////////////////////////////////////////////////////////////////
	// Pulsar
	// ////////////////////////////////////////////////////////////////////////
	ctx.Infof("Setting up Pulsar connectivity")
	pulsarClient, err := pulsarutils.NewPulsarClient(&config.Pulsar)
	if err != nil {
		return errors.WithMessage(err, "Error creating pulsar client")
	}
	defer pulsarClient.Close()
	pulsarPublisher, err := NewPulsarPublisher(pulsarClient, pulsar.ProducerOptions{
		Name:             fmt.Sprintf("armada-scheduler-%s", uuid.NewString()),
		CompressionType:  config.Pulsar.CompressionType,
		CompressionLevel: config.Pulsar.CompressionLevel,
		BatchingMaxSize:  config.Pulsar.MaxAllowedMessageSize,
		Topic:            config.Pulsar.JobsetEventsTopic,
	}, config.PulsarSendTimeout)
	if err != nil {
		return errors.WithMessage(err, "error creating pulsar publisher")
	}

	// ////////////////////////////////////////////////////////////////////////
	// Leader Election
	// ////////////////////////////////////////////////////////////////////////
	leaderController, err := createLeaderController(ctx, config.Leader)
	if err != nil {
		return errors.WithMessage(err, "error creating leader controller")
	}
	services = append(services, func() error { return leaderController.Run(ctx) })

	// ////////////////////////////////////////////////////////////////////////
	// Executor Api
	// ////////////////////////////////////////////////////////////////////////
	ctx.Infof("Setting up executor api")
	apiProducer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Name:             fmt.Sprintf("armada-executor-api-%s", uuid.NewString()),
		CompressionType:  config.Pulsar.CompressionType,
		CompressionLevel: config.Pulsar.CompressionLevel,
		BatchingMaxSize:  config.Pulsar.MaxAllowedMessageSize,
		Topic:            config.Pulsar.JobsetEventsTopic,
	})
	if err != nil {
		return errors.Wrapf(err, "error creating pulsar producer for executor api")
	}
	defer apiProducer.Close()
	authServices, err := auth.ConfigureAuth(config.Auth)
	if err != nil {
		return errors.WithMessage(err, "error creating auth services")
	}
	grpcServer := grpcCommon.CreateGrpcServer(config.Grpc.KeepaliveParams, config.Grpc.KeepaliveEnforcementPolicy, authServices, config.Grpc.Tls)
	defer grpcServer.GracefulStop()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Grpc.Port))
	if err != nil {
		return errors.WithMessage(err, "error setting up gRPC server")
	}
	executorServer, err := NewExecutorApi(
		apiProducer,
		jobRepository,
		executorRepository,
		legacyExecutorRepository,
		types.AllowedPriorities(config.Scheduling.PriorityClasses),
		config.Scheduling.NodeIdLabel,
		config.Scheduling.PriorityClassNameOverride,
		config.Pulsar.MaxAllowedMessageSize,
	)
	if err != nil {
		return errors.WithMessage(err, "error creating executorApi")
	}
	executorapi.RegisterExecutorApiServer(grpcServer, executorServer)
	services = append(services, func() error {
		ctx.Infof("Executor api listening on %s", lis.Addr())
		return grpcServer.Serve(lis)
	})
	services = append(services, grpcCommon.CreateShutdownHandler(ctx, 5*time.Second, grpcServer))

	// ////////////////////////////////////////////////////////////////////////
	// Scheduler Reports
	// ////////////////////////////////////////////////////////////////////////
	schedulingContextRepository := reports.NewSchedulingContextRepository()
	reportServer := reports.NewServer(schedulingContextRepository)

	leaderClientConnectionProvider := leader.NewLeaderConnectionProvider(leaderController, config.Leader)
	schedulingReportServer := reports.NewLeaderProxyingSchedulingReportsServer(reportServer, leaderClientConnectionProvider)
	schedulerobjects.RegisterSchedulerReportingServer(grpcServer, schedulingReportServer)

	// ////////////////////////////////////////////////////////////////////////
	// Scheduling
	// ////////////////////////////////////////////////////////////////////////
	ctx.Infof("setting up scheduling loop")

	submitChecker := NewSubmitChecker(
		30*time.Minute,
		config.Scheduling,
		executorRepository,
	)
	services = append(services, func() error {
		return submitChecker.Run(ctx)
	})
	if err != nil {
		return errors.WithMessage(err, "error creating submit checker")
	}

	// Setup failure estimation and quarantining.
	failureEstimator, err := failureestimator.New(
		config.Scheduling.FailureProbabilityEstimation.NumInnerIterations,
		// Invalid config will have failed validation.
		descent.MustNew(config.Scheduling.FailureProbabilityEstimation.InnerOptimiserStepSize),
		// Invalid config will have failed validation.
		nesterov.MustNew(
			config.Scheduling.FailureProbabilityEstimation.OuterOptimiserStepSize,
			config.Scheduling.FailureProbabilityEstimation.OuterOptimiserNesterovAcceleration,
		),
	)
	if err != nil {
		return err
	}
	failureEstimator.Disable(config.Scheduling.FailureProbabilityEstimation.Disabled)
	if err := prometheus.Register(failureEstimator); err != nil {
		return errors.WithStack(err)
	}
	nodeQuarantiner, err := quarantine.NewNodeQuarantiner(
		config.Scheduling.NodeQuarantining.FailureProbabilityQuarantineThreshold,
		config.Scheduling.NodeQuarantining.FailureProbabilityEstimateTimeout,
		failureEstimator,
	)
	if err != nil {
		return err
	}
	if err := prometheus.Register(nodeQuarantiner); err != nil {
		return errors.WithStack(err)
	}
	queueQuarantiner, err := quarantine.NewQueueQuarantiner(
		config.Scheduling.QueueQuarantining.QuarantineFactorMultiplier,
		config.Scheduling.QueueQuarantining.FailureProbabilityEstimateTimeout,
		failureEstimator,
	)
	if err != nil {
		return err
	}
	if err := prometheus.Register(queueQuarantiner); err != nil {
		return errors.WithStack(err)
	}

	schedulingAlgo, err := NewFairSchedulingAlgo(
		config.Scheduling,
		config.MaxSchedulingDuration,
		executorRepository,
		queueRepository,
		schedulingContextRepository,
		nodeQuarantiner,
		queueQuarantiner,
	)
	if err != nil {
		return errors.WithMessage(err, "error creating scheduling algo")
	}
	jobDb := jobdb.NewJobDb(
		config.Scheduling.PriorityClasses,
		config.Scheduling.DefaultPriorityClassName,
		config.InternedStringsCacheSize,
	)
	schedulingRoundMetrics := NewSchedulerMetrics(config.Metrics.Metrics)
	if err := prometheus.Register(schedulingRoundMetrics); err != nil {
		return errors.WithStack(err)
	}
	schedulerMetrics, err := metrics.New(config.SchedulerMetrics)
	if err != nil {
		return err
	}
	if err := prometheus.Register(schedulerMetrics); err != nil {
		return errors.WithStack(err)
	}

	scheduler, err := NewScheduler(
		jobDb,
		jobRepository,
		executorRepository,
		schedulingAlgo,
		leaderController,
		pulsarPublisher,
		submitChecker,
		config.CyclePeriod,
		config.SchedulePeriod,
		config.ExecutorTimeout,
		config.Scheduling.MaxRetries+1,
		config.Scheduling.NodeIdLabel,
		schedulingRoundMetrics,
		schedulerMetrics,
		failureEstimator,
	)
	if err != nil {
		return errors.WithMessage(err, "error creating scheduler")
	}
	if config.Scheduling.EnableAssertions {
		scheduler.EnableAssertions()
	}
	services = append(services, func() error { return scheduler.Run(ctx) })

	// ////////////////////////////////////////////////////////////////////////
	// Metrics
	// ////////////////////////////////////////////////////////////////////////
	poolAssigner, err := NewPoolAssigner(config.Scheduling.ExecutorTimeout, config.Scheduling, executorRepository)
	if err != nil {
		return errors.WithMessage(err, "error creating pool assigner")
	}
	metricsCollector := NewMetricsCollector(
		scheduler.jobDb,
		queueRepository,
		executorRepository,
		poolAssigner,
		config.Metrics.RefreshInterval,
	)
	if err := prometheus.Register(metricsCollector); err != nil {
		return errors.WithStack(err)
	}
	services = append(services, func() error { return metricsCollector.Run(ctx) })
	shutdownMetricServer := common.ServeMetrics(config.Metrics.Port)
	defer shutdownMetricServer()

	// start all services
	for _, service := range services {
		g.Go(service)
	}

	// Mark startup as complete, will allow the health check to return healthy
	startupCompleteCheck.MarkComplete()

	return g.Wait()
}

func createLeaderController(ctx *armadacontext.Context, config schedulerconfig.LeaderConfig) (leader.LeaderController, error) {
	switch mode := strings.ToLower(config.Mode); mode {
	case "standalone":
		ctx.Infof("Scheduler will run in standalone mode")
		return leader.NewStandaloneLeaderController(), nil
	case "kubernetes":
		ctx.Infof("Scheduler will run kubernetes mode")
		clusterConfig, err := loadClusterConfig(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "Error creating kubernetes client")
		}
		clientSet, err := kubernetes.NewForConfig(clusterConfig)
		if err != nil {
			return nil, errors.Wrapf(err, "Error creating kubernetes client")
		}
		leaderController := leader.NewKubernetesLeaderController(config, clientSet.CoordinationV1())
		leaderStatusMetrics := leader.NewLeaderStatusMetricsCollector(config.PodName)
		leaderController.RegisterListener(leaderStatusMetrics)
		prometheus.MustRegister(leaderStatusMetrics)
		return leaderController, nil
	default:
		return nil, errors.Errorf("%s is not a value leader mode", config.Mode)
	}
}

func loadClusterConfig(ctx *armadacontext.Context) (*rest.Config, error) {
	config, err := rest.InClusterConfig()
	if err == rest.ErrNotInCluster {
		ctx.Info("Running with default client configuration")
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		overrides := &clientcmd.ConfigOverrides{}
		return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	}
	ctx.Info("Running with in cluster client configuration")
	return config, err
}
