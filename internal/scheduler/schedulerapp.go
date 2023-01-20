package scheduler

import (
	"fmt"
	"net"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/armadaproject/armada/internal/common/app"
	"github.com/armadaproject/armada/internal/common/auth"
	dbcommon "github.com/armadaproject/armada/internal/common/database"
	grpcCommon "github.com/armadaproject/armada/internal/common/grpc"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/pkg/executorapi"
)

// Run sets up a Scheduler application and runs it until a SIGTERM is received
func Run(config *Configuration) error {
	g, ctx := errgroup.WithContext(app.CreateContextWithShutdown())

	log.Infof("Setting up database connections")
	db, err := dbcommon.OpenPgxPool(config.Postgres)
	if err != nil {
		return errors.WithMessage(err, "Error opening connection to postgres")
	}
	jobRepository := database.NewPostgresJobRepository(db, int32(config.DatabaseFetchSize))
	executorRepository := database.NewPostgresExecutorRepository(db)

	redisClient := redis.NewUniversalClient(&redis.UniversalOptions{})
	defer func() {
		err := redisClient.Close()
		if err != nil {
			log.WithError(errors.WithStack(err)).Warnf("Redis client didn't close down cleanly")
		}
	}()
	queueRepository := database.NewLegacyQueueRepository(redisClient)

	log.Infof("Setting up Pulsar connectivity")
	pulsarClient, err := pulsarutils.NewPulsarClient(&config.Pulsar)
	defer pulsarClient.Close()
	if err != nil {
		return errors.WithMessage(err, "Error creating pulsar client")
	}
	pulsarCompressionType, err := pulsarutils.ParsePulsarCompressionType(config.Pulsar.CompressionType)
	if err != nil {
		return err
	}
	pulsarCompressionLevel, err := pulsarutils.ParsePulsarCompressionLevel(config.Pulsar.CompressionLevel)
	if err != nil {
		return err
	}
	pulsarPublisher, err := NewPulsarPublisher(pulsarClient, pulsar.ProducerOptions{
		Name:             "",
		CompressionType:  pulsarCompressionType,
		CompressionLevel: pulsarCompressionLevel,
		BatchingMaxSize:  config.Pulsar.MaxAllowedMessageSize,
		Topic:            config.Pulsar.JobsetEventsTopic,
	}, config.PulsarSendTimeout)
	if err != nil {
		return errors.WithMessage(err, "Error creating pulsar publisher")
	}

	log.Infof("Setting up scheduling algorithm")
	schedulingAlgo := NewLegacySchedulingAlgo(config.Scheduling, executorRepository, queueRepository)

	log.Infof("Setting up leader election")
	clusterConfig, err := rest.InClusterConfig()
	if err != nil {
		return errors.Wrapf(err, "Error creating kubernetes client")
	}
	clientSet, err := kubernetes.NewForConfig(clusterConfig)
	if err != nil {
		return errors.Wrapf(err, "Error creating kubernetes client")
	}
	leaderController := NewKubernetesLeaderController(LeaderConfig{}, clientSet.CoordinationV1())
	g.Go(func() error { return leaderController.Run(ctx) })

	log.Infof("Setting up executor api")
	apiProducer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Name:             fmt.Sprintf("armada-executor-api-%s", uuid.New()),
		CompressionType:  pulsarCompressionType,
		CompressionLevel: pulsarCompressionLevel,
		BatchingMaxSize:  config.Pulsar.MaxAllowedMessageSize,
		Topic:            config.Pulsar.JobsetEventsTopic,
	})
	if err != nil {
		return errors.Wrapf(err, "Error creating pulsar producer for executor api")
	}
	authServices := auth.ConfigureAuth(config.Auth)
	grpcServer := grpcCommon.CreateGrpcServer(config.Grpc.KeepaliveParams, config.Grpc.KeepaliveEnforcementPolicy, authServices)
	defer grpcServer.GracefulStop()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Grpc.Port))
	if err != nil {
		return errors.WithMessage(err, "Error setting up grpc server")
	}
	executorServer := NewExecutorApi(apiProducer, jobRepository, executorRepository, []int32{}, config.MaxedLeasedJobsPerCall)
	executorapi.RegisterExecutorApiServer(grpcServer, executorServer)
	g.Go(func() error { return grpcServer.Serve(lis) })
	log.Infof("Executor api listening on %s", lis.Addr())

	log.Infof("Starting up scheduling loop")
	scheduler, err := NewScheduler(jobRepository,
		executorRepository,
		schedulingAlgo,
		leaderController,
		pulsarPublisher,
		config.cyclePeriod,
		config.executorTimeout,
		uint(config.MaxFailedLeaseReturns))
	if err != nil {
		return errors.WithMessage(err, "Error creating scheduler")
	}
	g.Go(func() error { return scheduler.Run(ctx) })

	return g.Wait()
}
