package scheduler

import (
	"fmt"
	"net"
	"strings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/armadaproject/armada/internal/common/app"
	"github.com/armadaproject/armada/internal/common/auth"
	dbcommon "github.com/armadaproject/armada/internal/common/database"
	grpcCommon "github.com/armadaproject/armada/internal/common/grpc"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/pkg/executorapi"
)

// Run sets up a Scheduler application and runs it until a SIGTERM is received
func Run(config Configuration) error {
	g, ctx := errgroup.WithContext(app.CreateContextWithShutdown())

	//////////////////////////////////////////////////////////////////////////
	// Database setup (postgres and redis)
	//////////////////////////////////////////////////////////////////////////
	log.Infof("Setting up database connections")
	db, err := dbcommon.OpenPgxPool(config.Postgres)
	if err != nil {
		return errors.WithMessage(err, "Error opening connection to postgres")
	}
	jobRepository := database.NewPostgresJobRepository(db, int32(config.DatabaseFetchSize))
	executorRepository := database.NewPostgresExecutorRepository(db)

	redisClient := redis.NewUniversalClient(config.Redis.AsUniversalOptions())
	defer func() {
		err := redisClient.Close()
		if err != nil {
			log.WithError(errors.WithStack(err)).Warnf("Redis client didn't close down cleanly")
		}
	}()
	queueRepository := database.NewLegacyQueueRepository(redisClient)

	//////////////////////////////////////////////////////////////////////////
	// Pulsar
	//////////////////////////////////////////////////////////////////////////
	log.Infof("Setting up Pulsar connectivity")
	pulsarClient, err := pulsarutils.NewPulsarClient(&config.Pulsar)
	defer pulsarClient.Close()
	if err != nil {
		return errors.WithMessage(err, "Error creating pulsar client")
	}
	pulsarPublisher, err := NewPulsarPublisher(pulsarClient, pulsar.ProducerOptions{
		Name:             fmt.Sprintf("armada-scheduler-%s", uuid.New()),
		CompressionType:  config.Pulsar.CompressionType,
		CompressionLevel: config.Pulsar.CompressionLevel,
		BatchingMaxSize:  config.Pulsar.MaxAllowedMessageSize,
		Topic:            config.Pulsar.JobsetEventsTopic,
	}, config.PulsarSendTimeout)
	if err != nil {
		return errors.WithMessage(err, "Error creating pulsar publisher")
	}

	//////////////////////////////////////////////////////////////////////////
	// Leader Election
	//////////////////////////////////////////////////////////////////////////
	leaderController, err := createLeaderController(config.Leader)
	if err != nil {
		return errors.WithMessage(err, "Error creating leader controller")
	}
	g.Go(func() error { return leaderController.Run(ctx) })

	//////////////////////////////////////////////////////////////////////////
	// Executor Api
	//////////////////////////////////////////////////////////////////////////
	log.Infof("Setting up executor api")
	apiProducer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Name:             fmt.Sprintf("armada-executor-api-%s", uuid.New()),
		CompressionType:  config.Pulsar.CompressionType,
		CompressionLevel: config.Pulsar.CompressionLevel,
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
	executorServer := NewExecutorApi(apiProducer, jobRepository, executorRepository, []int32{}, config.Scheduling.MaximumJobsToSchedule)
	executorapi.RegisterExecutorApiServer(grpcServer, executorServer)
	g.Go(func() error { return grpcServer.Serve(lis) })
	log.Infof("Executor api listening on %s", lis.Addr())

	//////////////////////////////////////////////////////////////////////////
	// Scheduling
	//////////////////////////////////////////////////////////////////////////
	log.Infof("Starting up scheduling loop")
	schedulingAlgo := NewLegacySchedulingAlgo(config.Scheduling, executorRepository, queueRepository)
	scheduler, err := NewScheduler(jobRepository,
		executorRepository,
		schedulingAlgo,
		leaderController,
		pulsarPublisher,
		config.CyclePeriod,
		config.ExecutorTimeout,
		config.Scheduling.MaxRetries)
	if err != nil {
		return errors.WithMessage(err, "Error creating scheduler")
	}
	g.Go(func() error { return scheduler.Run(ctx) })

	return g.Wait()
}

func createLeaderController(config LeaderConfig) (LeaderController, error) {
	switch mode := strings.ToLower(config.Mode); mode {
	case "standalone":
		log.Infof("Scheduler will run in standalone mode")
		return NewStandaloneLeaderController(), nil
	case "cluster":
		log.Infof("Scheduler will run cluster mode")
		clusterConfig, err := rest.InClusterConfig()
		if err != nil {
			return nil, errors.Wrapf(err, "Error creating kubernetes client")
		}
		clientSet, err := kubernetes.NewForConfig(clusterConfig)
		if err != nil {
			return nil, errors.Wrapf(err, "Error creating kubernetes client")
		}
		return NewKubernetesLeaderController(LeaderConfig{}, clientSet.CoordinationV1()), nil
	default:
		return nil, errors.New(fmt.Sprintf("%s is not a value leader mode", config.Mode))
	}
}
