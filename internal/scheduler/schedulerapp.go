package scheduler

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/armadaproject/armada/internal/common/app"
	dbcommon "github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/scheduler/database"
)

func Run(config *Configuration) error {

	ctx := app.CreateContextWithShutdown()

	log.Infof("Setting up database connections")
	db, err := dbcommon.OpenPgxPool(config.Postgres)
	if err != nil {
		return errors.WithMessage(err, "Error opening connection to postgres")
	}
	jobRepository := database.NewPostgresJobRepository(db, int32(config.DatabaseFetchSize))
	executorRepository := database.NewPostgresExecutorRepository(db)

	redisClient := redis.NewUniversalClient(&redis.UniversalOptions{})
	queueRepository := database.NewLegacyQueueRepository(redisClient)

	log.Infof("Setting up Pulsar connectivity")
	pulsarClient, err := pulsarutils.NewPulsarClient(&config.Pulsar)
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
	clientSet, err := kubernetes.NewForConfig(clusterConfig)
	leaderController := NewKubernetesLeaderController(LeaderConfig{}, clientSet.CoordinationV1())
	go leaderController.Run(ctx)

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

	return scheduler.Run(ctx)
}
