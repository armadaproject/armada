package jobservice

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/armadaproject/armada/internal/common/auth/authorization"
	grpcCommon "github.com/armadaproject/armada/internal/common/grpc"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/jobservice/configuration"
	"github.com/armadaproject/armada/internal/jobservice/events"
	"github.com/armadaproject/armada/internal/jobservice/eventstojobs"
	"github.com/armadaproject/armada/internal/jobservice/repository"
	"github.com/armadaproject/armada/internal/jobservice/server"
	js "github.com/armadaproject/armada/pkg/api/jobservice"
	"github.com/armadaproject/armada/pkg/client"

	grpcpool "github.com/ClifHouck/grpc-go-pool"
)

type App struct {
	// Configuration for jobService
	Config *configuration.JobServiceConfiguration
}

func New() *App {
	return &App{}
}

// Returns a non-nil error if mis-configuration is unrecoverable.
func CheckConfig(config *configuration.JobServiceConfiguration) error {
	logger := log.WithField("JobService", "CheckConfig")

	// Grpc Pool
	if config.GrpcPool.InitialConnections <= 0 {
		defIC := 5
		logger.WithFields(log.Fields{
			"default":    defIC,
			"configured": config.GrpcPool.InitialConnections,
		}).Warn("config.GrpcPool.InitialConnections invalid, using default instead")
		config.GrpcPool.InitialConnections = 5
	}
	if config.GrpcPool.Capacity <= 0 {
		defaultCapacity := 5
		logger.WithFields(log.Fields{
			"default":    defaultCapacity,
			"configured": config.GrpcPool.Capacity,
		}).Warn("config.GrpcPool.Capacity invalid, using default instead")
		config.GrpcPool.Capacity = defaultCapacity
	}

	return nil
}

func (a *App) StartUp(ctx context.Context, config *configuration.JobServiceConfiguration) error {
	// Setup an errgroup that cancels on any job failing or there being no active jobs.
	g, _ := errgroup.WithContext(ctx)

	err := CheckConfig(config)
	if err != nil {
		panic(err)
	}

	log := log.WithField("JobService", "Startup")
	grpcServer := grpcCommon.CreateGrpcServer(
		config.Grpc.KeepaliveParams,
		config.Grpc.KeepaliveEnforcementPolicy,
		[]authorization.AuthService{&authorization.AnonymousAuthService{}},
	)

	err, sqlJobRepo, dbCallbackFn := repository.NewSQLJobService(config, log)
	if err != nil {
		panic(err)
	}
	defer dbCallbackFn()
	sqlJobRepo.Setup(ctx)
	jobService := server.NewJobService(config, sqlJobRepo)
	js.RegisterJobServiceServer(grpcServer, jobService)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.GrpcPort))
	if err != nil {
		return err
	}

	connFactory := func(ctx context.Context) (*grpc.ClientConn, error) {
		return client.CreateApiConnection(&config.ApiConnection)
	}

	// Start a pool
	pool, err := grpcpool.NewWithContext(ctx, connFactory,
		config.GrpcPool.InitialConnections,
		config.GrpcPool.Capacity,
		0)
	if err != nil {
		return err
	}

	// This function runs in the background every 30 seconds
	// We will loop over the subscribed jobsets
	// And we check if we have already subscribed via subscribeMap
	// If we have then we skip that jobset
	g.Go(func() error {
		ticker := time.NewTicker(30 * time.Second)
		eventClient := events.NewPooledEventClient(pool)
		var subscribeMap sync.Map
		for range ticker.C {

			jobSets, err := sqlJobRepo.GetSubscribedJobSets(ctx)
			log.Infof("job service has %d subscribed job sets", len(jobSets))
			if err != nil {
				logging.WithStacktrace(log, err).Warn("error getting jobsets")
			}
			for _, value := range jobSets {
				queueJobSet := value.Queue + value.JobSet
				_, ok := subscribeMap.LoadOrStore(queueJobSet, true)
				if !ok {
					eventJob := eventstojobs.NewEventsToJobService(value.Queue, value.JobSet, eventClient, sqlJobRepo)
					go func(value repository.SubscribedTuple, subscribe sync.Map) {
						err := eventJob.SubscribeToJobSetId(context.Background(), config.SubscribeJobSetTime, value.FromMessageId)
						if err != nil {
							log.Error("error on subscribing", err)
						}
						queueJobSet := value.Queue + value.JobSet
						log.Infof("deleting %s from map", queueJobSet)
						subscribe.Delete(queueJobSet)
					}(value, subscribeMap)
				} else {
					log.Infof("job set %s/%s is subscribed", value.Queue, value.JobSet)
				}
			}
		}
		return nil
	})
	g.Go(func() error {
		defer log.Infof("stopping server.")

		log.Info("jobservice service listening on ", config.GrpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		log.Fatalf("error detected on wait %v", err)
		return err
	}

	return nil
}
