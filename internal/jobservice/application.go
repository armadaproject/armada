package jobservice

import (
	"context"
	"fmt"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/armadaproject/armada/internal/common/auth/authorization"
	grpcCommon "github.com/armadaproject/armada/internal/common/grpc"
	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
	"github.com/armadaproject/armada/internal/common/grpc/grpcpool"
	"github.com/armadaproject/armada/internal/jobservice/configuration"
	"github.com/armadaproject/armada/internal/jobservice/events"
	"github.com/armadaproject/armada/internal/jobservice/eventstojobs"
	"github.com/armadaproject/armada/internal/jobservice/repository"
	"github.com/armadaproject/armada/internal/jobservice/server"
	js "github.com/armadaproject/armada/pkg/api/jobservice"
	"github.com/armadaproject/armada/pkg/client"
)

type App struct {
	// Configuration for jobService
	Config *configuration.JobServiceConfiguration
}

func New() *App {
	return &App{}
}

var DefaultConfiguration = &configuration.JobServiceConfiguration{
	GrpcPool: grpcconfig.GrpcPoolConfig{
		InitialConnections: 5,
		Capacity:           5,
	},
	SubscriberPoolSize: 30,
}

// Mutates config where possible to correct mis-configurations.
func RectifyConfig(config *configuration.JobServiceConfiguration) {
	logger := log.WithField("JobService", "RectifyConfig")

	// Grpc Pool
	if config.GrpcPool.InitialConnections <= 0 {
		logger.WithFields(log.Fields{
			"default":    DefaultConfiguration.GrpcPool.InitialConnections,
			"configured": config.GrpcPool.InitialConnections,
		}).Warn("config.GrpcPool.InitialConnections invalid, using default instead")
		config.GrpcPool.InitialConnections = DefaultConfiguration.GrpcPool.InitialConnections
	}
	if config.GrpcPool.Capacity <= 0 {
		logger.WithFields(log.Fields{
			"default":    DefaultConfiguration.GrpcPool.Capacity,
			"configured": config.GrpcPool.Capacity,
		}).Warn("config.GrpcPool.Capacity invalid, using default instead")
		config.GrpcPool.Capacity = DefaultConfiguration.GrpcPool.Capacity
	}

	if config.SubscriberPoolSize <= 0 {
		logger.WithFields(log.Fields{
			"default":    DefaultConfiguration.SubscriberPoolSize,
			"configured": config.SubscriberPoolSize,
		}).Warn("config.SubscriberPoolSize invalid, using default instead")
		config.SubscriberPoolSize = DefaultConfiguration.SubscriberPoolSize
	}

	if config.ApiConnection.ForceNoTls {
		logger.Warn("Armada Server connection will be unsecured! TLS is forced OFF!")
	}

	return
}

func (a *App) StartUp(ctx context.Context, config *configuration.JobServiceConfiguration) error {
	// Setup an errgroup that cancels on any job failing or there being no active jobs.
	g, _ := errgroup.WithContext(ctx)

	RectifyConfig(config)

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
	evConnPool, err := grpcpool.NewWithContext(ctx, connFactory,
		config.GrpcPool.InitialConnections,
		config.GrpcPool.Capacity,
		0)
	if err != nil {
		return err
	}

	g.Go(func() error {
		eventClient := events.NewPooledEventClient(evConnPool)
		// Runs continuously until ctx is canceled or it runs into an unrecoverable error
		jobSubExecutor := eventstojobs.NewJobSetSubscriptionExecutor(
			ctx,
			eventClient,
			sqlJobRepo,
			jobService.GetNewSubscriptionChannel(),
			time.Duration(config.SubscribeJobSetTime)*time.Second)
		jobSubExecutor.Manage()
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
	g.Go(func() error {
		sqlJobRepo.PurgeExpiredJobSets(ctx)
		return nil
	})

	if err := g.Wait(); err != nil {
		log.Fatalf("error detected on wait %v", err)
		return err
	}

	return nil
}
