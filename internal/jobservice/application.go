package jobservice

import (
	"sync"

	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/auth/authorization"
	grpcCommon "github.com/G-Research/armada/internal/common/grpc"
	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/internal/jobservice/repository"

	"github.com/G-Research/armada/internal/jobservice/server"

	"github.com/G-Research/armada/pkg/api/jobservice"
)

type App struct {
	// Configuration for jobService
	Config *configuration.JobServiceConfiguration
}

func New(config *configuration.JobServiceConfiguration) *App {
	return &App{
		Config: config,
	}
}

func (a *App) StartUp() (func(), *sync.WaitGroup) {
	config := a.Config
	log.Info("Armada jobService service starting")

	wg := sync.WaitGroup{}
	wg.Add(1)

	db := createRedisClient(&config.Redis)
	defer func() {
		if err := db.Close(); err != nil {
			log.WithError(err).Error("failed to close Redis client")
		}
	}()

	grpcServer := grpcCommon.CreateGrpcServer(config.Grpc.KeepaliveParams, config.Grpc.KeepaliveEnforcementPolicy, []authorization.AuthService{&authorization.AnonymousAuthService{}})
	log.Info("JobService service listening on ", config.GrpcPort)

	log.Infof("JobService using armadaurl of %s", config.ApiConnection.ArmadaUrl)
	redisJobRepository := repository.NewRedisJobServiceRepository(db, config.CacheTimeToLive)
	jobService := server.NewJobService(config, *redisJobRepository)
	jobservice.RegisterJobServiceServer(grpcServer, jobService)

	grpcCommon.Listen(config.GrpcPort, grpcServer, &wg)

	wg.Wait()
	stop := func() {
		grpcServer.GracefulStop()
	}

	return stop, &wg
}

func createRedisClient(config *redis.UniversalOptions) redis.UniversalClient {
	return redis.NewUniversalClient(config)
}
