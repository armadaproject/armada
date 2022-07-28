package jobservice

import (
	"context"
	"fmt"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/common/auth/authorization"
	grpcCommon "github.com/G-Research/armada/internal/common/grpc"
	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/internal/jobservice/repository"

	"github.com/G-Research/armada/internal/jobservice/server"

	js "github.com/G-Research/armada/pkg/api/jobservice"
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

func (a *App) StartUp(ctx context.Context) error {
	config := a.Config

	// Setup an errgroup that cancels on any job failing or there being no active jobs.
	g, _ := errgroup.WithContext(ctx)

	grpcServer := grpcCommon.CreateGrpcServer(config.Grpc.KeepaliveParams, config.Grpc.KeepaliveEnforcementPolicy, []authorization.AuthService{&authorization.AnonymousAuthService{}})

	inMemoryMap := make(map[string]*repository.JobTable)
	subscribedJobSets := make(map[string]*repository.SubscribeTable)
	jobStatusMap := repository.NewJobStatus(inMemoryMap, subscribedJobSets)
	inMemoryJobService := repository.NewInMemoryJobServiceRepository(jobStatusMap, config)
	jobService := server.NewJobService(config, inMemoryJobService)
	js.RegisterJobServiceServer(grpcServer, jobService)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.GrpcPort))
	if err != nil { // TODO Don't call fatal, return an error.
		return err
	}

	g.Go(func() error {
		ticker := time.NewTicker(time.Duration(config.SubscribeJobSetTime) * time.Second)
		for range ticker.C {
			err := inMemoryJobService.PersistDataToDatabase()
			if err != nil {
				log.Warnf("Error Persisting data to database %v", err)
			}
		}
		return nil
	})
	g.Go(func() error {
		ticker := time.NewTicker(time.Duration(config.SubscribeJobSetTime) * time.Second)
		for range ticker.C {
			for _, value := range inMemoryJobService.GetSubscribedJobSets() {
				log.Infof("Subscribed job sets : %s", value)
				if inMemoryJobService.CheckToUnSubscribe(value, config.SubscribeJobSetTime) {
					inMemoryJobService.DeleteJobsInJobSet(value)
					inMemoryJobService.UnSubscribeJobSet(value)
				}
			}
		}
		return nil
	})
	g.Go(func() error {
		defer log.Println("Stopping server.")

		log.Info("JobService service listening on ", config.GrpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
			return err
		}
		return nil
	})

	g.Wait()

	return nil
}
