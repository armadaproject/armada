package jobservice

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/armadaproject/armada/internal/common/auth/authorization"
	grpcCommon "github.com/armadaproject/armada/internal/common/grpc"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/jobservice/configuration"
	"github.com/armadaproject/armada/internal/jobservice/events"
	"github.com/armadaproject/armada/internal/jobservice/eventstojobs"
	"github.com/armadaproject/armada/internal/jobservice/repository"
	"github.com/armadaproject/armada/internal/jobservice/server"
	js "github.com/armadaproject/armada/pkg/api/jobservice"
)

type App struct {
	// Configuration for jobService
	Config *configuration.JobServiceConfiguration
}

func New() *App {
	return &App{}
}

func (a *App) StartUp(ctx context.Context, config *configuration.JobServiceConfiguration) error {
	// Setup an errgroup that cancels on any job failing or there being no active jobs.
	g, _ := errgroup.WithContext(ctx)

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

	// TODO: Bug on cleanup
	g.Go(func() error {
		PurgeJobSets(ctx, log, config.PurgeJobSetTime, sqlJobRepo)
		return nil
	})
	g.Go(func() error {
		ticker := time.NewTicker(30 * time.Second)
		eventClient := events.NewEventClient(&config.ApiConnection)
		for range ticker.C {

			jobSets, err := sqlJobRepo.GetSubscribedJobSets(ctx)
			log.Infof("job service has %d subscribed job sets", len(jobSets))
			if err != nil {
				logging.WithStacktrace(log, err).Warn("error getting jobsets")
			}
			for _, value := range jobSets {
				eventJob := eventstojobs.NewEventsToJobService(value.Queue, value.JobSet, eventClient, sqlJobRepo)
				go func(value repository.SubscribedTuple) {
					err := eventJob.SubscribeToJobSetId(context.Background(), config.SubscribeJobSetTime, value.FromMessageId)
					if err != nil {
						log.Error("error on subscribing", err)
					}
				}(value)
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

func PurgeJobSets(ctx context.Context, log *log.Entry, purgeJobSetTime int64,
	sqlJobRepo repository.SQLJobService,
) {
	log.Info("duration config: ", purgeJobSetTime)
	ticker := time.NewTicker(time.Duration(purgeJobSetTime) * time.Second)
	for range ticker.C {
		jobSets, err := sqlJobRepo.GetSubscribedJobSets(ctx)
		if err != nil {
			logging.WithStacktrace(log, err).Warn("error getting jobsets")
		}
		for _, value := range jobSets {
			log.Infof("subscribed job sets : %s", value)
			unsubscribe, err := sqlJobRepo.CheckToUnSubscribe(ctx, value.Queue, value.JobSet, purgeJobSetTime)
			if err != nil {
				log.WithError(err).Errorf("Unable to unsubscribe from queue/jobset %s/%s", value.Queue, value.JobSet)
			}
			if unsubscribe {
				_, err := sqlJobRepo.CleanupJobSetAndJobs(ctx, value.Queue, value.JobSet)
				if err != nil {
					logging.WithStacktrace(log, err).Warn("error cleaning up jobs")
				}
				_, err = sqlJobRepo.UnsubscribeJobSet(ctx, value.Queue, value.JobSet)
				if err != nil {
					log.WithError(err).Errorf("unable to delete queue/jobset %s/%s", value.Queue, value.JobSet)
				}
				log.Infof("deleted queue/jobset %s/%s", value.Queue, value.JobSet)
			}
		}
	}
}
