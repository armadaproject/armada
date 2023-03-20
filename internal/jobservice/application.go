package jobservice

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
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

	dbDir := filepath.Dir(config.DatabasePath)
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		if errMkDir := os.Mkdir(dbDir, 0o755); errMkDir != nil {
			log.Fatalf("error: could not make directory at %s for sqlite db: %v", dbDir, errMkDir)
		}
	}

	db, err := sql.Open("sqlite", config.DatabasePath)
	if err != nil {
		log.Fatalf("error opening sqlite DB from %s %v", config.DatabasePath, err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Warnf("error closing database: %v", err)
		}
	}()
	sqlJobRepo := repository.NewSQLJobService(config, db)
	sqlJobRepo.Setup()
	jobService := server.NewJobService(config, sqlJobRepo)
	js.RegisterJobServiceServer(grpcServer, jobService)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.GrpcPort))
	if err != nil {
		return err
	}

	g.Go(func() error {
		PurgeJobSets(log, config.PurgeJobSetTime, sqlJobRepo)
		return nil
	})
	g.Go(func() error {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {

			jobSets, err := sqlJobRepo.GetSubscribedJobSets()
			log.Infof("job service has %d subscribed job sets", len(jobSets))
			if err != nil {
				logging.WithStacktrace(log, err).Warn("error getting jobsets")
			}
			for _, value := range jobSets {
				log.Infof("subscribing to %s-%s for %d s", value.Queue, value.JobSet, config.SubscribeJobSetTime)
				eventClient := events.NewEventClient(&config.ApiConnection)
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

func PurgeJobSets(log *log.Entry, purgeJobSetTime int64, sqlJobRepo *repository.SQLJobService) {
	log.Info("duration config: ", purgeJobSetTime)
	ticker := time.NewTicker(time.Duration(purgeJobSetTime) * time.Second)
	for range ticker.C {
		jobSets, err := sqlJobRepo.GetSubscribedJobSets()
		if err != nil {
			logging.WithStacktrace(log, err).Warn("error getting jobsets")
		}
		for _, value := range jobSets {
			log.Infof("subscribed job sets : %s", value)
			unsubscribe, err := sqlJobRepo.CheckToUnSubscribe(value.Queue, value.JobSet, purgeJobSetTime)
			if err != nil {
				log.WithError(err).Errorf("Unable to unsubscribe from queue/jobset %s/%s", value.Queue, value.JobSet)
			}
			if unsubscribe {
				_, err := sqlJobRepo.CleanupJobSetAndJobs(value.Queue, value.JobSet)
				if err != nil {
					logging.WithStacktrace(log, err).Warn("error cleaning up jobs")
				}
				_, err = sqlJobRepo.UnsubscribeJobSet(value.Queue, value.JobSet)
				if err != nil {
					log.WithError(err).Errorf("unable to delete queue/jobset %s/%s", value.Queue, value.JobSet)
				}
				log.Infof("deleted queue/jobset %s/%s", value.Queue, value.JobSet)
			}
		}
	}
}
