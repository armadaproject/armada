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

	subscribedJobSets := make(map[string]*repository.SubscribeTable)
	jobStatusMap := repository.NewJobSetSubscriptions(subscribedJobSets)

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
	sqlJobRepo := repository.NewSQLJobService(jobStatusMap, config, db)
	sqlJobRepo.Setup()
	jobService := server.NewJobService(config, sqlJobRepo)
	js.RegisterJobServiceServer(grpcServer, jobService)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.GrpcPort))
	if err != nil {
		return err
	}

	g.Go(func() error {
		ticker := time.NewTicker(time.Duration(config.SubscribeJobSetTime) * time.Second)
		for range ticker.C {
			for _, value := range sqlJobRepo.GetSubscribedJobSets() {
				log.Infof("subscribed job sets : %s", value)
				if sqlJobRepo.CheckToUnSubscribe(value.Queue, value.JobSet, config.CleanupJobSet) {
					_, err := sqlJobRepo.CleanupJobSetAndJobs(value.Queue, value.JobSet)
					if err != nil {
						logging.WithStacktrace(log, err).Warn("error cleaning up jobs")
					}
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
