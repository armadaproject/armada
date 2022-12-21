package jobservice

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/auth/authorization"
	grpcCommon "github.com/G-Research/armada/internal/common/grpc"
	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/internal/jobservice/repository"

	"github.com/G-Research/armada/internal/common/logging"

	"github.com/G-Research/armada/internal/jobservice/server"

	js "github.com/G-Research/armada/pkg/api/jobservice"
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
		err = os.Mkdir(dbDir, 0o755)
		if err != nil {
			log.Fatalf("error: could not make directory at %s for sqlite db: %v", dbDir, err)
		}
	}

	db, err := sql.Open("sqlite", config.DatabasePath)
	if err != nil {
		log.Fatalf("error opening sqlite DB from %s %v", config.DatabasePath, err)
	}
	defer func() {
		err := db.Close()
		if err != nil {
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
				if sqlJobRepo.CheckToUnSubscribe(value.Queue, value.JobSet, config.SubscribeJobSetTime) {
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

		log.Info("jobService service listening on ", config.GrpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
		return nil
	})

	err = g.Wait()
	if err != nil {
		log.Fatalf("error detected on wait %v", err)
		return err
	}

	return nil
}
