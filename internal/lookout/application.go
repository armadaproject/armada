package lookout

import (
	"sync"

	"github.com/doug-martin/goqu/v9"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/grpc"
	"github.com/G-Research/armada/internal/common/health"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/lookout/configuration"
	"github.com/G-Research/armada/internal/lookout/metrics"
	"github.com/G-Research/armada/internal/lookout/postgres"
	"github.com/G-Research/armada/internal/lookout/repository"
	"github.com/G-Research/armada/internal/lookout/server"
	"github.com/G-Research/armada/pkg/api/lookout"
)

type LogRusLogger struct{}

func (l LogRusLogger) Printf(format string, v ...interface{}) {
	log.Debugf(format, v...)
}

func StartUp(config configuration.LookoutConfiguration, healthChecks *health.MultiChecker) (func(), *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	grpcServer := grpc.CreateGrpcServer(
		config.Grpc.KeepaliveParams,
		config.Grpc.KeepaliveEnforcementPolicy,
		[]authorization.AuthService{&authorization.AnonymousAuthService{}},
	)

	db, err := postgres.Open(config.Postgres)
	if err != nil {
		panic(err)
	}

	goquDb := goqu.New("postgres", db)
	goquDb.Logger(&LogRusLogger{})

	jobRepository := repository.NewSQLJobRepository(goquDb, &util.DefaultClock{})

	healthChecks.Add(repository.NewSqlHealth(db))

	dbMetricsProvider := metrics.NewLookoutSqlDbMetricsProvider(db, config.Postgres)
	metrics.ExposeLookoutMetrics(dbMetricsProvider)

	lookoutServer := server.NewLookoutServer(jobRepository)
	lookout.RegisterLookoutServer(grpcServer, lookoutServer)

	grpc_prometheus.Register(grpcServer)

	grpc.Listen(config.GrpcPort, grpcServer, wg)

	stop := func() {
		err = db.Close()
		if err != nil {
			log.Errorf("failed to close db connection: %v", err)
		}
		grpcServer.GracefulStop()
	}

	return stop, wg
}
