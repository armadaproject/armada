package lookout

import (
	"strings"
	"sync"

	"github.com/doug-martin/goqu/v9"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/grpc"
	stanUtil "github.com/G-Research/armada/internal/common/stan-util"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/lookout/configuration"
	"github.com/G-Research/armada/internal/lookout/events"
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

func StartUp(config configuration.LookoutConfiguration) (func(), *sync.WaitGroup) {

	wg := &sync.WaitGroup{}
	wg.Add(1)

	grpcServer := grpc.CreateGrpcServer([]authorization.AuthService{&authorization.AnonymousAuthService{}})

	db, err := postgres.Open(config.Postgres)
	if err != nil {
		panic(err)
	}

	goquDb := goqu.New("postgres", db)
	goquDb.Logger(&LogRusLogger{})

	jobStore := repository.NewSQLJobStore(goquDb, config.UIConfig.UserAnnotationPrefix)
	jobRepository := repository.NewSQLJobRepository(goquDb, &repository.DefaultClock{})

	conn, err := stanUtil.DurableConnect(
		config.Nats.ClusterID,
		"armada-server-"+util.NewULID(),
		strings.Join(config.Nats.Servers, ","),
	)

	if err != nil {
		panic(err)
	}
	eventProcessor := events.NewEventProcessor(conn, jobStore, config.Nats.Subject, config.Nats.QueueGroup)
	eventProcessor.Start()

	dbMetricsProvider := metrics.NewLookoutSqlDbMetricsProvider(db, config.Postgres)
	metrics.ExposeLookoutMetrics(dbMetricsProvider)

	lookoutServer := server.NewLookoutServer(jobRepository)
	lookout.RegisterLookoutServer(grpcServer, lookoutServer)

	grpc_prometheus.Register(grpcServer)

	grpc.Listen(config.GrpcPort, grpcServer, wg)

	stop := func() {
		err := conn.Close()
		if err != nil {
			log.Errorf("failed to close nats connection: %v", err)
		}
		err = db.Close()
		if err != nil {
			log.Errorf("failed to close db connection: %v", err)
		}
		grpcServer.GracefulStop()
	}

	return stop, wg
}
