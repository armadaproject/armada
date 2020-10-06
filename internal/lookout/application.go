package lookout

import (
	"database/sql"
	"strings"
	"sync"

	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"

	stan_util "github.com/G-Research/armada/internal/common/stan-util"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/lookout/configuration"
	"github.com/G-Research/armada/internal/lookout/events"
	"github.com/G-Research/armada/internal/lookout/repository"
)

func StartUp(config configuration.LookoutConfiguration) (func(), *sync.WaitGroup) {

	wg := &sync.WaitGroup{}
	wg.Add(1)

	db, err := sql.Open("postgres", config.PostgresConnectionString)
	if err != nil {
		panic(err)
	}

	jobRepository := repository.NewSQLJobRepository(db)

	conn, err := stan_util.DurableConnect(
		config.Nats.ClusterID,
		"armada-server-"+util.NewULID(),
		strings.Join(config.Nats.Servers, ","),
	)

	if err != nil {
		panic(err)
	}
	eventProcessor := events.NewEventProcessor(conn, jobRepository, config.Nats.Subject, config.Nats.QueueGroup)
	eventProcessor.Start()

	stop := func() {
		err := conn.Close()
		if err != nil {
			log.Errorf("failed to close nats connection: %v", err)
		}
		err = db.Close()
		if err != nil {
			log.Errorf("failed to close db connection: %v", err)
		}
		wg.Done()
	}

	return stop, wg
}
