package eventingester

import (
	"regexp"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/app"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/jobsetevents"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/profiling"
	"github.com/armadaproject/armada/internal/eventingester/configuration"
	"github.com/armadaproject/armada/internal/eventingester/convert"
	"github.com/armadaproject/armada/internal/eventingester/metrics"
	"github.com/armadaproject/armada/internal/eventingester/store"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/leader"
	"github.com/armadaproject/armada/internal/server/redismetrics"
)

func registerCollector(collector prometheus.Collector, collectorName string) error {
	err := prometheus.Register(collector)
	if err == nil {
		return nil
	}

	if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
		log.Infof("Prometheus collector %s already registered; skipping duplicate registration", collectorName)
		return nil
	}

	return errors.WithMessagef(err, "failed to register collector %s", collectorName)
}

// Run will create a pipeline that will take Armada event messages from Pulsar and update the
// Events database accordingly.  This pipeline will run until a SIGTERM is received
func Run(config *configuration.EventIngesterConfiguration) {
	log.Info("Event Ingester Starting")

	// Expose profiling endpoints if enabled.
	err := profiling.SetupPprof(config.Profiling, armadacontext.Background(), nil)
	if err != nil {
		log.Fatalf("Pprof setup failed, exiting, %v", err)
	}

	metrics := metrics.Get()

	fatalRegexes := make([]*regexp.Regexp, len(config.FatalInsertionErrors))
	for i, str := range config.FatalInsertionErrors {
		rgx, err := regexp.Compile(str)
		if err != nil {
			log.Errorf("Error compiling regex %s", str)
			panic(err)
		}
		fatalRegexes[i] = rgx
	}

	db := redis.NewUniversalClient(&config.Redis)
	defer func() {
		if err := db.Close(); err != nil {
			log.WithError(err).Error("failed to close events Redis client")
		}
	}()

	dbs := []redis.UniversalClient{db}
	dbNames := []string{"main"}

	if len(config.RedisReplica.Addrs) > 0 {
		db2 := redis.NewUniversalClient(&config.RedisReplica)
		defer func() {
			if err := db2.Close(); err != nil {
				log.WithError(err).Error("failed to close events Redis replica client")
			}
		}()
		dbs = append(dbs, db2)
		dbNames = append(dbNames, "replica")
	}

	eventDb := store.NewRedisEventStore(dbs, dbNames, config.EventRetentionPolicy, fatalRegexes, 100*time.Millisecond, 60*time.Second)

	ctx := app.CreateContextWithShutdown()
	g, _ := errgroup.WithContext(ctx.Context)

	if config.Metrics.Redis.Enabled {
		var metricsRedisClient redis.UniversalClient
		if config.Metrics.Redis.ConnectionInfo.Addrs != nil {
			metricsRedisClient = redis.NewUniversalClient(&config.Metrics.Redis.ConnectionInfo)
			defer func() {
				if err := metricsRedisClient.Close(); err != nil {
					log.WithError(err).Error("failed to close metrics Redis client")
				}
			}()
		} else {
			metricsRedisClient = db
		}

		metricsConfig := redismetrics.Config{
			CollectionInterval: config.Metrics.Redis.CollectionInterval,
			TopN:               config.Metrics.Redis.TopN,
			ScanBatchSize:      config.Metrics.Redis.ScanBatchSize,
			PipelineBatchSize:  config.Metrics.Redis.PipelineBatchSize,
			InterBatchDelay:    config.Metrics.Redis.InterBatchDelay,
			MemoryUsageSamples: config.Metrics.Redis.MemoryUsageSamples,
		}

		scanner := redismetrics.NewScanner(metricsRedisClient, metricsConfig)

		leaderController, err := createLeaderController(ctx, config.Metrics.Redis.Leader)
		if err != nil {
			log.Fatalf("failed to create leader controller for redis metrics: %v", err)
		}

		collector := redismetrics.NewCollector(scanner, metricsConfig, leaderController)
		if err := registerCollector(collector, "redis usage metrics"); err != nil {
			log.Fatalf("%v", err)
		}

		g.Go(func() error {
			return leaderController.Run(ctx)
		})

		g.Go(func() error {
			return collector.Run(ctx)
		})
	}

	// Turn the messages into event rows
	compressor, err := compress.NewZlibCompressor(config.MinMessageCompressionSize)
	if err != nil {
		log.Errorf("Error creating compressor for consumer")
		panic(err)
	}
	converter := convert.NewEventConverter(compressor, uint(config.MaxOutputMessageSizeBytes), metrics)

	// Start metric server
	shutdownMetricServer := common.ServeMetrics(config.MetricsPort)
	defer shutdownMetricServer()

	ingester := ingest.NewIngestionPipeline(
		config.Pulsar,
		config.Pulsar.JobsetEventsTopic,
		config.SubscriptionName,
		config.BatchSize,
		config.BatchDuration,
		pulsar.Failover,
		jobsetevents.EventCounter,
		jobsetevents.MessageUnmarshaller,
		jobsetevents.BatchMerger,
		jobsetevents.BatchMetricPublisher,
		converter,
		eventDb,
		metrics,
	)

	g.Go(func() error {
		return ingester.Run(ctx)
	})

	if err := g.Wait(); err != nil {
		panic(errors.WithMessage(err, "Error running event ingester services"))
	}
}

func createLeaderController(ctx *armadacontext.Context, config configuration.LeaderConfig) (leader.LeaderController, error) {
	switch mode := strings.ToLower(config.Mode); mode {
	case "standalone":
		ctx.Infof("Redis metrics will run in standalone mode")
		return leader.NewStandaloneLeaderController(), nil
	case "kubernetes":
		ctx.Infof("Redis metrics will run kubernetes mode")
		clusterConfig, err := loadClusterConfig(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating kubernetes client")
		}
		clientSet, err := kubernetes.NewForConfig(clusterConfig)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating kubernetes client")
		}

		schedulerLeaderConfig := schedulerconfig.LeaderConfig{
			Mode:               config.Mode,
			LeaseLockName:      config.LeaseLockName,
			LeaseLockNamespace: config.LeaseLockNamespace,
			LeaseDuration:      config.LeaseDuration,
			RenewDeadline:      config.RenewDeadline,
			RetryPeriod:        config.RetryPeriod,
		}

		leaderController := leader.NewKubernetesLeaderController(schedulerLeaderConfig, clientSet.CoordinationV1())
		leaderStatusMetrics := leader.NewLeaderStatusMetricsCollector(config.LeaseLockName)
		leaderController.RegisterListener(leaderStatusMetrics)
		if err := registerCollector(leaderStatusMetrics, "redis metrics leader status"); err != nil {
			return nil, err
		}
		return leaderController, nil
	default:
		return nil, errors.Errorf("%s is not a valid leader mode", config.Mode)
	}
}

func loadClusterConfig(ctx *armadacontext.Context) (*rest.Config, error) {
	config, err := rest.InClusterConfig()
	if errors.Is(err, rest.ErrNotInCluster) {
		ctx.Info("Running with default client configuration")
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		overrides := &clientcmd.ConfigOverrides{}
		return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	}
	ctx.Info("Running with in cluster client configuration")
	return config, err
}
