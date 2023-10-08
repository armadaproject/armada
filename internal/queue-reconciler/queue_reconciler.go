package main

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"syscall"
	"time"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/queue"
)

// Config represents the application config
type Config struct {
	// QueuesDir is a filesystem directory containing queue files
	QueuesDir string
	// ReconcileFrequency is the frequency at which the reconciliation loop will be run
	ReconcileFrequency time.Duration
	// Redis contains redis connection details. It should point at the redis instance where Armada stores Queues
	Redis redis.UniversalOptions
	// Armada represents armada connection details
	Armada client.ApiConnectionDetails
}

// Run starts the reconciliation loop.  It will run until a shutdown signal (SIGINT, SIGTERM is received)
func Run(config Config) {
	ctx := createContextWithShutdown()

	// Armada Api client
	conn, err := client.CreateApiConnection(&config.Armada)
	if err != nil {
		log.WithError(err).Fatal("Error creating client connection")
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			log.WithError(err).Warnf("Armada api client didn't close down cleanly")
		}
	}()
	armadaClient := api.NewSubmitClient(conn)

	// Redis
	redisClient := redis.NewUniversalClient(&config.Redis)
	defer func() {
		err := redisClient.Close()
		if err != nil {
			log.WithError(err).Warnf("Redis client didn't close down cleanly")
		}
	}()

	log.Infof("Staerting reconciliation loop.  Will reconcile queues every %s", config.ReconcileFrequency)
	ticker := time.NewTicker(config.ReconcileFrequency)
	for {
		select {
		case <-ctx.Done():
			log.Infof("context cancelled; returning.")
			return
		case <-ticker.C:
			reconcile(ctx, config.QueuesDir, redisClient, armadaClient)
		}
	}
}

// reconcile represents a single iteration of the reconciliation loop.  The logic is:
//   - Read desired queue state from queue files
//   - Read existing queue state from redis
//   - Produce a list of queues that exist in the queue files but either do not exist on the server or are different on the server
//   - Call the Armada api to create the missing queues
//
// Not the above logic means that new queues may be created and existing queues modified, but old queues wil not be deleted.
func reconcile(ctx context.Context, queuesDir string, redisClient redis.UniversalClient, armadaClient api.SubmitClient) {
	start := time.Now()
	log.Infof("Starting reconciliation")

	desiredQueues, err := fetchQueuesFromFiles(queuesDir)
	if err != nil {
		log.WithError(err).Warnf("error retrieving queues")
		return
	}

	existingQueues, err := fetchQueuesFromRedis(redisClient)
	if err != nil {
		log.WithError(err).Warnf("error fetching queues from redis")
		return
	}

	queuesToCreate := diff(desiredQueues, existingQueues)
	if len(queuesToCreate) < 1 {
		log.Infof("No changes detected.  Reconciliation completed in %s", time.Since(start))
		return
	}

	err = createQueues(ctx, armadaClient, queuesToCreate)
	if err != nil {
		log.WithError(err).Warnf("error creating queues")
		return
	}

	log.Infof("Reconciliation successful in %s", time.Since(start))
}

// createQueues calls the armada api to create all queues
func createQueues(ctx context.Context, client api.SubmitClient, queues []*api.Queue) error {
	_, err := client.CreateQueues(ctx, &api.QueueList{Queues: queues})
	if err != nil {
		return err
	}
	return nil
}

// diff returns all queues that exist in desired but are not identical in actual
func diff(desired []*api.Queue, actual []*api.Queue) []*api.Queue {
	queuesToCreate := make([]*api.Queue, 0)
	actualByName := make(map[string]*api.Queue, len(actual))
	for _, q := range actual {
		actualByName[q.Name] = q
	}
	for _, desiredQueue := range desired {
		actualQueue, ok := actualByName[desiredQueue.Name]
		if !ok || !reflect.DeepEqual(actualQueue, desiredQueue) {
			queuesToCreate = append(queuesToCreate, desiredQueue)
		}
	}
	return queuesToCreate
}

// fetchQueuesFromFiles returns all queues present in yaml files in dir
func fetchQueuesFromFiles(dir string) ([]*api.Queue, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.y[a]*ml"))
	if err != nil {
		return nil, err
	}
	var results []*api.Queue
	for _, file := range files {
		content, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		var q queue.Queue
		if err := yaml.Unmarshal(content, &q); err != nil {
			return nil, err
		}
		results = append(results, q.ToAPI())
	}

	return results, nil
}

// fetchQueuesFromRedis returns all armada queues from redis
func fetchQueuesFromRedis(rc redis.UniversalClient) ([]*api.Queue, error) {
	result, err := rc.HGetAll("Queue").Result()
	if err != nil {
		return nil, err
	}
	queues := make([]*api.Queue, 0, len(result))
	for _, v := range result {
		redisQueue := &api.Queue{}
		e := proto.Unmarshal([]byte(v), redisQueue)
		if e != nil {
			return nil, err
		}
		queues = append(queues, redisQueue)
	}
	return queues, nil
}

// createContextWithShutdown returns a context that will report done when a SIGINT or SIGTERM is received
func createContextWithShutdown() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx
}
