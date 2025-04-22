package store

import (
	"fmt"
	"regexp"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/redis/go-redis/v9"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/ingest"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/eventingester/configuration"
	"github.com/armadaproject/armada/internal/eventingester/metrics"
	"github.com/armadaproject/armada/internal/eventingester/model"
)

const (
	eventStreamPrefix = "Events:"
	dataKey           = "message"
)

type RedisEventStore struct {
	dbs                []redis.UniversalClient
	dbNames            []string
	eventRetention     configuration.EventRetentionPolicy
	intialRetryBackoff time.Duration
	maxRetryBackoff    time.Duration
	maxRows            int
	maxSize            int
	fatalErrors        []*regexp.Regexp
}

func NewRedisEventStore(dbs []redis.UniversalClient, dbNames []string, eventRetention configuration.EventRetentionPolicy, fatalErrors []*regexp.Regexp, intialRetryBackoff time.Duration, maxRetryBackoff time.Duration) ingest.Sink[*model.BatchUpdate] {
	return &RedisEventStore{
		dbs:                dbs,
		dbNames:            dbNames,
		eventRetention:     eventRetention,
		fatalErrors:        fatalErrors,
		intialRetryBackoff: intialRetryBackoff,
		maxRetryBackoff:    maxRetryBackoff,
	}
}

func (repo *RedisEventStore) Store(ctx *armadacontext.Context, update *model.BatchUpdate) error {
	if len(update.Events) == 0 {
		return nil
	}
	var result *multierror.Error

	// Insert such that we never send more than maxRows rows or maxSize of data to redis at a time
	currentSize := 0
	currentRows := 0
	batch := make([]*model.Event, 0, repo.maxRows)

	for i, event := range update.Events {
		newSize := currentSize + len(event.Event)
		newRows := currentRows + 1
		if newSize > repo.maxSize || newRows > repo.maxRows {
			err := repo.doStore(ctx, batch)
			result = multierror.Append(result, err)
			batch = make([]*model.Event, 0, repo.maxRows)
			currentSize = 0
			currentRows = 0
		}
		batch = append(batch, event)
		currentSize += len(event.Event)
		currentRows++

		// If this is the last element we need to flush
		if i == len(update.Events)-1 {
			err := repo.doStore(ctx, batch)
			result = multierror.Append(result, err)
		}
	}
	return result.ErrorOrNil()
}

type eventData struct {
	key             string
	data            []byte
	redisSequenceId string
}

func (repo *RedisEventStore) doStore(ctx *armadacontext.Context, update []*model.Event) error {
	if len(update) == 0 {
		return nil
	}

	return ingest.WithRetry(func() (bool, error) {
		var data []eventData
		uniqueJobSets := make(map[string]bool)

		for _, e := range update {
			key := getJobSetEventsKey(e.Queue, e.Jobset)
			data = append(data, eventData{key: key, data: e.Event})
			uniqueJobSets[key] = true
		}

		for i, db := range repo.dbs {
			r, e := repo.writeToRedis(ctx, db, data, uniqueJobSets, repo.dbNames[i])
			if e != nil {
				return r, fmt.Errorf("error with redis %s: %v", repo.dbNames[i], e)
			}
		}
		return false, nil
	}, repo.intialRetryBackoff, repo.maxRetryBackoff)
}

func (repo *RedisEventStore) writeToRedis(ctx *armadacontext.Context, db redis.UniversalClient, data []eventData, uniqueJobSets map[string]bool, redisName string) (bool, error) {
	start := time.Now()
	pipe := db.Pipeline()
	for _, e := range data {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: e.key,
			ID:     e.redisSequenceId,
			Values: map[string]interface{}{
				dataKey: e.data,
			},
		})
	}

	for key := range uniqueJobSets {
		pipe.Expire(ctx, key, repo.eventRetention.RetentionDuration)
	}

	cmders, err := pipe.Exec(ctx)
	if err != nil {
		metrics.RecordWriteDuration(redisName, "failed_write", time.Since(start))
		return repo.isRetryableRedisError(err), err
	}

	err = populateRedisSequenceIds(cmders, data)
	if err != nil {
		metrics.RecordWriteDuration(redisName, "failed_get_sequence_id", time.Since(start))
		return true, err
	}

	metrics.RecordWriteDuration(redisName, "success", time.Since(start))
	return false, nil
}

func populateRedisSequenceIds(cmders []redis.Cmder, data []eventData) error {
	for i := range data {
		c := cmders[i]
		sc, sok := c.(*redis.StringCmd)
		if !sok {
			return fmt.Errorf("expected StringCmd got %T %v", c, c)
		}

		r, err := sc.Result()
		if err != nil {
			return fmt.Errorf("could not read result from StringCmd: %v", err)
		}

		data[i].redisSequenceId = r
	}
	return nil
}

// IsRetryableRedisError returns true if the error doesn't match the list of nonRetryableErrors
func (repo *RedisEventStore) isRetryableRedisError(err error) bool {
	if err == nil {
		return true
	}
	s := err.Error()
	for _, r := range repo.fatalErrors {
		if r.MatchString(s) {
			log.Infof("Error %s matched regex %s and so will be considered fatal", s, r)
			return false
		}
	}
	return true
}

func getJobSetEventsKey(queue, jobSetId string) string {
	return eventStreamPrefix + queue + ":" + jobSetId
}
