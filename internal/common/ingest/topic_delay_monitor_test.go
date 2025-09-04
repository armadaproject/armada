package ingest

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/rest"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/ingest/pulsarclient"
)

const (
	partition1 = "events-partition-1"
	interval   = time.Second * 5
)

// This is what pulsar returns when there are no message to peek, it doesn't just return an empty list
var pulsarNotFoundError = rest.Error{messageNotFound, 404}

func TestInitialise(t *testing.T) {
	delayMonitor, _, _, _, _ := setupTopicDelayMonitorTest()

	ctx := armadacontext.Background()
	err := delayMonitor.Initialise(ctx)
	assert.NoError(t, err)

	assert.Len(t, delayMonitor.partitions, 1)
	assert.Equal(t, partition1, delayMonitor.partitions[0].GetEncodedTopic())
	assert.Equal(t, 1, delayMonitor.partitions[0].GetPartitionIndex())
}

func TestInitialise_Error(t *testing.T) {
	delayMonitor, _, partitionFetcher, _, _ := setupTopicDelayMonitorTest()
	fetchErr := fmt.Errorf("failed to fetch")
	partitionFetcher.err = fetchErr

	ctx := armadacontext.Background()
	err := delayMonitor.Initialise(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), fetchErr.Error())
}

func TestInitialise_InvalidPartition(t *testing.T) {
	delayMonitor, _, partitionFetcher, _, _ := setupTopicDelayMonitorTest()
	partitionFetcher.partitions = []string{"://"}

	ctx := armadacontext.Background()
	err := delayMonitor.Initialise(ctx)
	assert.Error(t, err)
}

func TestCalculatePartitionDelay(t *testing.T) {
	startTime := time.Now()
	initDuration := time.Second
	type loopInfo struct {
		latestMessagePublishTime *time.Time
		retrieveLatestMessageErr error
		expectedDelay            time.Duration
		expectRecorderCalled     bool
	}
	tests := map[string]struct {
		loops []loopInfo
	}{
		"happy path": {
			loops: []loopInfo{
				{
					latestMessagePublishTime: toPtr(startTime.Add(-time.Second * 10)),
					expectedDelay:            time.Second*10 + initDuration + interval*time.Duration(1),
					expectRecorderCalled:     true,
				},
				{
					latestMessagePublishTime: toPtr(startTime.Add(-time.Second * 5)),
					expectedDelay:            time.Second*5 + initDuration + interval*time.Duration(2),
					expectRecorderCalled:     true,
				},
			},
		},
		"fallback with existing value": {
			loops: []loopInfo{
				{
					latestMessagePublishTime: toPtr(startTime.Add(-time.Second * 10)),
					expectedDelay:            time.Second*10 + initDuration + interval*time.Duration(1),
					expectRecorderCalled:     true,
				},
				{
					retrieveLatestMessageErr: fmt.Errorf("failed to get latest"),
					expectedDelay:            time.Second*10 + initDuration + interval*time.Duration(2),
					expectRecorderCalled:     true,
				},
			},
		},
		"no delay": {
			loops: []loopInfo{
				{
					retrieveLatestMessageErr: pulsarNotFoundError,
					expectedDelay:            time.Second * 0,
					expectRecorderCalled:     true,
				},
			},
		},
		"fall back from no delay": {
			loops: []loopInfo{
				{
					retrieveLatestMessageErr: pulsarNotFoundError,
					expectedDelay:            time.Second * 0,
					expectRecorderCalled:     true,
				},
				{
					retrieveLatestMessageErr: fmt.Errorf("failed to get latest"),
					expectedDelay:            interval * time.Duration(1),
					expectRecorderCalled:     true,
				},
				{
					retrieveLatestMessageErr: fmt.Errorf("failed to get latest"),
					expectedDelay:            interval * time.Duration(2),
					expectRecorderCalled:     true,
				},
			},
		},
		"no value if always errored": {
			loops: []loopInfo{
				{
					retrieveLatestMessageErr: fmt.Errorf("failed to get latest"),
					expectRecorderCalled:     false,
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			delayMonitor, fakeClock, _, subscriptionStub, recorder := setupTopicDelayMonitorTest()
			ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
			defer cancel()
			go func() {
				err := delayMonitor.Run(ctx)
				require.NoError(t, err)
			}()
			fakeClock.SetTime(startTime)
			// Let routine start + monitor initialise
			fakeClock.Step(time.Second)
			time.Sleep(time.Millisecond * 50)

			expectedNumberOfRecorderCalls := 0
			for i, loop := range tc.loops {
				if loop.latestMessagePublishTime != nil {
					formattedTime := loop.latestMessagePublishTime.Format(time.RFC3339Nano)
					subscriptionStub.message = &utils.Message{Properties: map[string]string{publishTimeProperty: formattedTime}}
				} else {
					subscriptionStub.message = &utils.Message{}
				}

				if loop.retrieveLatestMessageErr != nil {
					subscriptionStub.err = loop.retrieveLatestMessageErr
				} else {
					subscriptionStub.err = nil
				}

				// Step 1 interval
				fakeClock.Step(interval)
				time.Sleep(time.Millisecond * 50)

				if loop.expectRecorderCalled {
					assert.Equal(t, float64(loop.expectedDelay.Milliseconds()), recorder.delay["subscription"][1])
					expectedNumberOfRecorderCalls++
				}
				assert.Equal(t, expectedNumberOfRecorderCalls, recorder.numberOfCalls)
				assert.Equal(t, i+1, subscriptionStub.numberOfCalls)
			}
		})
	}
}

func toPtr(t time.Time) *time.Time {
	return &t
}

func setupTopicDelayMonitorTest() (*TopicProcessingDelayMonitor, *clock.FakeClock, *PartitionFetcherStub, *SubscriptionsStub, *PulsarDelayRecorderStub) {
	partitionFetcher := &PartitionFetcherStub{partitions: []string{partition1}}
	subscriptionStub := &SubscriptionsStub{}
	pulsarAdminClient := &PulsarAdminClientStub{subscriptions: subscriptionStub}
	recorder := &PulsarDelayRecorderStub{}
	fakeClock := clock.NewFakeClock(time.Now())
	delayMonitor := NewTopicProcessingDelayMonitor(partitionFetcher, pulsarAdminClient, "topic", "subscription", interval, recorder)
	delayMonitor.clock = fakeClock
	// Remove jitter for ease of testing
	delayMonitor.jitterFunc = func() time.Duration {
		return time.Second * 0
	}
	return delayMonitor, fakeClock, partitionFetcher, subscriptionStub, recorder
}

type PartitionFetcherStub struct {
	partitions []string
	err        error
}

func (p *PartitionFetcherStub) TopicPartitions(topic string) ([]string, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.partitions, nil
}

type PulsarDelayRecorderStub struct {
	delay         map[string]map[int]float64
	numberOfCalls int
}

func (p *PulsarDelayRecorderStub) RecordPulsarProcessingDelay(subscriptionName string, partition int, delayInMs float64) {
	p.numberOfCalls++
	if p.delay == nil {
		p.delay = make(map[string]map[int]float64)
	}
	if _, exists := p.delay[subscriptionName]; !exists {
		p.delay[subscriptionName] = make(map[int]float64)
	}
	p.delay[subscriptionName][partition] = delayInMs
}

type SubscriptionsStub struct {
	numberOfCalls int
	message       *utils.Message
	err           error
}

func (s *SubscriptionsStub) PeekMessages(topicName utils.TopicName, subscriptionName string, count int) ([]*utils.Message, error) {
	s.numberOfCalls++
	if s.err != nil {
		return nil, s.err
	}
	return []*utils.Message{s.message}, nil
}

type PulsarAdminClientStub struct {
	subscriptions pulsarclient.Subscriptions
}

func (c *PulsarAdminClientStub) Subscriptions() pulsarclient.Subscriptions {
	return c.subscriptions
}
