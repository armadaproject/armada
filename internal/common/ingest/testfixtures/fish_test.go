package testfixtures

import (
	"math/rand"
	"testing"
	"time"

	"github.com/caarlos0/log"

	"github.com/armadaproject/armada/internal/common/ingest/metrics"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestPerf(t *testing.T) {
	testMetrics := metrics.NewMetrics("test")
	queues := make([]string, 1000)
	for i := range queues {
		queues[i] = randSeq(10)
	}

	msgTypes := make([]string, 20)
	for i := range msgTypes {
		msgTypes[i] = randSeq(20)
	}

	start := time.Now()
	numIterations := 1000000
	for i := 0; i < numIterations; i++ {
		queue := queues[rand.Intn(len(queues))]
		msgType := msgTypes[rand.Intn(len(msgTypes))]
		testMetrics.RecordEventSequenceProcessed(queue, msgType)
	}
	log.Infof("Completed %d iterations in %v", numIterations, time.Since(start))
}
