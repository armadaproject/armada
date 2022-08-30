package batch

import (
	"sync"
	"testing"
	"time"

	"github.com/G-Research/armada/internal/pulsarutils"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/G-Research/armada/internal/lookoutingester/model"
)

const (
	defaultMaxItems   = 2
	defaultMaxTimeOut = 1 * time.Second
	defaultBufferSize = 3
)

var (
	update1 = &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{{JobId: "job1"}},
		MessageIds:   []*pulsarutils.ConsumerMessageId{pulsarutils.NewConsumerMessageId(1)},
	}
	update2 = &model.InstructionSet{
		JobsToCreate: []*model.CreateJobInstruction{{JobId: "job2"}},
		MessageIds:   []*pulsarutils.ConsumerMessageId{pulsarutils.NewConsumerMessageId(2)},
	}
)

func TestBatchByMaxItems(t *testing.T) {
	inputChan := make(chan *model.InstructionSet)
	testClock := clock.NewFakeClock(time.Now())
	outputChan := Batch(inputChan, defaultMaxItems, defaultMaxTimeOut, defaultBufferSize, testClock)

	// Post 2 instruction sets on the input channel without advancing the clock
	// And we should get a single update on the output channel
	inputChan <- update1
	inputChan <- update2

	wg := &sync.WaitGroup{}
	wg.Add(1)

	var received *model.InstructionSet = nil

	go func() {
		for e := range outputChan {
			received = e
			close(inputChan)
			wg.Done()
		}
	}()

	wg.Wait()
	expected := &model.InstructionSet{
		JobsToCreate:             []*model.CreateJobInstruction{{JobId: "job1"}, {JobId: "job2"}},
		MessageIds:               []*pulsarutils.ConsumerMessageId{pulsarutils.NewConsumerMessageId(1), pulsarutils.NewConsumerMessageId(2)},
		JobsToUpdate:             []*model.UpdateJobInstruction{},
		JobRunsToCreate:          []*model.CreateJobRunInstruction{},
		JobRunsToUpdate:          []*model.UpdateJobRunInstruction{},
		UserAnnotationsToCreate:  []*model.CreateUserAnnotationInstruction{},
		JobRunContainersToCreate: []*model.CreateJobRunContainerInstruction{},
	}
	assert.Equal(t, expected, received)
}

func TestBatchByTime(t *testing.T) {
	inputChan := make(chan *model.InstructionSet)
	testClock := clock.NewFakeClock(time.Now())
	outputChan := Batch(inputChan, defaultMaxItems, defaultMaxTimeOut, defaultBufferSize, testClock)

	// Post 1 instruction sets on the input channel and advance clock
	// And we should get a single update on the output channel
	inputChan <- update1

	wg := &sync.WaitGroup{}
	wg.Add(1)

	var received *model.InstructionSet = nil

	go func() {
		for e := range outputChan {
			received = e
			close(inputChan)
			wg.Done()
		}
	}()
	testClock.Step(2 * time.Second)
	wg.Wait()
	expected := &model.InstructionSet{
		JobsToCreate:             []*model.CreateJobInstruction{{JobId: "job1"}},
		MessageIds:               []*pulsarutils.ConsumerMessageId{pulsarutils.NewConsumerMessageId(1)},
		JobsToUpdate:             []*model.UpdateJobInstruction{},
		JobRunsToCreate:          []*model.CreateJobRunInstruction{},
		JobRunsToUpdate:          []*model.UpdateJobRunInstruction{},
		UserAnnotationsToCreate:  []*model.CreateUserAnnotationInstruction{},
		JobRunContainersToCreate: []*model.CreateJobRunContainerInstruction{},
	}
	assert.Equal(t, expected, received)
}
