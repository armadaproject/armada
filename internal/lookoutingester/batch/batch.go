package batch

import (
	"github.com/G-Research/armada/internal/pulsarutils"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/G-Research/armada/internal/lookoutingester/model"
)

// Batch batches up Instruction sets from a channel.  Batches are created whenever maxItems InstructionSets have been
// received or maxTimeout has elapsed since the last batch was created (whichever occurs first).
func Batch(values <-chan *model.InstructionSet, maxItems int, maxTimeout time.Duration, bufferSize int, clock clock.Clock) chan *model.InstructionSet {

	out := make(chan *model.InstructionSet, bufferSize)

	go func() {
		defer close(out)

		for keepGoing := true; keepGoing; {
			var batch []*model.InstructionSet
			expire := clock.After(maxTimeout)
			for {
				select {
				case value, ok := <-values:
					if !ok {
						keepGoing = false
						goto done
					}

					batch = append(batch, value)
					if len(batch) == maxItems {
						goto done
					}

				case <-expire:
					goto done
				}
			}

		done:
			if len(batch) > 0 {
				out <- mergeInstructionSets(batch)
			}
		}
	}()
	return out
}

// TODO- this function is relatively efficient but is too verbose.
// Generics should help, when they become
func mergeInstructionSets(batch []*model.InstructionSet) *model.InstructionSet {

	var lenMessageIds = 0
	var lenJobsToCreate = 0
	var lenJobsToUpdate = 0
	var lenJobRunsToCreate = 0
	var lenJobRunsToUpdate = 0
	var lenUserAnnotationsToCreate = 0
	var lenJobRunConaintersToCreate = 0

	for _, instructionSet := range batch {
		lenMessageIds += len(instructionSet.MessageIds)
		lenJobsToCreate += len(instructionSet.JobsToCreate)
		lenJobsToUpdate += len(instructionSet.JobsToUpdate)
		lenJobRunsToCreate += len(instructionSet.JobRunsToCreate)
		lenJobRunsToUpdate += len(instructionSet.JobRunsToUpdate)
		lenUserAnnotationsToCreate += len(instructionSet.UserAnnotationsToCreate)
		lenJobRunConaintersToCreate += len(instructionSet.JobRunContainersToCreate)
	}
	messageIds := make([]*pulsarutils.ConsumerMessageId, lenMessageIds)
	jobsToCreate := make([]*model.CreateJobInstruction, lenJobsToCreate)
	jobsToUpdate := make([]*model.UpdateJobInstruction, lenJobsToUpdate)
	jobRunsToCreate := make([]*model.CreateJobRunInstruction, lenJobRunsToCreate)
	jobRunsToUpdate := make([]*model.UpdateJobRunInstruction, lenJobRunsToUpdate)
	userAnnotationsToCreate := make([]*model.CreateUserAnnotationInstruction, lenUserAnnotationsToCreate)
	jobRunContainersToCreate := make([]*model.CreateJobRunContainerInstruction, lenJobRunConaintersToCreate)

	var messageIdIdx = 0
	var jobsToCreateIdx = 0
	var jobsToUpdateIdx = 0
	var jobRunsToCreateIdx = 0
	var jobRunsToUpdateIdx = 0
	var userAnnotationsToCreateIdx = 0
	var jobRunContainersToCreateIdx = 0

	for _, instructionSet := range batch {

		for _, id := range instructionSet.MessageIds {
			messageIds[messageIdIdx] = id
			messageIdIdx++
		}

		for _, instruction := range instructionSet.JobsToCreate {
			jobsToCreate[jobsToCreateIdx] = instruction
			jobsToCreateIdx++
		}

		for _, instruction := range instructionSet.JobsToUpdate {
			jobsToUpdate[jobsToUpdateIdx] = instruction
			jobsToUpdateIdx++
		}

		for _, instruction := range instructionSet.JobRunsToCreate {
			jobRunsToCreate[jobRunsToCreateIdx] = instruction
			jobRunsToCreateIdx++
		}

		for _, instruction := range instructionSet.JobRunsToUpdate {
			jobRunsToUpdate[jobRunsToUpdateIdx] = instruction
			jobRunsToUpdateIdx++
		}

		for _, instruction := range instructionSet.UserAnnotationsToCreate {
			userAnnotationsToCreate[userAnnotationsToCreateIdx] = instruction
			userAnnotationsToCreateIdx++
		}

		for _, instruction := range instructionSet.JobRunContainersToCreate {
			jobRunContainersToCreate[jobRunContainersToCreateIdx] = instruction
			jobRunContainersToCreateIdx++
		}
	}

	return &model.InstructionSet{
		JobsToCreate:             jobsToCreate,
		JobsToUpdate:             jobsToUpdate,
		JobRunsToCreate:          jobRunsToCreate,
		JobRunsToUpdate:          jobRunsToUpdate,
		UserAnnotationsToCreate:  userAnnotationsToCreate,
		JobRunContainersToCreate: jobRunContainersToCreate,
		MessageIds:               messageIds,
	}
}
