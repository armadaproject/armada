package model

import (
	"github.com/G-Research/armada/internal/pulsarutils"
	"time"
)

// CreateJobInstruction is an instruction to insert a new row into the jobs table
type CreateJobInstruction struct {
	JobId     string
	Queue     string
	Owner     string
	JobSet    string
	Priority  uint32
	Submitted time.Time
	JobJson   []byte
	JobProto  []byte
	State     int64
	Updated   time.Time
}

// UpdateJobInstruction is an instruction to update an existing row in the jobs table
type UpdateJobInstruction struct {
	JobId     string
	Priority  *int32
	State     *int32
	Updated   time.Time
	Cancelled *time.Time
	Duplicate *bool
}

// CreateJobRunContainerInstruction is an instruction to create a new entry in the jobRunContainerInstruction table
type CreateJobRunContainerInstruction struct {
	RunId         string
	ContainerName string
	ExitCode      int32
}

// CreateUserAnnotationInstruction is an instruction to create a new entry in the UserAnnotationInstruction table
type CreateUserAnnotationInstruction struct {
	JobId string
	Key   string
	Value string
}

// CreateJobRunInstruction is an instruction to update an existing row in the jobRuns table
type CreateJobRunInstruction struct {
	RunId   string
	JobId   string
	Cluster string
	Created time.Time
}

// UpdateJobRunInstruction is an instruction to update an existing row in the job runs table
type UpdateJobRunInstruction struct {
	RunId            string
	Node             *string
	Started          *time.Time
	Finished         *time.Time
	Succeeded        *bool
	Error            *string
	PodNumber        *int32
	UnableToSchedule *bool
}

// InstructionSet represents a set of instructions to apply to the database.  Each type of instruction is stored in its
// own ordered list representign the order it was received.  We also store the original message ids corresponding to
// these instructions so that when they are saved to the database, we can ACK the corresponding messages.
type InstructionSet struct {
	JobsToCreate             []*CreateJobInstruction
	JobsToUpdate             []*UpdateJobInstruction
	JobRunsToCreate          []*CreateJobRunInstruction
	JobRunsToUpdate          []*UpdateJobRunInstruction
	UserAnnotationsToCreate  []*CreateUserAnnotationInstruction
	JobRunContainersToCreate []*CreateJobRunContainerInstruction
	MessageIds               []*pulsarutils.ConsumerMessageId
}
