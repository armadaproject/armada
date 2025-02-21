package model

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// CreateJobInstruction is an instruction to insert a new row into the jobs table
type CreateJobInstruction struct {
	JobId                     string
	Queue                     string
	Owner                     string
	Namespace                 string
	JobSet                    string
	Cpu                       int64
	Memory                    int64
	EphemeralStorage          int64
	Gpu                       int64
	Priority                  int64
	Submitted                 time.Time
	State                     int32
	LastTransitionTime        time.Time
	LastTransitionTimeSeconds int64
	JobProto                  []byte
	PriorityClass             *string
	Annotations               map[string]string
	ExternalJobUri            string
}

// UpdateJobInstruction is an instruction to update an existing row in the jobs table
type UpdateJobInstruction struct {
	JobId                     string
	Priority                  *int64
	State                     *int32
	Cancelled                 *time.Time
	CancelReason              *string
	LastTransitionTime        *time.Time
	LastTransitionTimeSeconds *int64
	Duplicate                 *bool
	LatestRunId               *string
}

// CreateJobRunInstruction is an instruction to update an existing row in the jobRuns table
type CreateJobRunInstruction struct {
	RunId       string
	JobId       string
	Cluster     string
	Node        *string
	Leased      *time.Time
	Pending     *time.Time
	JobRunState int32
}

// UpdateJobRunInstruction is an instruction to update an existing row in the job runs table
type UpdateJobRunInstruction struct {
	RunId       string
	Node        *string
	Pending     *time.Time
	Started     *time.Time
	Finished    *time.Time
	JobRunState *int32
	Error       []byte
	Debug       []byte
	ExitCode    *int32
}

// CreateJobErrorInstruction is an instruction to crearte a new row in the job_error table
type CreateJobErrorInstruction struct {
	JobId string
	Error []byte
}

// InstructionSet represents a set of instructions to apply to the database.  Each type of instruction is stored in its
// own ordered list representing the order it was received.  We also store the original message ids corresponding to
// these instructions so that when they are saved to the database, we can ACK the corresponding messages.
type InstructionSet struct {
	JobsToCreate      []*CreateJobInstruction
	JobsToUpdate      []*UpdateJobInstruction
	JobRunsToCreate   []*CreateJobRunInstruction
	JobRunsToUpdate   []*UpdateJobRunInstruction
	JobErrorsToCreate []*CreateJobErrorInstruction
	MessageIds        []pulsar.MessageID
}

func (i *InstructionSet) GetMessageIDs() []pulsar.MessageID {
	return i.MessageIds
}
