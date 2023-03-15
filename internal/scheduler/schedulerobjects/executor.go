package schedulerobjects

import (
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

func (m *Executor) AllRuns() ([]uuid.UUID, error) {
	runIds := make([]uuid.UUID, 0)
	// add all runids from nodes
	for _, node := range m.Nodes {
		for runIdStr := range node.StateByJobRunId {
			runId, err := uuid.Parse(runIdStr)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			runIds = append(runIds, runId)
		}
	}
	// add all unassigned runids
	for _, runIdStr := range m.UnassignedJobRuns {
		runId, err := uuid.Parse(runIdStr)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		runIds = append(runIds, runId)
	}
	return runIds, nil
}
