package schedulerobjects

func (m *Executor) AllRuns() ([]string, error) {
	runIds := make([]string, 0)
	// add all runids from nodes
	for _, node := range m.Nodes {
		for runId := range node.StateByJobRunId {
			runIds = append(runIds, runId)
		}
	}
	// add all unassigned runids
	for _, runId := range m.UnassignedJobRuns {
		runIds = append(runIds, runId)
	}
	return runIds, nil
}
