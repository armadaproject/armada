package optimiser

import "testing"

func TestFairnessOptimisingScheduler_Schedule(t *testing.T) {

	// Should schedule on lowest cost result - 0 cost
	// Should schedule on lowest cost result
	// Should schedule on updated sctx + nodeDb state
	// Should only schedule if exceeds fairness threshold
	// Should handle gangs
	// - Validates node uniformity label
	// - Groups nodes by uniformity label
	// - All nodes must exceed threshold
	// - Passes updated node/sctx throughout scheduling ?
	// Does not update state if no scheduling performed

}
