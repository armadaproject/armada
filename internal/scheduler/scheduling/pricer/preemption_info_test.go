package pricer

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunPreemptionInfo_GlobalPreemptionOrder(t *testing.T) {
	// Sort by cost ascending, ageMillis ascending and tie-break on jobId
	result1 := &jobDetails{cost: 0.01, ageMillis: 0, jobId: "c"}
	result2 := &jobDetails{cost: 0.01, ageMillis: 0, jobId: "z"}
	result3 := &jobDetails{cost: 0.05, ageMillis: 0, jobId: "h"}
	result4 := &jobDetails{cost: 0.05, ageMillis: 5, jobId: "f"}
	result5 := &jobDetails{cost: 0.05, ageMillis: 5, jobId: "g"}
	result6 := &jobDetails{cost: 0.1, ageMillis: 0, jobId: "a"}
	result7 := &jobDetails{cost: 0.1, ageMillis: 0, jobId: "b"}
	result8 := &jobDetails{cost: 0.2, ageMillis: 5, jobId: "h"}

	items := []*jobDetails{result3, result2, result5, result1, result6, result4, result8, result7}
	expected := []*jobDetails{result1, result2, result3, result4, result5, result6, result7, result8}

	sort.Sort(priceOrder(items))
	assert.Equal(t, expected, items)
}
