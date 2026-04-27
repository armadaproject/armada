package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestRecordJobFailure(t *testing.T) {
	tests := map[string]struct {
		category      string
		subcategory   string
		expectedDelta float64
	}{
		"increments counter for category and subcategory": {
			category:      "metrics-test-cat",
			subcategory:   "metrics-test-sub",
			expectedDelta: 1,
		},
		"accepts empty subcategory": {
			category:      "metrics-test-empty-sub",
			subcategory:   "",
			expectedDelta: 1,
		},
		"empty category is a no-op": {
			category:      "",
			subcategory:   "metrics-test-empty-cat",
			expectedDelta: 0,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			before := testutil.ToFloat64(jobFailureCategoryTotal.WithLabelValues(tc.category, tc.subcategory))
			RecordJobFailure(tc.category, tc.subcategory)
			after := testutil.ToFloat64(jobFailureCategoryTotal.WithLabelValues(tc.category, tc.subcategory))
			assert.Equal(t, tc.expectedDelta, after-before)
		})
	}
}
