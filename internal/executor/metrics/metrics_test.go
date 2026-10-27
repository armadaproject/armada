package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestRecordRuleEvaluationDuration(t *testing.T) {
	tests := map[string]struct {
		category      string
		subcategory   string
		duration      time.Duration
		expectedDelta uint64
	}{
		"records observation for category and subcategory": {
			category:      "rule-eval-test-cat",
			subcategory:   "rule-eval-test-sub",
			duration:      100 * time.Microsecond,
			expectedDelta: 1,
		},
		"accepts empty subcategory": {
			category:      "rule-eval-test-no-sub",
			subcategory:   "",
			duration:      50 * time.Microsecond,
			expectedDelta: 1,
		},
		"empty category is a no-op": {
			category:      "",
			subcategory:   "rule-eval-test-empty-cat",
			duration:      75 * time.Microsecond,
			expectedDelta: 0,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			h := jobFailureRuleEvaluationDurationSeconds.WithLabelValues(tc.category, tc.subcategory).(prometheus.Histogram)
			before := histogramSampleCount(t, h)
			RecordRuleEvaluationDuration(tc.category, tc.subcategory, tc.duration)
			after := histogramSampleCount(t, h)
			assert.Equal(t, tc.expectedDelta, after-before)
		})
	}
}

func histogramSampleCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	pb := &dto.Metric{}
	require.NoError(t, h.Write(pb))
	require.NotNil(t, pb.Histogram)
	return pb.Histogram.GetSampleCount()
}
