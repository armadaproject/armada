package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueueLabelValidation(t *testing.T) {
	tests := map[string]struct {
		labelName string
		isValid   bool
	}{
		"Empty label name": {
			labelName: "",
			isValid:   false,
		},
		"Priority label": {
			labelName: "priority",
			isValid:   true,
		},
		"Label name with underscores": {
			labelName: "priority__cpu_pool",
			isValid:   true,
		},
		"Label name with spaces": {
			labelName: "priority cpu pool",
			isValid:   false,
		},
		"Alphanumeric label name": {
			labelName: "cluster_12_user",
			isValid:   true,
		},
		"Invalid Kubernetes-style label name 1": {
			labelName: "armadaproject.io/category",
			isValid:   false,
		},
		"Invalid Kubernetes-style label name 2": {
			labelName: "armadaproject.io/ttl",
			isValid:   false,
		},
		"Invalid Kubernetes-style label name 3": {
			labelName: "kubernetes.io/metadata.name",
			isValid:   false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			isValid := isValidMetricLabelName(tc.labelName)
			assert.Equal(t, tc.isValid, isValid)
		})
	}
}
