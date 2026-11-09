package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/metricevents"
)

func TestClassifyTaintGroup(t *testing.T) {
	taint := func(category string, userFacing bool) internaltypes.NodeTaint {
		return internaltypes.NewNodeTaint(v1.Taint{Key: "example"}, category, userFacing)
	}
	for _, tc := range []struct {
		name      string
		taints    []internaltypes.NodeTaint
		rawTaints []v1.Taint
		want      string
	}{
		{name: "nil taints", want: "none"},
		{name: "empty taints", taints: []internaltypes.NodeTaint{}, want: "none"},
		{name: "raw taints without metadata", rawTaints: []v1.Taint{{Key: "legacy"}}, want: "none"},
		{name: "user taint", taints: []internaltypes.NodeTaint{taint("user", false)}, want: "user"},
		{name: "multiple user taints", taints: []internaltypes.NodeTaint{taint("user", false), taint("user", true)}, want: "user"},
		{name: "unknown taint", taints: []internaltypes.NodeTaint{taint("unknown", false)}, want: "unknown"},
		{name: "unspecified category", taints: []internaltypes.NodeTaint{taint("", false)}, want: "unknown"},
		{name: "admin category", taints: []internaltypes.NodeTaint{taint("platform", false)}, want: "admin"},
		{name: "another admin category", taints: []internaltypes.NodeTaint{taint("maintenance", false)}, want: "admin"},
		{name: "unknown after user", taints: []internaltypes.NodeTaint{taint("user", false), taint("unknown", false)}, want: "unknown"},
		{name: "unknown before user", taints: []internaltypes.NodeTaint{taint("unknown", false), taint("user", false)}, want: "unknown"},
		{name: "unspecified and user", taints: []internaltypes.NodeTaint{taint("", false), taint("user", false)}, want: "unknown"},
		{name: "admin after unknown", taints: []internaltypes.NodeTaint{taint("unknown", false), taint("platform", false)}, want: "admin"},
		{name: "admin before unknown", taints: []internaltypes.NodeTaint{taint("platform", false), taint("unknown", false)}, want: "admin"},
		{name: "admin after unspecified", taints: []internaltypes.NodeTaint{taint("", false), taint("platform", false)}, want: "admin"},
		{name: "admin before unspecified", taints: []internaltypes.NodeTaint{taint("platform", false), taint("", false)}, want: "admin"},
		{name: "admin and user", taints: []internaltypes.NodeTaint{taint("user", false), taint("platform", false)}, want: "admin"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := testfixtures.Test32CpuNode(testfixtures.TestPriorities).WithTaints(tc.rawTaints).WithNodeTaints(tc.taints)
			assert.Equal(t, tc.want, classifyTaintGroup(node.GetNodeTaints()))
		})
	}
}

func TestReportSchedulerResult_ReportsTaintGroupType(t *testing.T) {
	config := testfixtures.TestSchedulingConfig()
	nodeDb, err := nodedb.NewNodeDb(config.PriorityClasses, config.IndexedResources, config.IndexedTaints, config.IndexedNodeLabels, config.WellKnownNodeTypes, testfixtures.TestResourceListFactory)
	require.NoError(t, err)
	node := testfixtures.Test32CpuNode(testfixtures.TestPriorities).WithNodeTaints([]internaltypes.NodeTaint{
		internaltypes.NewNodeTaint(v1.Taint{Key: "example"}, "platform", false),
	})
	require.NoError(t, nodeDb.Upsert(node))

	m := newCycleMetrics(pulsarutils.NoOpPublisher[*metricevents.Event]{}, "")
	m.ReportSchedulerResult(armadacontext.Background(), scheduling.SchedulerResult{
		PoolResults: []*scheduling.PoolSchedulingResult{{
			Name: node.GetPool(),
			SchedulingResult: &scheduling.SchedulingResult{AdditionalSchedulingInfo: &scheduling.SchedulingInformation{
				NodeDb: nodeDb,
			}},
		}},
	})

	registry := prometheus.NewRegistry()
	cycle := m.latestCycleMetrics.Load()
	registry.MustRegister(cycle.nodeAllocatableResource, cycle.nodeAllocatedResource)
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 2)
	for _, family := range families {
		require.NotEmpty(t, family.GetMetric(), family.GetName())
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			assert.Equal(t, "admin", labels["taintGroupType"], family.GetName())
		}
	}
}
