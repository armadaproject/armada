package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	executorconfig "github.com/armadaproject/armada/internal/executor/configuration"
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
	unschedulable := internaltypes.NewNodeTaint(internaltypes.UnschedulableTaint(), "", false)
	kubernetesCordon := internaltypes.NewNodeTaint(v1.Taint{Key: v1.TaintNodeUnschedulable}, "", false)
	for name, tc := range map[string]struct {
		taints    []internaltypes.NodeTaint
		rawTaints []v1.Taint
		want      string
	}{
		"nil taints":                   {want: "none"},
		"empty taints":                 {taints: []internaltypes.NodeTaint{}, want: "none"},
		"synthetic marker only":        {taints: []internaltypes.NodeTaint{unschedulable}, want: "none"},
		"marker after user":            {taints: []internaltypes.NodeTaint{taint("user", true), unschedulable}, want: "user"},
		"marker before user":           {taints: []internaltypes.NodeTaint{unschedulable, taint("user", true)}, want: "user"},
		"marker and unknown":           {taints: []internaltypes.NodeTaint{unschedulable, taint("unknown", false)}, want: "unknown"},
		"marker and admin":             {taints: []internaltypes.NodeTaint{unschedulable, taint("platform", false)}, want: "admin"},
		"kubernetes cordon taint":      {taints: []internaltypes.NodeTaint{kubernetesCordon}, want: "unknown"},
		"raw taints without metadata":  {rawTaints: []v1.Taint{{Key: "legacy"}}, want: "none"},
		"user taint":                   {taints: []internaltypes.NodeTaint{taint("user", false)}, want: "user"},
		"multiple user taints":         {taints: []internaltypes.NodeTaint{taint("user", false), taint("user", true)}, want: "user"},
		"unknown taint":                {taints: []internaltypes.NodeTaint{taint("unknown", false)}, want: "unknown"},
		"unspecified category":         {taints: []internaltypes.NodeTaint{taint("", false)}, want: "unknown"},
		"legacy user-facing taint":     {taints: []internaltypes.NodeTaint{taint("", true)}, want: "user"},
		"legacy and user taints":       {taints: []internaltypes.NodeTaint{taint("", true), taint("user", false)}, want: "user"},
		"legacy and unknown taints":    {taints: []internaltypes.NodeTaint{taint("", true), taint("unknown", false)}, want: "unknown"},
		"unknown and legacy taints":    {taints: []internaltypes.NodeTaint{taint("unknown", false), taint("", true)}, want: "unknown"},
		"legacy and admin taints":      {taints: []internaltypes.NodeTaint{taint("", true), taint("platform", false)}, want: "admin"},
		"admin and legacy taints":      {taints: []internaltypes.NodeTaint{taint("platform", false), taint("", true)}, want: "admin"},
		"user-facing unknown category": {taints: []internaltypes.NodeTaint{taint("unknown", true)}, want: "unknown"},
		"admin category":               {taints: []internaltypes.NodeTaint{taint("platform", false)}, want: "admin"},
		"another admin category":       {taints: []internaltypes.NodeTaint{taint("maintenance", false)}, want: "admin"},
		"unknown after user":           {taints: []internaltypes.NodeTaint{taint("user", false), taint("unknown", false)}, want: "unknown"},
		"unknown before user":          {taints: []internaltypes.NodeTaint{taint("unknown", false), taint("user", false)}, want: "unknown"},
		"unspecified and user":         {taints: []internaltypes.NodeTaint{taint("", false), taint("user", false)}, want: "unknown"},
		"admin after unknown":          {taints: []internaltypes.NodeTaint{taint("unknown", false), taint("platform", false)}, want: "admin"},
		"admin before unknown":         {taints: []internaltypes.NodeTaint{taint("platform", false), taint("unknown", false)}, want: "admin"},
		"admin after unspecified":      {taints: []internaltypes.NodeTaint{taint("", false), taint("platform", false)}, want: "admin"},
		"admin before unspecified":     {taints: []internaltypes.NodeTaint{taint("platform", false), taint("", false)}, want: "admin"},
		"admin and user":               {taints: []internaltypes.NodeTaint{taint("user", false), taint("platform", false)}, want: "admin"},
	} {
		t.Run(name, func(t *testing.T) {
			node := testfixtures.Test32CpuNode(testfixtures.TestPriorities).WithTaints(tc.rawTaints).WithNodeTaints(tc.taints)
			assert.Equal(t, tc.want, classifyTaintGroup(node.GetNodeTaints()))
		})
	}
}

func TestClassifyTaintGroup_AfterNodeBecomesUnschedulable(t *testing.T) {
	userTaint := internaltypes.NewNodeTaint(v1.Taint{Key: "example"}, "user", true)
	node := testfixtures.Test32CpuNode(testfixtures.TestPriorities).WithNodeTaints([]internaltypes.NodeTaint{userTaint})
	assert.Equal(t, "user", classifyTaintGroup(node.GetNodeTaints()))
	unschedulableNode := node.WithSchedulable(false)
	assert.True(t, unschedulableNode.IsUnschedulable())
	assert.Equal(t, "user", classifyTaintGroup(unschedulableNode.GetNodeTaints()))
}

func TestClassifyTaintGroup_LegacyWellKnownTaints(t *testing.T) {
	config := executorconfig.KubernetesConfiguration{
		ToleratedTaints: []string{"example.com/dedicated"},
		TaintCategorisation: executorconfig.TaintCategorisationConfiguration{
			WellKnownTaints: executorconfig.WellKnownTaintsConfiguration{Keys: []string{"example.com/dedicated"}},
		},
	}
	matcher, err := config.NewWellKnownTaints()
	require.NoError(t, err)
	for name, key := range map[string]string{
		"overlapping legacy and well-known key": "example.com/dedicated",
		"reservation key":                       constants.ReservationTaintKey,
	} {
		t.Run(name, func(t *testing.T) {
			match, ok := matcher.MatchWellKnownTaint(key)
			require.True(t, ok)
			assert.Empty(t, match.Category)
			assert.True(t, match.UserFacing)
			taint := internaltypes.NewNodeTaint(v1.Taint{Key: key}, match.Category, match.UserFacing)
			assert.Equal(t, "user", classifyTaintGroup([]internaltypes.NodeTaint{taint}))
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
