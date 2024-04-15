package taint

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var testTaints = []v1.Taint{
	{Key: "key1", Value: "val", Effect: v1.TaintEffectNoSchedule, TimeAdded: nil},
	{Key: "key2", Value: "val", Effect: v1.TaintEffectNoExecute, TimeAdded: &metav1.Time{time.Now()}},
}

func TestDeepCopy(t *testing.T) {
	assert.Equal(t, testTaints[0], DeepCopyTaint(&testTaints[0]))
	assert.Equal(t, testTaints, DeepCopyTaints(testTaints))
}

func TestDeepCopyInternStrings(t *testing.T) {
	seen := map[string]bool{}
	addToSeen := func(s string) string { seen[s] = true; return s }
	assert.Equal(t, testTaints[0], DeepCopyTaintInternStrings(&testTaints[0], addToSeen))
	assert.Equal(t, testTaints, DeepCopyTaintsInternStrings(testTaints, addToSeen))
	assert.Equal(t, map[string]bool{"NoExecute": true, "NoSchedule": true, "key1": true, "key2": true, "val": true}, seen)
}

func TestFindMatchingUntoleratedTaint_AllTolerated(t *testing.T) {
	firstTaintFound, found := FindMatchingUntoleratedTaint(testTaints, []v1.Toleration{
		{Key: "key1", Value: "val", Effect: v1.TaintEffectNoSchedule, Operator: v1.TolerationOpEqual},
		{Key: "key2", Value: "val", Effect: v1.TaintEffectNoExecute, Operator: v1.TolerationOpEqual},
	})
	assert.Empty(t, firstTaintFound.Key)
	assert.Empty(t, firstTaintFound.Value)
	assert.False(t, found)
}

func TestFindMatchingUntoleratedTaint_OneTolerated(t *testing.T) {
	firstTaintFound, found := FindMatchingUntoleratedTaint(testTaints, []v1.Toleration{
		{Key: "key1", Value: "val", Effect: v1.TaintEffectNoSchedule, Operator: v1.TolerationOpEqual},
	})
	assert.Equal(t, testTaints[1], firstTaintFound)
	assert.True(t, found)
}
