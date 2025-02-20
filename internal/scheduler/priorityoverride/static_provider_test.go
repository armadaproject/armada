package priorityoverride

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestStaticProvider_testEmpty(t *testing.T) {
	provider := NewNoOpProvider()
	_, ok, err := provider.Override("queue", "pool")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestStaticProvider_testNonEmpty(t *testing.T) {
	provider := NewStaticProvider(map[overrideKey]float64{
		{
			queue: "testQueue",
			pool:  "testPool",
		}: 100.0,
	})

	override, ok, err := provider.Override("testPool", "testQueue")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 100.0, override)

	_, ok, err = provider.Override("testQueue", "testPool2")
	require.NoError(t, err)
	assert.False(t, ok)

	_, ok, err = provider.Override("testQueue2", "testPool")
	require.NoError(t, err)
	assert.False(t, ok)
}
