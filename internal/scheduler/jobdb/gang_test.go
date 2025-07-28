package jobdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGangInfo_BasicJobGangInfo(t *testing.T) {
	info := BasicJobGangInfo()

	assert.False(t, info.IsGang())
	assert.Empty(t, info.NodeUniformity())
	assert.Empty(t, info.Id())
	assert.Equal(t, 1, info.Cardinality())
}

func TestGangInfo_CreateGangInfo(t *testing.T) {
	info := CreateGangInfo("id", 4, "gang-label")

	assert.True(t, info.IsGang())
	assert.Equal(t, "gang-label", info.NodeUniformity())
	assert.Equal(t, "id", info.Id())
	assert.Equal(t, 4, info.Cardinality())
}

func TestGangInfo_IsGang(t *testing.T) {
	info := CreateGangInfo("id", 1, "gang-label")
	// Is false if cardinality <= 1
	assert.False(t, info.IsGang())

	info = CreateGangInfo("id", 2, "gang-label")
	// Is true if cardinality > 1
	assert.True(t, info.IsGang())
}
