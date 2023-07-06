package scheduler

import (
	"testing"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/stretchr/testify/assert"
)

const currentProcessPodName = "current-process-name"

var defaultLeaderConfig = configuration.LeaderConfig{
	PodName: currentProcessPodName,
	LeaderConnection: client.ApiConnectionDetails{
		ArmadaUrl: "localhost:50052",
	},
}

var templatedLeader = configuration.LeaderConfig{
	PodName: currentProcessPodName,
	LeaderConnection: client.ApiConnectionDetails{
		ArmadaUrl: "<name>.localhost:50052",
	},
}

func TestGetCurrentLeaderClientConnection(t *testing.T) {
	clientProvider := NewLeaderConnectionProvider(defaultLeaderConfig)
	clientProvider.OnNewLeader("new-leader")

	result, err := clientProvider.GetCurrentLeaderClientConnection()
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, result.Target(), defaultLeaderConfig.LeaderConnection.ArmadaUrl)
}

func TestGetCurrentLeaderClientConnection_WithTemplatedConnection(t *testing.T) {
	clientProvider := NewLeaderConnectionProvider(templatedLeader)

	clientProvider.OnNewLeader("new-leader")
	result, err := clientProvider.GetCurrentLeaderClientConnection()
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, result.Target(), "new-leader.localhost:50052")

	clientProvider.OnNewLeader("new-leader-2")
	result, err = clientProvider.GetCurrentLeaderClientConnection()
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, result.Target(), "new-leader-2.localhost:50052")
}

func TestGetCurrentLeaderClientConnection_NoLeader(t *testing.T) {
	clientProvider := NewLeaderConnectionProvider(defaultLeaderConfig)

	result, err := clientProvider.GetCurrentLeaderClientConnection()
	assert.Nil(t, result)
	assert.Error(t, err)
}

func TestGetCurrentLeaderClientConnection_OnCurrentProcessIsLeader(t *testing.T) {
	clientProvider := NewLeaderConnectionProvider(defaultLeaderConfig)
	clientProvider.OnNewLeader(currentProcessPodName)

	result, err := clientProvider.GetCurrentLeaderClientConnection()
	assert.Nil(t, result)
	assert.NoError(t, err)
}
