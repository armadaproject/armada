package scheduler

import (
	"github.com/armadaproject/armada/internal/common/context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/pkg/client"
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
	leaderController := &FakeLeaderController{}
	clientProvider := NewLeaderConnectionProvider(leaderController, defaultLeaderConfig)
	leaderController.LeaderName = "new-leader"

	isCurrentProcessLeader, result, err := clientProvider.GetCurrentLeaderClientConnection()
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, isCurrentProcessLeader)
	assert.Equal(t, result.Target(), defaultLeaderConfig.LeaderConnection.ArmadaUrl)
}

func TestGetCurrentLeaderClientConnection_WithTemplatedConnection(t *testing.T) {
	leaderController := &FakeLeaderController{}
	clientProvider := NewLeaderConnectionProvider(leaderController, templatedLeader)

	leaderController.LeaderName = "new-leader"
	isCurrentProcessLeader, result, err := clientProvider.GetCurrentLeaderClientConnection()
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, isCurrentProcessLeader)
	assert.Equal(t, result.Target(), "new-leader.localhost:50052")

	leaderController.LeaderName = "new-leader-2"
	isCurrentProcessLeader, result, err = clientProvider.GetCurrentLeaderClientConnection()
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, isCurrentProcessLeader)
	assert.Equal(t, result.Target(), "new-leader-2.localhost:50052")
}

func TestGetCurrentLeaderClientConnection_NoLeader(t *testing.T) {
	leaderController := &FakeLeaderController{}
	clientProvider := NewLeaderConnectionProvider(leaderController, defaultLeaderConfig)

	isCurrentProcessLeader, result, err := clientProvider.GetCurrentLeaderClientConnection()
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.False(t, isCurrentProcessLeader)
}

func TestGetCurrentLeaderClientConnection_OnCurrentProcessIsLeader(t *testing.T) {
	leaderController := &FakeLeaderController{}
	clientProvider := NewLeaderConnectionProvider(leaderController, defaultLeaderConfig)
	leaderController.IsCurrentlyLeader = true

	isCurrentProcessLeader, result, err := clientProvider.GetCurrentLeaderClientConnection()
	assert.Nil(t, result)
	assert.NoError(t, err)
	assert.True(t, isCurrentProcessLeader)
}

type FakeLeaderController struct {
	IsCurrentlyLeader bool
	LeaderName        string
}

func (f *FakeLeaderController) GetToken() LeaderToken {
	return NewLeaderToken()
}

func (f *FakeLeaderController) ValidateToken(tok LeaderToken) bool {
	return f.IsCurrentlyLeader
}

func (f *FakeLeaderController) Run(ctx *context.ArmadaContext) error {
	return nil
}

func (f *FakeLeaderController) GetLeaderReport() LeaderReport {
	return LeaderReport{
		LeaderName:             f.LeaderName,
		IsCurrentProcessLeader: f.IsCurrentlyLeader,
	}
}
