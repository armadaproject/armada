package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/pkg/client"
)

const leaseHolderNameToken = "<name>"

type LeaderClientConnectionProvider interface {
	GetCurrentLeaderClientConnection() (*grpc.ClientConn, error)
}

type LeaderConnectionProvider struct {
	currentLeaderName string
	leaderConfig      configuration.LeaderConfig
	connectionLock    sync.Mutex
	connectionByName  map[string]*grpc.ClientConn
}

func NewLeaderConnectionProvider(leaderConfig configuration.LeaderConfig) *LeaderConnectionProvider {
	return &LeaderConnectionProvider{
		leaderConfig:      leaderConfig,
		connectionLock:    sync.Mutex{},
		currentLeaderName: "",
		connectionByName:  map[string]*grpc.ClientConn{},
	}
}

func (l *LeaderConnectionProvider) GetCurrentLeaderClientConnection() (*grpc.ClientConn, error) {
	currentLeader := l.currentLeaderName
	if currentLeader == "" {
		return nil, fmt.Errorf("no leader found to retrieve scheduling reports from")
	}

	if currentLeader == l.leaderConfig.PodName {
		return nil, nil
	}

	leaderClient, err := l.getClientByName(currentLeader)
	return leaderClient, err
}

func (l *LeaderConnectionProvider) getClientByName(currentLeaderName string) (*grpc.ClientConn, error) {
	l.connectionLock.Lock()
	defer l.connectionLock.Unlock()

	if leaderClient, present := l.connectionByName[currentLeaderName]; present {
		return leaderClient, nil
	}

	leaderConnectionDetails := l.leaderConfig.LeaderConnection
	leaderConnectionDetails.ArmadaUrl = strings.ReplaceAll(leaderConnectionDetails.ArmadaUrl, leaseHolderNameToken, currentLeaderName)

	apiConnection, err := createApiConnection(leaderConnectionDetails)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating connection to leader")
	}

	l.connectionByName[currentLeaderName] = apiConnection
	return apiConnection, nil
}

func (l *LeaderConnectionProvider) OnStartedLeading(context.Context) {
}

func (l *LeaderConnectionProvider) OnStoppedLeading() {
}

func (l *LeaderConnectionProvider) OnNewLeader(identity string) {
	l.currentLeaderName = identity
}

func createApiConnection(connectionDetails client.ApiConnectionDetails) (*grpc.ClientConn, error) {
	grpc_prometheus.EnableClientHandlingTimeHistogram()
	return client.CreateApiConnectionWithCallOptions(
		&connectionDetails,
		[]grpc.CallOption{},
		grpc.WithChainUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(grpc_prometheus.StreamClientInterceptor),
	)
}
