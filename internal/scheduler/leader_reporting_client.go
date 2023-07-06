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
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/client"
)

const leaseHolderNameToken = "<name>"

type LeaderSchedulingReportClientProvider interface {
	GetCurrentLeaderClient() (schedulerobjects.SchedulerReportingClient, error)
}

type KubernetesLeaderSchedulingReportClientProvider struct {
	currentLeaderName string
	leaderConfig      configuration.LeaderConfig
	clientLock        sync.Mutex
	clientByName      map[string]schedulerobjects.SchedulerReportingClient
}

func NewKubernetesLeaderSchedulingReportClientProvider(leaderConfig configuration.LeaderConfig) *KubernetesLeaderSchedulingReportClientProvider {
	return &KubernetesLeaderSchedulingReportClientProvider{
		leaderConfig:      leaderConfig,
		clientLock:        sync.Mutex{},
		currentLeaderName: "",
		clientByName:      map[string]schedulerobjects.SchedulerReportingClient{},
	}
}

func (l *KubernetesLeaderSchedulingReportClientProvider) GetCurrentLeaderClient() (schedulerobjects.SchedulerReportingClient, error) {
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

func (l *KubernetesLeaderSchedulingReportClientProvider) getClientByName(currentLeaderName string) (schedulerobjects.SchedulerReportingClient, error) {
	l.clientLock.Lock()
	defer l.clientLock.Unlock()

	if leaderClient, present := l.clientByName[currentLeaderName]; present {
		return leaderClient, nil
	}

	leaderConnectionDetails := l.leaderConfig.LeaderConnection
	leaderConnectionDetails.ArmadaUrl = strings.ReplaceAll(leaderConnectionDetails.ArmadaUrl, leaseHolderNameToken, currentLeaderName)

	schedulerApiConnection, err := createApiConnection(leaderConnectionDetails)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating connection to leader")
	}

	leaderClient := schedulerobjects.NewSchedulerReportingClient(schedulerApiConnection)
	l.clientByName[currentLeaderName] = leaderClient
	return leaderClient, nil
}

func (l *KubernetesLeaderSchedulingReportClientProvider) OnStartedLeading(context.Context) {
}

func (l *KubernetesLeaderSchedulingReportClientProvider) OnStoppedLeading() {
}

func (l *KubernetesLeaderSchedulingReportClientProvider) OnNewLeader(identity string) {
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
