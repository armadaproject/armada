package mocks

import "github.com/armadaproject/armada/internal/executor/utilisation"

type StubUtilisationService struct {
	ClusterAvailableCapacityReport     *utilisation.ClusterAvailableCapacityReport
	GetClusterAvailableCapacityError   error
	AllNodeGroupAllocationInfo         []*utilisation.NodeGroupAllocationInfo
	GetAllNodeGroupAllocationInfoError error
}

func (f *StubUtilisationService) GetAvailableClusterCapacity(useLegacyIds bool) (*utilisation.ClusterAvailableCapacityReport, error) {
	return f.ClusterAvailableCapacityReport, f.GetClusterAvailableCapacityError
}

func (f *StubUtilisationService) GetAllNodeGroupAllocationInfo() ([]*utilisation.NodeGroupAllocationInfo, error) {
	return f.AllNodeGroupAllocationInfo, f.GetAllNodeGroupAllocationInfoError
}
