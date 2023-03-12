package utilisation

type StubUtilisationService struct {
	ClusterAvailableCapacityReport     *ClusterAvailableCapacityReport
	GetClusterAvailableCapacityError   error
	AllNodeGroupAllocationInfo         []*NodeGroupAllocationInfo
	GetAllNodeGroupAllocationInfoError error
}

func (f *StubUtilisationService) GetAvailableClusterCapacity(useLegacyIds bool) (*ClusterAvailableCapacityReport, error) {
	return f.ClusterAvailableCapacityReport, f.GetClusterAvailableCapacityError
}

func (f *StubUtilisationService) GetAllNodeGroupAllocationInfo() ([]*NodeGroupAllocationInfo, error) {
	return f.AllNodeGroupAllocationInfo, f.GetAllNodeGroupAllocationInfoError
}
