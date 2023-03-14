package utilisation

type StubUtilisationService struct {
	ClusterAvailableCapacityReport     *ClusterAvailableCapacityReport
	GetClusterAvailableCapacityError   error
	AllNodeGroupAllocationInfo         []*NodeGroupAllocationInfo
	GetAllNodeGroupAllocationInfoError error
}

func (f *StubUtilisationService) GetAvailableClusterCapacity(legacy bool) (*ClusterAvailableCapacityReport, error) {
	return f.ClusterAvailableCapacityReport, f.GetClusterAvailableCapacityError
}

func (f *StubUtilisationService) GetAllNodeGroupAllocationInfo(legacy bool) ([]*NodeGroupAllocationInfo, error) {
	return f.AllNodeGroupAllocationInfo, f.GetAllNodeGroupAllocationInfoError
}
