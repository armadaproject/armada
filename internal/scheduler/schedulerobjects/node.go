package schedulerobjects

func (node *Node) AvailableArmadaResource() ResourceList {
	tr := node.TotalResources.DeepCopy()
	for _, rl := range node.UnallocatableResources {
		tr.Sub(rl)
	}
	return tr
}
