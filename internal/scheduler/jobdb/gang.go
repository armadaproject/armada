package jobdb

type GangInfo struct {
	isGang         bool
	id             string
	cardinality    int
	nodeUniformity string
}

var basicJobGangInfo = GangInfo{
	id:             "",
	isGang:         false,
	cardinality:    1,
	nodeUniformity: "",
}

// BasicJobGangInfo The info used for non-gang jobs
func BasicJobGangInfo() GangInfo {
	return basicJobGangInfo
}

func CreateGangInfo(id string, cardinality int, nodeUniformity string) GangInfo {
	return GangInfo{
		id:             id,
		isGang:         true,
		cardinality:    cardinality,
		nodeUniformity: nodeUniformity,
	}
}

func (g GangInfo) IsGang() bool {
	return g.isGang
}

func (g GangInfo) Id() string {
	return g.id
}

func (g GangInfo) Cardinality() int {
	return g.cardinality
}

func (g GangInfo) NodeUniformity() string {
	return g.nodeUniformity
}

func (g GangInfo) Equal(other GangInfo) bool {
	// Currently we only have comparable fields, so we can rely on simple equality check
	return g == other
}
