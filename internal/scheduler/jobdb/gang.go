package jobdb

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/server/configuration"
)

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

func GangInfoFromMinimalJob(job interfaces.MinimalJob) (*GangInfo, error) {
	basicGangInfo := BasicJobGangInfo()
	annotations := job.Annotations()
	gangId, ok := annotations[configuration.GangIdAnnotation]
	if !ok {
		// Not a gang, default to basic gang info
		return &basicGangInfo, nil
	}
	if gangId == "" {
		return nil, errors.Errorf("gang id is empty")
	}

	gangCardinalityString, ok := annotations[configuration.GangCardinalityAnnotation]
	if !ok {
		return nil, errors.Errorf("gang cardinality annotation %s is missing", configuration.GangCardinalityAnnotation)
	}
	gangCardinality, err := strconv.Atoi(gangCardinalityString)
	if err != nil {
		return nil, fmt.Errorf("gang cardinality is not parseable - %s", errors.WithStack(err))
	}
	if gangCardinality <= 0 {
		return nil, errors.Errorf("gang cardinality %d is non-positive", gangCardinality)
	}
	if gangCardinality < 2 {
		// Not a gang, default to basic gang info
		return &basicGangInfo, nil
	}

	nodeUniformityLabel := job.Annotations()[configuration.GangNodeUniformityLabelAnnotation]
	gangInfo := CreateGangInfo(gangId, gangCardinality, nodeUniformityLabel)
	return &gangInfo, nil
}
