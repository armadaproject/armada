package hash

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func CalculatePodRequirementsHash(reqs *schedulerobjects.PodRequirements) ([]byte, error) {
	if reqs == nil {
		return []byte{}, errors.Errorf("cannot calculate hash of nil pod requirements")
	}
	podReqsCopy := proto.Clone(reqs).(*schedulerobjects.PodRequirements)
	// Clear annotations, as otherwise it'll cause the hash to be different for equivalent requirements
	// Ideally long term we'd remove annotations from the PodRequirements
	podReqsCopy.Annotations = map[string]string{}
	reqsHash, err := protoutil.Hash(podReqsCopy)
	if err != nil {
		return []byte{}, err
	}
	return reqsHash, nil
}
