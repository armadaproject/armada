package serving

type SequenceManager interface {
	Get(jobsetId int64) (int64, bool)
	Update(newOffsets map[int64]int64)
}

type DefaultSequenceManager struct {
	sequences map[int64]int64
}

func NewSequenceManager(intialSequences map[int64]int64) *DefaultSequenceManager {
	om := &DefaultSequenceManager{
		sequences: intialSequences,
	}
	return om
}

func (sm *DefaultSequenceManager) Get(jobsetId int64) (int64, bool) {
	existingOffset, ok := sm.sequences[jobsetId]
	return existingOffset, ok
}

func (sm *DefaultSequenceManager) Update(newOffsets map[int64]int64) {
	for k, v := range newOffsets {
		existingOffset, ok := sm.sequences[k]
		if !ok || v > existingOffset {
			sm.sequences[k] = v
		}
	}
}
