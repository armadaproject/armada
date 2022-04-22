package eventapi

type OffsetManager struct {
	offsets map[int64]int64
}

func NewOffsetManager(initialOffsets map[int64]int64) *OffsetManager {
	om := &OffsetManager{
		offsets: initialOffsets,
	}
	return om
}

func (om *OffsetManager) Get(jobsetId int64) (int64, bool) {
	existingOffset, ok := om.offsets[jobsetId]
	return existingOffset, ok
}

func (om *OffsetManager) Update(newOffsets map[int64]int64) {
	for k, v := range newOffsets {
		existingOffset, ok := om.offsets[k]
		if !ok || v > existingOffset {
			om.offsets[k] = v
		}
	}
}
