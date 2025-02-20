package priorityoverride

// StaticProvider is Provider that loads priority overrides from a static map
type StaticProvider struct {
	overrides map[overrideKey]float64
}

func NewStaticProvider(overrides map[overrideKey]float64) Provider {
	return &StaticProvider{overrides: overrides}
}

func NewNoOpProvider() Provider {
	return NewStaticProvider(map[overrideKey]float64{})
}

func (s *StaticProvider) Ready() bool {
	return true
}

func (s *StaticProvider) Override(pool, queue string) (float64, bool, error) {
	multiplier, ok := s.overrides[overrideKey{pool: pool, queue: queue}]
	return multiplier, ok, nil
}
