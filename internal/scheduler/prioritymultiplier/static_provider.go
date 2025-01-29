package prioritymultiplier

// StaticProvider is Provider that loads priority overrides from a static map
type StaticProvider struct {
	multipliers map[multiplierKey]float64
}

func NewStaticProvider(multipliers map[multiplierKey]float64) Provider {
	return &StaticProvider{multipliers: multipliers}
}

func NewNoOpProvider() Provider {
	return NewStaticProvider(map[multiplierKey]float64{})
}

func (s *StaticProvider) Ready() bool {
	return true
}

func (s *StaticProvider) Multiplier(pool, queue string) (float64, error) {
	multiplier, ok := s.multipliers[multiplierKey{pool: pool, queue: queue}]
	if !ok {
		return 1.0, nil
	}
	return multiplier, nil
}
