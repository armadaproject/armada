package configuration

func (c *SchedulingConfig) GetResourceScarcity(pool string) map[string]float64 {
	if c.PoolResourceScarcity != nil {
		s, ok := c.PoolResourceScarcity[pool]
		if ok {
			return s
		}
	}
	return c.ResourceScarcity
}
