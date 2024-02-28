package optimisation

import "gonum.org/v1/gonum/mat"

// Optimiser represents a first-order optimisation algorithm.
type Optimiser interface {
	// Update the parameters p using gradient g and store the result in out.
	Update(out, p *mat.VecDense, g mat.Vector) *mat.VecDense
	// Extend the internal state of the optimiser to accommodate at least n parameters.
	Extend(n int)
}
