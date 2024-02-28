package optimisation

import "gonum.org/v1/gonum/mat"

// Optimiser represents a first-order optimisation algorithm.
type Optimiser interface {
	// Update the parameters using gradient and store the result in out.
	Update(out, parameters *mat.VecDense, gradient mat.Vector) *mat.VecDense
	// Extend the internal state of the optimiser to accommodate at least n parameters.
	Extend(n int)
}
