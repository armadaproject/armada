package optimisation

import "gonum.org/v1/gonum/mat"

type Optimiser interface {
	Update(out, p *mat.VecDense, g mat.Vector) *mat.VecDense
	Extend(n int)
}
