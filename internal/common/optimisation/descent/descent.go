package descent

import (
	"fmt"

	"github.com/pkg/errors"
	"gonum.org/v1/gonum/mat"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
)

// Gradient descent optimiser; see the following link for details:
// https://fluxml.ai/Flux.jl/stable/training/optimisers/
type Descent struct {
	eta float64
}

func New(eta float64) (*Descent, error) {
	if eta < 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "eta",
			Value:   eta,
			Message: fmt.Sprintf("outside allowed range [0, Inf)"),
		})
	}
	return &Descent{eta: eta}, nil
}

func MustNew(eta float64) *Descent {
	opt, err := New(eta)
	if err != nil {
		panic(err)
	}
	return opt
}

func (o *Descent) Update(out, p *mat.VecDense, g mat.Vector) *mat.VecDense {
	out.AddScaledVec(p, -o.eta, g)
	return p
}

func (o *Descent) Extend(_ int) {
	return
}
