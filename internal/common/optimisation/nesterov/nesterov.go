package nesterov

import (
	"fmt"
	"math"

	"github.com/pkg/errors"
	"gonum.org/v1/gonum/mat"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/linalg"
)

// Nesterov accelerated gradient descent optimiser; see the following link for details:
// https://fluxml.ai/Flux.jl/stable/training/optimisers/
type Nesterov struct {
	eta float64
	rho float64
	vel *mat.VecDense
}

func New(eta, rho float64) (*Nesterov, error) {
	if eta < 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "eta",
			Value:   eta,
			Message: fmt.Sprintf("outside allowed range [0, Inf)"),
		})
	}
	if rho < 0 || rho >= 1 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "rho",
			Value:   rho,
			Message: fmt.Sprintf("outside allowed range [0, 1)"),
		})
	}
	return &Nesterov{eta: eta, rho: rho}, nil
}

func MustNew(eta, rho float64) *Nesterov {
	opt, err := New(eta, rho)
	if err != nil {
		panic(err)
	}
	return opt
}

func (o *Nesterov) Update(out, p *mat.VecDense, g mat.Vector) *mat.VecDense {
	out.CopyVec(p)
	out.AddScaledVec(out, math.Pow(o.rho, 2), o.vel)
	out.AddScaledVec(out, -(1+o.rho)*o.eta, g)

	o.vel.ScaleVec(o.rho, o.vel)
	o.vel.AddScaledVec(o.vel, -o.eta, g)
	return p
}

func (o *Nesterov) Extend(n int) {
	o.vel = linalg.ExtendVecDense(o.vel, n)
}
