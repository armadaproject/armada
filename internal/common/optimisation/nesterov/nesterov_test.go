package nesterov

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gonum.org/v1/gonum/mat"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
)

func TestNesterov(t *testing.T) {
	tests := map[string]struct {
		eta       float64
		rho       float64
		p0        *mat.VecDense
		gs        []*mat.VecDense
		expecteds []*mat.VecDense
	}{
		"eta is zero": {
			eta: 0.0,
			rho: 0.9,
			p0:  mat.NewVecDense(2, armadaslices.Ones[float64](2)),
			gs: []*mat.VecDense{
				mat.NewVecDense(2, armadaslices.Ones[float64](2)),
				mat.NewVecDense(2, armadaslices.Ones[float64](2)),
			},
			expecteds: []*mat.VecDense{
				mat.NewVecDense(2, armadaslices.Ones[float64](2)),
				mat.NewVecDense(2, armadaslices.Ones[float64](2)),
			},
		},
		"rho is zero": {
			eta: 2.0,
			rho: 0.0,
			p0:  mat.NewVecDense(2, armadaslices.Zeros[float64](2)),
			gs: []*mat.VecDense{
				mat.NewVecDense(2, armadaslices.Ones[float64](2)),
				mat.NewVecDense(2, armadaslices.Ones[float64](2)),
			},
			expecteds: func() []*mat.VecDense {
				rv := make([]*mat.VecDense, 2)
				rv[0] = mat.NewVecDense(2, armadaslices.Ones[float64](2))
				rv[0].ScaleVec(-2, rv[0])
				rv[1] = mat.NewVecDense(2, armadaslices.Ones[float64](2))
				rv[1].ScaleVec(-4, rv[1])
				return rv
			}(),
		},
		"eta and rho non-zero": {
			eta: 2.0,
			rho: 0.5,
			p0:  mat.NewVecDense(2, armadaslices.Zeros[float64](2)),
			gs: []*mat.VecDense{
				mat.NewVecDense(2, armadaslices.Ones[float64](2)),
				mat.NewVecDense(2, armadaslices.Ones[float64](2)),
				mat.NewVecDense(2, armadaslices.Ones[float64](2)),
			},
			expecteds: func() []*mat.VecDense {
				rv := make([]*mat.VecDense, 3)
				rv[0] = mat.NewVecDense(2, armadaslices.Ones[float64](2))
				rv[0].ScaleVec(-3, rv[0])
				rv[1] = mat.NewVecDense(2, armadaslices.Ones[float64](2))
				rv[1].ScaleVec(-6.5, rv[1])
				rv[2] = mat.NewVecDense(2, armadaslices.Ones[float64](2))
				rv[2].ScaleVec(-10.25, rv[2])
				return rv
			}(),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			opt := MustNew(tc.eta, tc.rho)
			p := tc.p0
			for i, g := range tc.gs {
				opt.Extend(g.Len())
				rv := opt.Update(p, p, g)
				assert.Equal(t, p, rv)
				assert.Equal(t, tc.expecteds[i], p)
			}
		})
	}
}
