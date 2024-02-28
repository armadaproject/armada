package descent

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gonum.org/v1/gonum/mat"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
)

func TestDescent(t *testing.T) {
	tests := map[string]struct {
		eta      float64
		p        *mat.VecDense
		g        *mat.VecDense
		expected *mat.VecDense
	}{
		"eta is zero": {
			eta:      0.0,
			p:        mat.NewVecDense(2, armadaslices.Ones[float64](2)),
			g:        mat.NewVecDense(2, armadaslices.Ones[float64](2)),
			expected: mat.NewVecDense(2, armadaslices.Ones[float64](2)),
		},
		"eta is non-zero": {
			eta: 2.0,
			p:   mat.NewVecDense(2, armadaslices.Zeros[float64](2)),
			g:   mat.NewVecDense(2, armadaslices.Ones[float64](2)),
			expected: func() *mat.VecDense {
				rv := mat.NewVecDense(2, armadaslices.Ones[float64](2))
				rv.ScaleVec(-2, rv)
				return rv
			}(),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			opt := MustNew(tc.eta)
			rv := opt.Update(tc.p, tc.p, tc.g)
			assert.Equal(t, tc.p, rv)
			assert.Equal(t, tc.expected, tc.p)
		})
	}
}
