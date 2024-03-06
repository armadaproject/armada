package linalg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gonum.org/v1/gonum/mat"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
)

func TestExtendVecDense(t *testing.T) {
	tests := map[string]struct {
		vec      *mat.VecDense
		n        int
		expected *mat.VecDense
	}{
		"nil vec": {
			vec:      nil,
			n:        3,
			expected: mat.NewVecDense(3, armadaslices.Zeros[float64](3)),
		},
		"extend": {
			vec:      mat.NewVecDense(1, armadaslices.Zeros[float64](1)),
			n:        3,
			expected: mat.NewVecDense(3, armadaslices.Zeros[float64](3)),
		},
		"extend unnecessary due to greater length": {
			vec:      mat.NewVecDense(3, armadaslices.Zeros[float64](3)),
			n:        1,
			expected: mat.NewVecDense(3, armadaslices.Zeros[float64](3)),
		},
		"extend unnecessary due to equal length": {
			vec:      mat.NewVecDense(3, armadaslices.Zeros[float64](3)),
			n:        3,
			expected: mat.NewVecDense(3, armadaslices.Zeros[float64](3)),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			actual := ExtendVecDense(tc.vec, tc.n)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
