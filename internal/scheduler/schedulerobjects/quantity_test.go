package schedulerobjects

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
)

// The memdb internally uses bytes.Compare to compare keys.
// Here, we test that byte representation comparison of quantities works as expected.
func TestQuantityIndexComparison(t *testing.T) {
	tests := map[string]struct {
		A resource.Quantity
		B resource.Quantity
	}{
		"10Mi 10Mi": {
			A: resource.MustParse("10Mi"),
			B: resource.MustParse("10Mi"),
		},
		"5Mi 10Mi": {
			A: resource.MustParse("5Mi"),
			B: resource.MustParse("10Mi"),
		},
		"10Gi 10Gi": {
			A: resource.MustParse("10Gi"),
			B: resource.MustParse("10Gi"),
		},
		"5Gi 10Gi": {
			A: resource.MustParse("5Gi"),
			B: resource.MustParse("10Gi"),
		},
		"1 1": {
			A: resource.MustParse("1"),
			B: resource.MustParse("1"),
		},
		"1 2": {
			A: resource.MustParse("1"),
			B: resource.MustParse("2"),
		},
		"-1 1": {
			A: resource.MustParse("-1"),
			B: resource.MustParse("1"),
		},
		"100m 100m": {
			A: resource.MustParse("100M"),
			B: resource.MustParse("100M"),
		},
		"100m 200m": {
			A: resource.MustParse("100M"),
			B: resource.MustParse("200M"),
		},
		"54870m 54871m": {
			A: resource.MustParse("54870m"),
			B: resource.MustParse("54871m"),
		},
		"1000Ti 1001Ti": {
			A: resource.MustParse("1000Ti"),
			B: resource.MustParse("1001Ti"),
		},
		"1000Pi 1001Pi": {
			A: resource.MustParse("1000Pi"),
			B: resource.MustParse("1001Pi"),
		},
		"1 1001m": {
			A: resource.MustParse("1"),
			B: resource.MustParse("1001m"),
		},
		"1 1000m": {
			A: resource.MustParse("1"),
			B: resource.MustParse("1000m"),
		},
		"1Gi 1001Mi": {
			A: resource.MustParse("1Gi"),
			B: resource.MustParse("1001Mi"),
		},
		"1Gi 1000Mi": {
			A: resource.MustParse("1Gi"),
			B: resource.MustParse("1000Mi"),
		},
		"5188205838208Ki 5188205838209Ki": {
			A: resource.MustParse("5188205838208Ki"),
			B: resource.MustParse("5188205838209Ki"),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			expected := tc.A.Cmp(tc.B)
			actual := bytes.Compare(EncodeQuantity(tc.A), EncodeQuantity(tc.B))
			assert.Equal(t, expected, actual)

			expected = tc.B.Cmp(tc.A)
			actual = bytes.Compare(EncodeQuantity(tc.B), EncodeQuantity(tc.A))
			assert.Equal(t, expected, actual)
		})
	}
}

func TestScaleQuantity(t *testing.T) {
	tests := map[string]struct {
		input    resource.Quantity
		f        float64
		expected resource.Quantity
	}{
		"one": {
			input:    resource.MustParse("1"),
			f:        1,
			expected: resource.MustParse("1"),
		},
		"zero": {
			input:    resource.MustParse("1"),
			f:        0,
			expected: resource.MustParse("0"),
		},
		"rounding": {
			input:    resource.MustParse("1"),
			f:        0.3006,
			expected: resource.MustParse("301m"),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.True(t, tc.expected.Equal(ScaleQuantity(tc.input, tc.f)), "expected %s, but got %s", tc.expected.String(), tc.input.String())
		})
	}
}

func BenchmarkEncodeQuantityBuffer(b *testing.B) {
	out := make([]byte, 8)
	q := resource.MustParse("16Gi")
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		out = out[0:0]
		EncodeQuantityBuffer(out, q)
	}
}
