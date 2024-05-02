package internaltypes

import (
	"testing"

	"gopkg.in/inf.v0"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
)

type quantityTest struct {
	q                    resource.Quantity
	expectedIntRoundDown int64
	expectedIntRoundUp   int64
}

func TestQuantityToInt64_WithScaleMillis(t *testing.T) {
	tests := []quantityTest{
		{resource.MustParse("0"), 0, 0},
		{resource.MustParse("1"), 1000, 1000},
		{resource.MustParse("1m"), 1, 1},
		{resource.MustParse("50m"), 50, 50},
		{resource.MustParse("1Mi"), 1024 * 1024 * 1000, 1024 * 1024 * 1000},
		{resource.MustParse("1e3"), 1000 * 1000, 1000 * 1000},
		{*resource.NewMilliQuantity(1, resource.DecimalExponent), 1, 1},
		{*resource.NewDecimalQuantity(*inf.NewDec(1, inf.Scale(0)), resource.DecimalExponent), 1000, 1000},
		{resource.MustParse("1n"), 0, 1},
		{resource.MustParse("100n"), 0, 1},
		{resource.MustParse("999999999n"), 999, 1000},
		{resource.MustParse("1000000000n"), 1000, 1000},
		{resource.MustParse("1000000001n"), 1000, 1001},
		{resource.MustParse("12.3m"), 12, 13},
		{resource.MustParse("0.99m"), 0, 1},
		{resource.MustParse("1.001m"), 1, 2},
	}

	for _, test := range tests {
		assert.Equal(t, test.expectedIntRoundDown, QuantityToInt64RoundDown(test.q, resource.Milli), test.q)
		assert.Equal(t, test.expectedIntRoundUp, QuantityToInt64RoundUp(test.q, resource.Milli), test.q)
	}
}

func TestQuantityToInt64_WithUnitScale(t *testing.T) {
	const tebi = 1024 * 1024 * 1024 * 1024
	tests := []quantityTest{
		{resource.MustParse("0"), 0, 0},
		{resource.MustParse("1"), 1, 1},
		{resource.MustParse("1m"), 0, 1},
		{resource.MustParse("50m"), 0, 1},
		{resource.MustParse("1Mi"), 1024 * 1024, 1024 * 1024},
		{resource.MustParse("1.5Mi"), 1536 * 1024, 1536 * 1024},
		{resource.MustParse("4Ti"), 4 * tebi, 4 * tebi},
		{resource.MustParse("100000Ti"), 100000 * tebi, 100000 * tebi},
		{resource.MustParse("1e3"), 1000, 1000},
		{*resource.NewMilliQuantity(1, resource.DecimalExponent), 0, 1},
		{*resource.NewDecimalQuantity(*inf.NewDec(1, inf.Scale(0)), resource.DecimalExponent), 1, 1},
		{resource.MustParse("1n"), 0, 1},
		{resource.MustParse("100n"), 0, 1},
		{resource.MustParse("999999999n"), 0, 1},
		{resource.MustParse("1000000000n"), 1, 1},
		{resource.MustParse("1000000001n"), 1, 2},
	}

	for _, test := range tests {
		assert.Equal(t, test.expectedIntRoundDown, QuantityToInt64RoundDown(test.q, resource.Scale(0)), test.q)
		assert.Equal(t, test.expectedIntRoundUp, QuantityToInt64RoundUp(test.q, resource.Scale(0)), test.q)
	}
}
