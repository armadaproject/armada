package replay

import (
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
)

var quantityParts = regexp.MustCompile(`^([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))([a-zA-Z][+-]?[0-9]*|[KMGTPE]i)?$`)

var decimalPowers = map[string]int64{
	"": 0, "n": -9, "u": -6, "m": -3, "k": 3,
	"M": 6, "G": 9, "T": 12, "P": 15, "E": 18,
}

func exactQuantity(text string) (resource.Quantity, error) {
	if len(text) > 128 {
		return resource.Quantity{}, fmt.Errorf("resource quantity exceeds 128 bytes")
	}
	parts := quantityParts.FindStringSubmatch(text)
	if parts == nil {
		return resource.Quantity{}, fmt.Errorf("invalid resource quantity %q", text)
	}
	value, ok := new(big.Rat).SetString(parts[1])
	if !ok || value.Sign() < 0 {
		return resource.Quantity{}, fmt.Errorf("expected a nonnegative resource quantity, got %q", text)
	}
	suffix := parts[2]
	base := int64(10)
	var exponent int64
	switch {
	case len(suffix) == 2 && suffix[1] == 'i':
		base = 2
		exponent = 10 * int64(strings.IndexByte("KMGTPE", suffix[0])+1)
	case len(suffix) > 1 && (suffix[0] == 'e' || suffix[0] == 'E'):
		n, err := strconv.ParseInt(suffix[1:], 10, 8)
		if err != nil || n < -24 || n > 24 {
			return resource.Quantity{}, fmt.Errorf("resource quantity exponent outside [-24, 24]")
		}
		exponent = n
	default:
		exponent, ok = decimalPowers[suffix]
		if !ok {
			return resource.Quantity{}, fmt.Errorf("invalid resource suffix %q", suffix)
		}
	}
	factor := new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(base), big.NewInt(max(exponent, -exponent)), nil))
	if exponent < 0 {
		value.Quo(value, factor)
	} else {
		value.Mul(value, factor)
	}
	quantity, err := resource.ParseQuantity(text)
	if err != nil {
		return resource.Quantity{}, err
	}
	actual, ok := new(big.Rat).SetString(quantity.AsDec().String())
	if !ok || value.Cmp(actual) != 0 {
		return resource.Quantity{}, fmt.Errorf("Kubernetes rounds or clamps quantity %q", text)
	}
	return quantity, nil
}
