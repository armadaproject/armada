package util

import (
	"fmt"
	"math"
)

func Min(a, b int) int {
	if b < a {
		return b
	}
	return a
}

func FormatBinarySI(q int64) string {
	ti := math.Pow(2, 40)
	gi := math.Pow(2, 30)
	mi := math.Pow(2, 20)
	ki := math.Pow(2, 10)
	f64 := float64(q)
	switch {
	case f64 > ti:
		return fmt.Sprintf("%.2fTi", f64/ti)
	case f64 > gi:
		return fmt.Sprintf("%.2fGi", f64/gi)
	case f64 > mi:
		return fmt.Sprintf("%.2fMi", f64/mi)
	case f64 > ki:
		return fmt.Sprintf("%.2fKi", f64/ki)
	default:
		return fmt.Sprintf("%d", q)
	}
}
