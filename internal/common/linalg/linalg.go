package linalg

import "gonum.org/v1/gonum/mat"

// ExtendVecDense extends the length of vec in-place to be at least n.
func ExtendVecDense(vec *mat.VecDense, n int) *mat.VecDense {
	if vec == nil {
		return mat.NewVecDense(n, make([]float64, n))
	}
	rawVec := vec.RawVector()
	d := n - rawVec.N
	if d <= 0 {
		return vec
	}
	rawVec.Data = append(rawVec.Data, make([]float64, d)...)
	rawVec.N = n
	vec.SetRawVector(rawVec)
	return vec
}
