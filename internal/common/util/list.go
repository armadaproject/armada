package util

func SubtractStringList(a []string, b []string) []string {
	bSet := StringListToSet(b)
	result := []string{}
	for _, item := range a {
		if !bSet[item] {
			result = append(result, item)
		}
	}
	return result
}

func StringListToSet(list []string) map[string]bool {
	set := map[string]bool{}
	for _, item := range list {
		set[item] = true
	}
	return set
}

func ContainsString(list []string, val string) bool {
	for _, elem := range list {
		if elem == val {
			return true
		}
	}
	return false
}

func DeepCopyListUint32(list []uint32) []uint32 {
	result := make([]uint32, 0, len(list))
	for _, v := range list {
		result = append(result, v)
	}
	return result
}

func Filter[S ~[]E, E any](s S, predicate func(e E) bool) S {
	if s == nil {
		return nil
	}
	out := make(S, 0, len(s))
	for _, e := range s {
		if predicate(e) {
			out = append(out, e)
		}
	}
	return out
}

func Concat[T any](slices ...[]T) []T {
	total := 0
	for _, s := range slices {
		total += len(s)
	}
	result := make([]T, total)
	var i int
	for _, s := range slices {
		i += copy(result[i:], s)
	}
	return result
}

func Map[T any, U any](list []T, fn func(val T) U) []U {
	out := make([]U, len(list))
	for i, val := range list {
		out[i] = fn(val)
	}
	return out
}
