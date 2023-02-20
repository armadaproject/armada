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

func Filter[T any](list []T, predicate func(val T) bool) []T {
	if list == nil {
		return nil
	}
	out := make([]T, 0, len(list))
	for _, val := range list {
		if predicate(val) {
			out = append(out, val)
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
