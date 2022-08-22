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

func Chunk(list []string, size int) [][]string {
	chunks := [][]string{}
	for start := 0; start < len(list); start += size {
		end := start + size
		if end > len(list) {
			end = len(list)
		}
		chunks = append(chunks, list[start:end])
	}
	return chunks
}
