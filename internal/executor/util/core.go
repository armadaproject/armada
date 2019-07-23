package util

func Min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func DeepCopy(originalMap map[string]string) map[string]string {
	targetMap := make(map[string]string)

	for key, value := range originalMap {
		targetMap[key] = value
	}

	return targetMap
}
