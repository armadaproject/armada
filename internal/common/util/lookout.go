package util

import "strings"

func RemoveNullsFromJson(json []byte) []byte {
	jsonString := string(json)
	jsonString = strings.ReplaceAll(jsonString, "\\u0000", "")
	return []byte(jsonString)
}

func TruncateAndRemoveNullsFromString(s string, max int) string {
	s = strings.ReplaceAll(s, "\000", "")
	if max > len(s) {
		return s
	}
	return s[:max]
}
