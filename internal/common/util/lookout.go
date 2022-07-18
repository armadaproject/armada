package util

import "strings"

const MaxMessageLength = 2048

func RemoveNullsFromString(s string) string {
	return strings.ReplaceAll(s, "\000", "")
}

func RemoveNullsFromJson(json []byte) []byte {
	jsonString := string(json)
	jsonString = strings.ReplaceAll(jsonString, "\\u0000", "")
	return []byte(jsonString)
}
