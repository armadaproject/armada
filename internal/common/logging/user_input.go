package logging

import "strings"

func SanitizeUserInput(str string) string {
	safeStr := strings.Replace(str, "\n", "", -1)
	return strings.Replace(safeStr, "\r", "", -1)
}
