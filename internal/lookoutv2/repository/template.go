package repository

import (
	"fmt"
	"strings"
)

const (
	templateBegin = "<<<"
	templateEnd   = ">>>"
)

func templateSql(raw string, valuesMap map[string]interface{}) (string, []interface{}) {
	sb := strings.Builder{}
	var args []interface{}
	i := 0
	argIndex := 1
	idToArgIndexMap := make(map[string]int)
	for i < len(raw) {
		if raw[i] != templateBegin[0] {
			// Copy if char isn't start of template
			sb.WriteByte(raw[i])
			i++
			continue
		}
		// tmpIdx at end of templateBegin
		tmpIdx := parseTemplate(raw, i, templateBegin)
		if tmpIdx == -1 {
			// failed to parse template start, continuing
			sb.WriteByte(raw[i])
			i++
			continue
		}
		valueIdBuilder := strings.Builder{}
		for raw[tmpIdx] != templateEnd[0] || tmpIdx >= len(raw) {
			valueIdBuilder.WriteByte(raw[tmpIdx])
			tmpIdx++
		}
		// endIdx after templateEnd
		endIdx := parseTemplate(raw, tmpIdx, templateEnd)
		if endIdx == -1 {
			// failed to parse template end, continuing
			sb.WriteByte(raw[i])
			i++
			continue
		}
		valueId := valueIdBuilder.String()
		value, ok := valuesMap[valueId]
		if !ok {
			// value id not found in map, continuing
			sb.WriteByte(raw[i])
			i++
			continue
		}
		argIndexToUse, ok := idToArgIndexMap[valueId]
		if !ok {
			// New value ID found
			argIndexToUse = argIndex
			args = append(args, value)
			idToArgIndexMap[valueId] = argIndexToUse
			argIndex++
		}
		sb.WriteString(fmt.Sprintf("$%d", argIndexToUse))
		i = endIdx
	}
	return sb.String(), args
}

func idToTemplateString(valueId string) string {
	return fmt.Sprintf("%s%s%s", templateBegin, valueId, templateEnd)
}

func parseTemplate(raw string, startIdx int, templateStr string) int {
	j := 0
	for ; j < len(templateStr); j++ {
		if startIdx+j >= len(raw) || raw[startIdx+j] != templateStr[j] {
			return -1
		}
	}
	return startIdx + j
}
