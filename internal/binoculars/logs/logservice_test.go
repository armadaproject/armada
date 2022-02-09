package logs

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertLogs_ReturnsLogLineWithTime(t *testing.T) {
	line := "2022-02-08T11:32:21.183268868Z Hello world!"
	logLines := ConvertLogs([]byte(line))

	assert.Len(t, logLines, 1)
	assert.Equal(t, "2022-02-08T11:32:21.183268868Z", logLines[0].Timestamp)
	assert.Equal(t, "Hello world!", logLines[0].Line)
}

func TestConvertLogs_ReturnsUnknownTimeForLogWithNoTimestamp(t *testing.T) {
	line := "Hello world!"
	logLines := ConvertLogs([]byte(line))

	assert.Len(t, logLines, 1)
	assert.Equal(t, "", logLines[0].Timestamp)
	assert.Equal(t, "Hello world!", logLines[0].Line)
}

func TestConvertLogs_EmptyLog(t *testing.T) {
	line := "2022-02-08T11:32:21.183268868Z "
	logLines := ConvertLogs([]byte(line))

	assert.Len(t, logLines, 1)
	assert.Equal(t, "2022-02-08T11:32:21.183268868Z", logLines[0].Timestamp)
	assert.Equal(t, "", logLines[0].Line)
}

func TestConvertLogs_MultipleLogLines(t *testing.T) {
	lines := []string{
		"2022-02-08T11:32:21.183268868Z these are",
		"2022-02-08T11:32:22.183268868Z some Logs",
		"hello",
		"2022-02-08T11:32:24.183268868Z done",
	}
	rawLog := strings.Join(lines, "\n")

	expected := [][]string{
		{"2022-02-08T11:32:21.183268868Z", "these are"},
		{"2022-02-08T11:32:22.183268868Z", "some Logs"},
		{"", "hello"},
		{"2022-02-08T11:32:24.183268868Z", "done"},
	}

	logLines := ConvertLogs([]byte(rawLog))

	for i := 0; i < len(lines); i++ {
		assert.Equal(t, expected[i][0], logLines[i].Timestamp)
		assert.Equal(t, expected[i][1], logLines[i].Line)
	}
}

func TestConvertLogs_IgnoreEmptyLines(t *testing.T) {
	lines := []string{}
	rawLog := "2022-02-08T11:32:21.183268868Z these are\n" +
		"2022-02-08T11:32:22.183268868Z some Logs\n" +
		"\n" +
		"2022-02-08T11:32:24.183268868Z done\n" +
		"\n"

	expected := [][]string{
		{"2022-02-08T11:32:21.183268868Z", "these are"},
		{"2022-02-08T11:32:22.183268868Z", "some Logs"},
		{"2022-02-08T11:32:24.183268868Z", "done"},
	}

	logLines := ConvertLogs([]byte(rawLog))

	assert.Len(t, logLines, len(expected))
	for i := 0; i < len(lines); i++ {
		assert.Equal(t, expected[i][0], logLines[i].Timestamp)
		assert.Equal(t, expected[i][1], logLines[i].Line)
	}
}

func TestConvertLogs_LargerThanMaxBytesTruncatesLogs(t *testing.T) {
	line := strings.Repeat("x", 999) + "\n"
	nLines := MaxLogBytes / len(line)

	log := strings.Repeat(line, nLines+53)
	log = log[:len(log)-1] // Exclude last newline

	logLines := ConvertLogs([]byte(log))

	assert.Len(t, logLines, nLines, fmt.Sprintf("should be %d, is %d", nLines, len(logLines)))
}
