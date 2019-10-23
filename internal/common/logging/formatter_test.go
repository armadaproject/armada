package logging

import (
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestCommandLineFormatter(t *testing.T) {
	commandLineFormatter := new(CommandLineFormatter)

	testMessage := "Test"
	expectedOutput := testMessage + "\n"

	event := log.Entry{
		Message: testMessage,
	}

	output, err := commandLineFormatter.Format(&event)

	assert.Nil(t, err)
	assert.Equal(t, expectedOutput, string(output[:]))
}
