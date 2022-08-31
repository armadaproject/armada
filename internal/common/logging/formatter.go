package logging

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

type CommandLineFormatter struct{}

func (f *CommandLineFormatter) Format(entry *log.Entry) ([]byte, error) {
	return []byte(fmt.Sprintf("%s\n", entry.Message)), nil
}
