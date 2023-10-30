package logging

import (
	"io"

	"github.com/sirupsen/logrus"
)

var NullLogger = &logrus.Logger{
	Out:       io.Discard,
	Formatter: new(logrus.TextFormatter),
	Hooks:     make(logrus.LevelHooks),
	Level:     logrus.PanicLevel,
}
