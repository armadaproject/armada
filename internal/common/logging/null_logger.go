package logging

import (
	"github.com/rs/zerolog"
)

// NullLogger is Logger that sends a log lines into the ether
var NullLogger = &Logger{
	underlying: zerolog.Nop(),
}
