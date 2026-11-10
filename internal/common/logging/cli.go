package logging

import (
	"io"
	"os"

	"github.com/rs/zerolog"
)

// ConfigureCliLogging logs to stdout.
func ConfigureCliLogging() {
	ConfigureCliLoggingTo(os.Stdout)
}

// ConfigureCliLoggingTo is like ConfigureCliLogging but logs to out. Commands whose stdout carries
// machine-readable output (armadactl --output json/yaml) pass os.Stderr so logs don't corrupt it.
func ConfigureCliLoggingTo(out io.Writer) {
	zerolog.TimestampFieldName = ""
	zerolog.LevelFieldName = ""

	// Create a ConsoleWriter that writes to out.
	// Since we’ve cleared out the field names above, only the message will be printed.
	consoleWriter := zerolog.ConsoleWriter{
		Out:        out,
		TimeFormat: "", // No timestamp
	}

	// Create a logger that logs at InfoLevel and above.
	l := zerolog.New(consoleWriter).Level(zerolog.InfoLevel).With().Logger()
	ReplaceStdLogger(FromZerolog(l))
}
