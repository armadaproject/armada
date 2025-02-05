package logging

import (
	"os"

	"github.com/rs/zerolog"
)

func ConfigureCliLogging() {
	zerolog.TimestampFieldName = ""
	zerolog.LevelFieldName = ""

	// Create a ConsoleWriter that writes to stdout.
	// Since we’ve cleared out the field names above, only the message will be printed.
	consoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: "", // No timestamp
	}

	// Create a logger that logs at InfoLevel and above.
	l := zerolog.New(consoleWriter).Level(zerolog.InfoLevel).With().Logger()
	ReplaceStdLogger(FromZerolog(l))
}
