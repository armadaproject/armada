package logging

import (
	"io"
	"log/slog"
)

var NullLogger = slog.New(
	slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}),
)
