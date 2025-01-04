package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NullLogger is Logger that sends a log lines into the ether
var NullLogger = &Logger{
	undlerlying: zap.New(zapcore.NewNopCore()).Sugar(),
}
