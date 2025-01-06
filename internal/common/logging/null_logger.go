package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NullLogger is Logger that sends a log lines into the ether
var NullLogger = &Logger{
	underlying: zap.New(zapcore.NewNopCore()).Sugar(),
}
