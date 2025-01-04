package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var NullLogger = &Logger{
	undlerlying: zap.New(zapcore.NewNopCore()).Sugar(),
}
