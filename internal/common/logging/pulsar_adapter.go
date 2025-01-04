package logging

import (
	pulsarlog "github.com/apache/pulsar-client-go/pulsar/log"
)

// Wrapper to adapt Logger to the logger interface expected by the pulsar client
type pulsarWrapper struct {
	l *Logger
}

// NewPulsarLogger returns a Logger that can be used by the pulsar client
func NewPulsarLogger() pulsarlog.Logger {
	return &pulsarWrapper{
		l: StdLogger(),
	}
}

func (p pulsarWrapper) SubLogger(fs pulsarlog.Fields) pulsarlog.Logger {
	return &pulsarWrapper{
		l: p.l.WithFields(fs),
	}
}

func (p pulsarWrapper) WithFields(fs pulsarlog.Fields) pulsarlog.Entry {
	return &pulsarWrapper{
		l: p.l.WithFields(fs),
	}
}

func (p pulsarWrapper) WithField(name string, value interface{}) pulsarlog.Entry {
	return &pulsarWrapper{
		l: p.l.WithField(name, value),
	}
}

func (p pulsarWrapper) WithError(err error) pulsarlog.Entry {
	return &pulsarWrapper{
		l: p.l.WithError(err),
	}
}

func (p pulsarWrapper) Debug(args ...any) {
	p.l.Debug(args...)
}

func (p pulsarWrapper) Info(args ...any) {
	p.l.Info(args...)
}

func (p pulsarWrapper) Warn(args ...any) {
	p.l.Warn(args...)
}

func (p pulsarWrapper) Error(args ...any) {
	p.l.Error(args...)
}

func (p pulsarWrapper) Debugf(format string, args ...any) {
	p.l.Debugf(format, args)
}

func (p pulsarWrapper) Infof(format string, args ...any) {
	p.l.Infof(format, args)
}

func (p pulsarWrapper) Warnf(format string, args ...any) {
	p.l.Warnf(format, args)
}

func (p pulsarWrapper) Errorf(format string, args ...any) {
	p.l.Errorf(format, args)
}
