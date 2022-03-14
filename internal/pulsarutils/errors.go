package pulsarutils

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
)

// PulsarError returns the first pulsar.Error in the chain,
// or nil if no such error is found.
func PulsarError(err error) *pulsar.Error {
	var pulsarError *pulsar.Error
	if ok := errors.Is(err, pulsarError); ok {
		return pulsarError
	}
	return nil // no Pulsar errors in the chain
}

// IsPulsarError returns true if there is a pulsar.Error in the chain.
func IsPulsarError(err error) bool {
	var pulsarError *pulsar.Error
	return errors.Is(err, pulsarError)
}
