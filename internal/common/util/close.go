package util

import (
	"io"

	log "github.com/armadaproject/armada/internal/common/logging"
)

func CloseResource(name string, c io.Closer) {
	if err := c.Close(); err != nil {
		log.WithError(err).Warnf("Failed to close %s cleanly", name)
	}
}
