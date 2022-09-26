package pulsartest

import (
	//"context"

	"os"

	"github.com/G-Research/armada/pkg/armadaevents"
	log "github.com/sirupsen/logrus"
)

// Submit a job, represented by a file, to the Pulsar server.
// If dry-run is true, the job file is validated but not submitted.
func (a *App) Submit(path string, dryRun bool) error {
	eventYaml, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	es := &armadaevents.EventSequence{}

	if err = UnmarshalEventSubmission(eventYaml, es); err != nil {
		return err
	}

	if dryRun {
		return nil
	}

	log.Infof("submitting event sequence: %+v\n", es)

	// synchronously send request with event sequence

	return nil
}
