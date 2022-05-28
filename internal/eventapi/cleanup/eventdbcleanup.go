package cleanup

import (
	ctx "context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/eventapi/configuration"
	"github.com/G-Research/armada/internal/eventapi/eventdb"
	"github.com/G-Research/armada/internal/lookout/postgres"
)

func Run(config *configuration.EventIngesterConfiguration) error {
	db, err := postgres.OpenPgxPool(config.Postgres)
	defer db.Close()
	if err != nil {
		return errors.WithStack(err)
	}
	eventsDb := eventdb.NewEventDb(db)

	cutOffTime := time.Now().Add(-config.InactiveJobsetExpiration).In(time.UTC)
	log.Infof("Detecting all jobsets that haven't been updated since %s", cutOffTime)

	// Get a candidate list of jobsets to delete
	jobsetsToDelete, err := eventsDb.LoadSeqNosAfter(ctx.Background(), cutOffTime)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Infof("Found %d jobsets to delete", len(jobsetsToDelete))
	numFailed := 0
	for _, js := range jobsetsToDelete {
		log.Infof("Deleting Jobset %d", js.JobSetId)
		err := eventsDb.DeleteJobsetInfo(ctx.Background(), js.JobSetId, js.SeqNo)
		if err != nil {
			numFailed++
			log.Errorf("Error cleaning up jobset %d: %+v", js.JobSetId, err)
		}
	}
	if numFailed > 0 {
		return errors.WithStack(fmt.Errorf("%d of %d jobsets could not be deleted", numFailed, len(jobsetsToDelete)))
	}
	return nil
}
