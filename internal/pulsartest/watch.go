package pulsartest

import (
	"fmt"
	"log"
	"os"

	"github.com/armadaproject/armada/internal/common/context"

	"github.com/sanity-io/litter"

	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
)

// Watch for Pulsar events
func (a *App) Watch() error {
	defer a.Reader.Close()

	for a.Reader.HasNext() {
		msg, err := a.Reader.Next(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		ctx := context.Background()
		msgId := pulsarutils.New(msg.ID().LedgerID(), msg.ID().EntryID(),
			msg.ID().PartitionIdx(), msg.ID().BatchIdx())

		es, err := eventutil.UnmarshalEventSequence(ctx, msg.Payload())
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not unmarshal proto for msg %s\n", msgId.String())
		}

		fmt.Printf("Id: %s\nMessage: %s\n", msgId.String(), litter.Sdump(es))
	}
	return nil
}
