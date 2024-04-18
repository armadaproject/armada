package pulsartest

import (
	"fmt"
	"log"
	"os"

	"github.com/sanity-io/litter"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/eventutil"
)

// Watch for Pulsar events
func (a *App) Watch() error {
	defer a.Reader.Close()

	for a.Reader.HasNext() {
		msg, err := a.Reader.Next(armadacontext.Background())
		if err != nil {
			log.Fatal(err)
		}

		ctx := armadacontext.Background()

		es, err := eventutil.UnmarshalEventSequence(ctx, msg.Payload())
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not unmarshal proto for msg %s\n", msg.ID())
		}

		fmt.Printf("Id: %s\nMessage: %s\n", msg.ID().String(), litter.Sdump(es))
	}
	return nil
}
