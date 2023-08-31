package eventutil

// import (
// 	"github.com/armadaproject/armada/internal/common/context"
// 	"testing"
// 	"time"

// 	"github.com/armadaproject/armada/internal/pulsarutils"
// 	"github.com/armadaproject/armada/pkg/armadaevents"
// 	"github.com/apache/pulsar-client-go/pulsar"
// )

// func TestSequenceCompacter(t *testing.T) {

// }

// func TestEventFilter(t *testing.T) {
// 	tests := map[string]struct {
// 		filter func(*armadaevents.EventSequence_Event) bool
// 		n      int // Number of event expected to pass the filter
// 	}{
// 		"filter all": {
// 			filter: func(a *armadaevents.EventSequence_Event) bool {
// 				return false
// 			},
// 			n: 0,
// 		},
// 		"filter none": {
// 			filter: func(a *armadaevents.EventSequence_Event) bool {
// 				return true
// 			},
// 			n: 1,
// 		},
// 	}
// 	for name, tc := range tests {
// 		t.Run(name, func(t *testing.T) {
// 			C := make(chan *EventSequenceWithMessageIds, 1)
// 			eventFilter := NewEventFilter(C, tc.filter)
// 			ctx, _ := context.WithTimeout(context.Background(), time.Second)
// 			sequence := &EventSequenceWithMessageIds{
// 				Sequence: &armadaevents.EventSequence{
// 					Events: []*armadaevents.EventSequence_Event{
// 						{Event: nil},
// 						{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
// 					},
// 				},
// 				MessageIds: []pulsar.MessageID{pulsarutils.New(0, i, 0, 0)},
// 			}
// 			C <- sequence

// 		})
// 	}
// }

// func generateEvents(ctx *context.ArmadaContext, out chan *EventSequenceWithMessageIds) error {
// 	var i int64
// 	for {
// 		sequence := EventSequenceWithMessageIds{
// 			Sequence: &armadaevents.EventSequence{
// 				Events: []*armadaevents.EventSequence_Event{
// 					{Event: nil},
// 					{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
// 				},
// 			},
// 			MessageIds: []pulsar.MessageID{pulsarutils.New(0, i, 0, 0)},
// 		}
// 		select {
// 		case <-ctx.Done():
// 			return ctx.Err()
// 		case out <- &sequence:
// 		}
// 	}
// }
