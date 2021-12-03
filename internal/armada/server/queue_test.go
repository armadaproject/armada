package server

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/pkg/api"
)

func TestGetQueueOwnership(t *testing.T) {
	properties := map[string]interface{}{
		"success": func(qname string, qeueu api.Queue, groups []string) bool {
			getOwnership := GetQueueOwnership(
				func(queueName string) (*api.Queue, error) {
					if queueName != qname {
						return nil, fmt.Errorf("invalid queue name")
					}
					return &qeueu, nil
				},
				func(c context.Context, o authorization.Owned) (bool, []string) {
					return true, groups
				},
			)

			ownerships, err := getOwnership(context.Background(), qname)
			if err != nil {
				t.Errorf("failed to retrieve queue ownership: %s", err)
				return false
			}
			return reflect.DeepEqual(ownerships, groups)
		},
		"error": func(qname string) bool {
			getOwnership := GetQueueOwnership(
				func(queueName string) (*api.Queue, error) {
					return nil, fmt.Errorf("")
				},
				func(c context.Context, o authorization.Owned) (bool, []string) {
					return true, nil
				},
			)

			if _, err := getOwnership(context.Background(), qname); err == nil {
				t.Errorf("failed to handle an error")
				return false
			}
			return true
		},
	}

	for name, property := range properties {
		t.Run(name, func(tb *testing.T) {
			if err := quick.Check(property, nil); err != nil {
				tb.Fatal(err)
			}
		})
	}
}
