package common

import (
	"fmt"
	"strings"

	"github.com/G-Research/armada/pkg/api"
)

func ShortStringFromApiEvent(msg *api.EventMessage) string {
	s := stringFromApiEvent(msg)
	s = strings.ReplaceAll(s, "*api.EventMessage_", "")
	return s
}

func stringFromApiEvent(msg *api.EventMessage) string {
	return fmt.Sprintf("%T", msg.Events)
}
