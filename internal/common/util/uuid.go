package util

import (
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid"
)

var entropy = ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
var m sync.Mutex

func NewULID() string {
	m.Lock()
	defer m.Unlock()
	return strings.ToLower(ulid.MustNew(ulid.Now(), entropy).String())
}
