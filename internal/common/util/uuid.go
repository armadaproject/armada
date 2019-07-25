package util

import (
	"github.com/oklog/ulid"
	"math/rand"
	"strings"
	"sync"
	"time"
)

var entropy = ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
var m sync.Mutex

func NewULID() string {
	m.Lock()
	defer m.Unlock()
	return strings.ToLower(ulid.MustNew(ulid.Now(), entropy).String())
}
