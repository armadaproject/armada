package armadaevents

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
)

func TestUuidConversion(t *testing.T) {
	expected := uuid.New()
	protoUuid := ProtoUuidFromUuid(expected)
	actual := UuidFromProtoUuid(protoUuid)
	assert.Equal(t, expected, actual)
}

func TestUlidConversion(t *testing.T) {
	entropy := ulid.Monotonic(rand.New(rand.NewSource(time.Unix(1000000, 0).UnixNano())), 0)
	expected := ulid.MustNew(ulid.Now(), entropy)
	protoUuid := ProtoUuidFromUlid(expected)
	actual := UuidFromProtoUuid(protoUuid)
	assert.Equal(t, expected[:], actual[:]) // Compare the byte representation only
}

func TestStringConversion(t *testing.T) {
	entropy := ulid.Monotonic(rand.New(rand.NewSource(time.Unix(1000000, 0).UnixNano())), 0)
	id := ulid.MustNew(ulid.Now(), entropy)
	expected := ProtoUuidFromUlid(id)
	s, err := UlidStringFromProtoUuid(expected)
	if ok := assert.NoError(t, err); !ok {
		t.FailNow()
	}
	actual, err := ProtoUuidFromUlidString(s)
	if ok := assert.NoError(t, err); !ok {
		t.FailNow()
	}
	assert.Equal(t, expected, actual)
}
