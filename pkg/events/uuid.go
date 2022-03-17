package events

import (
	"encoding/binary"
	"strings"

	"github.com/google/uuid"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

// UuidFromProtoUuid creates and returns a uuid.UUID from an events.Uuid
// (i.e., the efficient representation used in proto messages).
func UuidFromProtoUuid(id *Uuid) uuid.UUID {
	var rv uuid.UUID
	binary.BigEndian.PutUint64(rv[:8], id.High64)
	binary.BigEndian.PutUint64(rv[8:], id.Low64)
	return rv
}

// ProtoUuidFromUuid returns an efficient representation of a UUID meant for embedding in proto messages
// from a uuid.UUID.
func ProtoUuidFromUuid(id uuid.UUID) *Uuid {
	return &Uuid{
		High64: binary.BigEndian.Uint64(id[:8]),
		Low64:  binary.BigEndian.Uint64(id[8:]),
	}
}

// UlidFromProtoUuid creates and returns a ulid.ULID from an events.Uuid
// (i.e., the efficient representation used in proto messages).
func UlidFromProtoUuid(protoUuid *Uuid) ulid.ULID {
	var rv ulid.ULID
	binary.BigEndian.PutUint64(rv[:8], protoUuid.High64)
	binary.BigEndian.PutUint64(rv[8:], protoUuid.Low64)
	return rv
}

// ProtoUuidFromUlid returns an efficient representation of a UUID meant for embedding in proto messages
// from a uuid.UUID.
func ProtoUuidFromUlid(id ulid.ULID) *Uuid {
	return &Uuid{
		High64: binary.BigEndian.Uint64(id[:8]),
		Low64:  binary.BigEndian.Uint64(id[8:]),
	}
}

// StringFromProtoUuid returns a string representation of a proto UUID.
// Because Kubernetes requires ids to be valid DNS subdomain names, the string is returned in lower-case; see
// https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-subdomain-names
func StringFromProtoUuid(id *Uuid) string {
	return strings.ToLower(UlidFromProtoUuid(id).String())
}

// ProtoUuidFromUlidString parses a string into a proto UUID and returns it.
func ProtoUuidFromUlidString(ulidString string) (*Uuid, error) {
	id, err := ulid.Parse(ulidString)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}
	return ProtoUuidFromUlid(id), nil
}
