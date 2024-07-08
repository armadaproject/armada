package queryapi

import (
	"github.com/gogo/protobuf/types"
	"github.com/jackc/pgx/v5/pgtype"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
)

func NilStringToString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func DbTimeToTimestamp(t pgtype.Timestamp) *types.Timestamp {
	if !t.Valid {
		return nil
	}
	return protoutil.ToTimestamp(t.Time.UTC())
}
