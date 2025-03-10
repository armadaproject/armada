package queryapi

import (
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
)

func NilStringToString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func DbTimeToTimestamp(t pgtype.Timestamp) *timestamppb.Timestamp {
	if !t.Valid {
		return nil
	}
	return protoutil.ToTimestamp(t.Time.UTC())
}
