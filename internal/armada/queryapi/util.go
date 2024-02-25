package queryapi

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func NilStringToString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func DbTimeToGoTime(t pgtype.Timestamp) *time.Time {
	if !t.Valid {
		return nil
	}
	tt := t.Time.UTC()
	return &tt
}
