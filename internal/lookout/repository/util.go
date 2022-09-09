package repository

import (
	"database/sql"
	"strings"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"

	"github.com/G-Research/armada/internal/common/util"
)

func ToUTC(t time.Time) time.Time {
	location, _ := time.LoadLocation("UTC")
	return t.In(location)
}

func upsert(db *goqu.TxDatabase, table interface{}, keys []string, records []goqu.Record) error {
	if len(records) == 0 {
		return nil
	}

	// TODO: should check for the union of fields among all records
	aRecord := records[0]
	onConflictDefaults := goqu.Record{}
	for field := range aRecord {
		if !util.ContainsString(keys, field) {
			onConflictDefaults[field] = goqu.L("EXCLUDED." + field)
		}
	}

	ds := db.Insert(table).
		Rows(recordsToInterfaces(records)...).
		OnConflict(goqu.DoUpdate(strings.Join(keys, ", "), onConflictDefaults))

	_, err := ds.Prepared(true).Executor().Exec()
	return err
}

func recordsToInterfaces(records []goqu.Record) []interface{} {
	out := make([]interface{}, len(records))
	for i, record := range records {
		out[i] = record
	}
	return out
}

func StartsWith(field exp.IdentifierExpression, pattern string) goqu.Expression {
	return field.Like(pattern + "%")
}

func NewNullString(s string) sql.NullString {
	if len(s) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{
		String: s,
		Valid:  true,
	}
}

func ParseNullString(nullString sql.NullString) string {
	if !nullString.Valid {
		return ""
	}
	return nullString.String
}

func ParseNullBool(nullBool sql.NullBool) bool {
	if !nullBool.Valid {
		return false
	}
	return nullBool.Bool
}

func ParseNullInt(nullInt sql.NullInt64) int64 {
	if !nullInt.Valid {
		return 0
	}
	return nullInt.Int64
}

func ParseNullFloat(nullFloat sql.NullFloat64) float64 {
	if !nullFloat.Valid {
		return 0
	}
	return nullFloat.Float64
}

func ParseNullTime(nullTime sql.NullTime) *time.Time {
	if !nullTime.Valid {
		return nil
	}
	return &nullTime.Time
}

func ParseNullTimeDefault(nullTime sql.NullTime) time.Time {
	if !nullTime.Valid {
		return time.Time{}
	}
	return nullTime.Time
}
