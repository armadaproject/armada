package repository

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/lib/pq"
)

func insert(db *goqu.Database, table string, fields []string, values []interface{}) (sql.Result, error) {
	insertSql := createInsertQuery(table, fields, values)

	return db.Exec(insertSql, values...)
}

func upsert(db *goqu.Database, table string, key string, fields []string, values []interface{}) (sql.Result, error) {
	return upsertCombinedKey(db, table, []string{key}, fields, values)
}

func upsertCombinedKey(db *goqu.Database, table string, key []string, fields []string, values []interface{}) (sql.Result, error) {
	insertSql := createInsertQuery(table, append(key, fields...), values)
	insertSql += " ON CONFLICT (" + strings.Join(key, ",") + ") DO UPDATE SET "

	for i, f := range fields {
		if i != 0 {
			insertSql += ","
		}
		insertSql += f + " = EXCLUDED." + f
	}

	return db.Exec(insertSql, values...)
}

func createInsertQuery(table string, fields []string, values []interface{}) string {
	insertSql := "INSERT INTO " + table + "(" + strings.Join(fields, ",") + ") VALUES "
	for i := 0; i < len(values); i += len(fields) {
		if i != 0 {
			insertSql += ","
		}
		insertSql += "("
		for k, _ := range fields {
			if k != 0 {
				insertSql += ","
			}
			insertSql += fmt.Sprintf("$%d", i+k+1)
		}
		insertSql += ")"
	}
	return insertSql
}

func BOOL_OR(col interface{}) exp.SQLFunctionExpression {
	return goqu.Func("BOOL_OR", col)
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

func ParseNullFloat(nullFloat sql.NullFloat64) float64 {
	if !nullFloat.Valid {
		return 0
	}
	return nullFloat.Float64
}

func ParseNullTime(nullTime pq.NullTime) *time.Time {
	if !nullTime.Valid {
		return nil
	}
	return &nullTime.Time
}

func ParseNullTimeDefault(nullTime pq.NullTime) time.Time {
	if !nullTime.Valid {
		return time.Time{}
	}
	return nullTime.Time
}
