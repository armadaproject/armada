package repository

import (
	"database/sql"
	"fmt"
	"strings"
)

func insert(db *sql.DB, table string, fields []string, values []interface{}) (sql.Result, error) {
	insertSql := createInsertQuery(table, fields, values)

	return db.Exec(insertSql, values...)
}

func upsert(db *sql.DB, table string, key string, fields []string, values []interface{}) (sql.Result, error) {
	return upsertCombinedKey(db, table, []string{key}, fields, values)
}

func upsertCombinedKey(db *sql.DB, table string, key []string, fields []string, values []interface{}) (sql.Result, error) {
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
