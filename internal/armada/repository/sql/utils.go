package sql

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
	insertSql := createInsertQuery(table, append([]string{key}, fields...), values)
	insertSql += " ON CONFLICT (" + key + ") DO UPDATE SET "

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

func readStrings(rows *sql.Rows) ([]string, error) {
	renewedIds := []string{}
	for rows.Next() {
		var id string
		err := rows.Scan(&id)
		if err != nil {
			return nil, err
		}
		renewedIds = append(renewedIds, id)
	}
	return renewedIds, nil
}

func readByteRows(rows *sql.Rows, processValue func([]byte) error) error {
	for rows.Next() {
		var value interface{}
		err := rows.Scan(&value)
		if err != nil {
			return err
		}
		b, ok := value.([]byte)
		if !ok {
			return fmt.Errorf("could not read job from database")
		}
		err = processValue(b)
		if err != nil {
			return err
		}
	}
	return nil
}
