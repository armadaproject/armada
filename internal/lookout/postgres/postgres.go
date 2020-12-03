package postgres

import (
	"database/sql"
	"strings"
)

func Open(connectionString map[string]string) (*sql.DB, error) {
	return sql.Open("postgres", createConnectionString(connectionString))
}

func createConnectionString(values map[string]string) string {
	// https://www.postgresql.org/docs/10/libpq-connect.html#id-1.7.3.8.3.5
	result := ""
	replacer := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	for k, v := range values {
		result += k + "='" + replacer.Replace(v) + "'"
	}
	return result
}
