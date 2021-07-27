package repository

import (
	"database/sql"
	"fmt"
)

type SqlHealth struct {
	db *sql.DB
}

func NewSqlHealth(db *sql.DB) *SqlHealth {
	return &SqlHealth{db: db}
}

func (r *SqlHealth) Check() error {
	var col string
	row := r.db.QueryRow("SELECT 1")
	err := row.Scan(&col)
	if err == nil {
		return nil
	} else {
		return fmt.Errorf("SQL health check failed: %v", err)
	}
}
