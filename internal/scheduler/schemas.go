package scheduler

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/scheduler/sql"
)

func RunsSchema() string {
	return schemaByTable("runs")
}

func JobsSchema() string {
	return schemaByTable("jobs")
}

func PulsarSchema() string {
	return schemaByTable("pulsar")
}

func NodeInfoSchema() string {
	return schemaByTable("nodeinfo")
}

func JobErrorsSchema() string {
	return schemaByTable("job_errors")
}

func JobRunErrorsSchema() string {
	return schemaByTable("job_run_errors")
}

func JobRunAssignmentSchema() string {
	return schemaByTable("job_run_assignments")
}

func LeaderelectionSchema() string {
	return schemaByTable("leaderelection")
}

func schemaByTable(table string) string {
	s := sql.SchemaTemplate()
	schema, err := schemaFromString(s, table)
	if err != nil {
		err = errors.Wrapf(err, "failed to read schema for table %s", table)
		panic(err)
	}
	return schema
}

// schemaFromString searches for and returns the column name definitions for a given table.
//
// For example, if s is equal to the following string
// CREATE TABLE rectangle (
//
//	id UUID PRIMARY KEY,
//	width int NOT NULL,
//	height int NOT NULL
//
// );
//
// CREATE TABLE circle (
//
//	id UUID PRIMARY KEY,
//	radius int NOT NULL
//
// );
//
// Then schemaFromString(s, "circle"), returns
// (
//
//	id UUID PRIMARY KEY,
//	radius int NOT NULL
//
// )
func schemaFromString(s, tableName string) (string, error) {
	sl := strings.ToLower(s) // Lower-case to handle inconsistent case, e.g., CREATE TABLE and create table.

	i := strings.Index(sl, fmt.Sprintf("create table %s", tableName))
	if i == -1 {
		return "", errors.Errorf("could not find table %s", tableName)
	}
	sl = sl[i:]

	j := strings.Index(sl, "(")
	if j == -1 {
		return "", errors.Errorf("could not read schema for table %s: reached EOF when searching for (", tableName)
	}
	sl = sl[j:]

	k := strings.Index(sl, ");")
	if k == -1 {
		return "", errors.Errorf("could not read schema for table %s: reached EOF when searching for );", tableName)
	}
	k += len(");")
	return s[i+j : i+j+k-1], nil
}
