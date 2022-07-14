package eventscheduler

import (
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
)

var RunsSchema string
var PulsarSchema string

func init() {
	schema, err := schemaFromFile("./sql/schema/runs.sql", "runs")
	if err != nil {
		err = errors.Wrap(err, "failed to read runs schema")
		panic(err)
	}
	RunsSchema = schema

	schema, err = schemaFromFile("./sql/schema/pulsar.sql", "pulsar")
	if err != nil {
		err = errors.Wrap(err, "failed to read pulsar schema")
		panic(err)
	}
	PulsarSchema = schema
}

func schemaFromFile(filename, tableName string) (string, error) {
	dat, err := os.ReadFile(filename)
	if err != nil {
		return "", errors.Wrap(err, "failed to read runs SQL schema")
	}
	return schemaFromString(string(dat), tableName)
}

// schemaFromString searches for and returns the column name definitions for a given table.
//
// For example, if s is equal to the following string
// CREATE TABLE rectangle (
//   id UUID PRIMARY KEY,
//   width int NOT NULL,
//   height int NOT NULL
// );
//
// CREATE TABLE circle (
//   id UUID PRIMARY KEY,
//   radius int NOT NULL
// );
//
// Then schemaFromString(s, "circle"), returns
// (
//   id UUID PRIMARY KEY,
//   radius int NOT NULL
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

func (r Run) Schema() string {
	return RunsSchema
}

// func (x Run) Names() []string {
// 	t := reflect.TypeOf(x)
// 	names := make([]string, t.NumField())
// 	for i := 0; i < t.NumField(); i++ {
// 		names[i] = t.Field(i).Tag.Get("db")
// 	}
// 	return names
// }

// func (x Run) Values() []interface{} {
// 	v := reflect.ValueOf(x)
// 	values := make([]interface{}, v.NumField())
// 	for i := 0; i < v.NumField(); i++ {
// 		values[i] = v.Field(i).Interface()
// 	}
// 	return values
// }
