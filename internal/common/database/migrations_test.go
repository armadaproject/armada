package database

import (
	"testing"
	"strconv"
	"strings"
	"io/fs"
	"github.com/stretchr/testify/assert"
)

func TestReadMigrations(t *testing.T) {
	mockFiles := []string{
		"migration1.sql",
	}
	mockFS := createMockFileSystem(mockFiles)
	migrations, err := ReadMigrations(mockFS, "path")

	// Checks the suffix of files and handles file naming format
	for _, migration := range migrations {
		assert.True(t, strings.HasSuffix(migration.name, "migration.sql"), "Invalid file format for %v", migration.name)
	}
	
	// Checks slice format
	expectedMigrations := []Migration{
		{id: 0, name: "", sql: ""},
	}
	assert.Len(t, migrations, 1, "Unexpected slice length")

	// Slice first value should be int
	assert.IsType(t, expectedMigrations[0].id, migrations[0].id, "Should be of type int")
	// Slice second value should be string
	assert.IsType(t, expectedMigrations[0].name, migrations[0].name, "Should be of type string")
	// Slice third value should be string
	assert.IsType(t, expectedMigrations[0].sql, migrations[0].sql, "Should be of type string")

	// Checks ID length
	for _, migration := range migrations {
		assert.True(t, len(strconv.Itoa(migration.id)) <= 5, "Invalid ID length for %v", migration.name)
	}
	assert.NoError(t, err, "Unexpected error in ReadMigrations")
}

/*
func createMockFileSystem(files []string) fs.FS {
	return nil
} */