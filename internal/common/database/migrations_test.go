package database

import (
	"testing"
	"strconv"
	"strings"
	"io/fs"
	"github.com/stretchr/testify/assert"
)

type testFS struct {
	files map[string]string
}

func (myFS *testFS) Open(name string) (fs.File, error) {
	content, ok := myFS.files[name]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return fs.FileString(content), nil
}

func TestReadMigrations(t *testing.T) {
    basePathScheduler := "internal/scheduler/database/migrations"
	basePathLookoutV2 := "internal/lookoutv2/schema/migrations"

	mockFiles := []string{
        "001_initial_schema.sql",
        "002_cancel_reason.sql",
        "003_run_leased_column.sql",
        "004_job_namespace.sql",
        "001_initialize_schema.up.sql",
        "002_add_runs_timestamps.up.sql",
        "003_add_runs_scheduled_at_priority.up.sql",
        "004_add_preempted_pending.up.sql",
        "005_add_runs_timestamps.up.sql",
        "006_add_run_pod_requirements.up.sql",
        "007_add_queue_jobset_index.sql",
    }

	mockFS := createMockFileSystem(mockFiles)
	migrations, err := ReadMigrations(mockFS, basePathScheduler, basePathLookoutV2)

	for _, migration := range migrations {
		assert.True(t, strings.HasSuffix(migration.name, ".sql"), "Invalid file format for %v", migration.name)
	}
    
	for i := 1; i < len(migrations); i++ {
		assert.LessOrEqual(t, migrations[i-1].name <= migrations[i].name, "Not sorted correctly")
	}

	for _, migration := range migrations {
		assert.IsType(t, 0, migration.id, "Should be of type int")
		assert.IsType(t, "", migration.name, "Should be of type string")
		assert.IsType(t, "", migration.sql, "Should be of type string")
	}
    
	assert.Equal(t, migrations[0].id, 1, "Incorrect ID for 001_initial_schema.sql")
	assert.Equal(t, migrations[1].id, 2, "Incorrect ID for 002_cancel_reason.sql")
	assert.Equal(t, migrations[2].id, 3, "Incorrect ID for 003_run_leased_column.sql")
    
	for _, migration := range migrations {
		assert.True(t, len(strconv.Itoa(migration.id)) <= 2, "Invalid ID length for %v", migration.name)
	}

	for _, migration := range migrations {
		assert.NotEmpty(t, migration.sql, "Content should not be empty for %v", migration.sql)
	}
}

func createMockFileSystem(files []string) fs.FS {
	mockFiles := make(map[string]string)
	for _, file := range files {
		mockFiles[file] = "content of " + file
	}
	return &testFS{files: mockFiles}
}