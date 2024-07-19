package validation

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateSubmitFile_WhenValidFile_ReturnValid(t *testing.T) {
	path := filepath.Join("testdata", "valid.yaml")

	ok, err := ValidateSubmitFile(path)

	assert.NoError(t, err)
	assert.True(t, ok)
}

func TestValidateSubmitFile_WhenEmptyFile_ReturnInvalid(t *testing.T) {
	path := filepath.Join("testdata", "empty.json")

	ok, err := ValidateSubmitFile(path)

	assert.Error(t, err)
	assert.False(t, ok)
}

func TestValidateSubmitFile_WhenFileFormatting_ReturnInvalid(t *testing.T) {
	path := filepath.Join("testdata", "invalid_format.yaml")

	ok, err := ValidateSubmitFile(path)

	assert.Error(t, err)
	assert.False(t, ok)
}

func TestValidateSubmitFile_WhenFileContainsNoJobs_ReturnInvalid(t *testing.T) {
	path := filepath.Join("testdata", "invalid_no-jobs.yaml")

	ok, err := ValidateSubmitFile(path)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no jobs to submit")
	assert.False(t, ok)
}
