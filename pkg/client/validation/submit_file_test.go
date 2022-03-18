package validation

// TODO: Tests commented out because they pull in schemas from the Internet.
//import (
//	"path/filepath"
//	"testing"
//
//	"github.com/stretchr/testify/assert"
//)
//
//func TestValidateSubmitFile_WhenValidFile_ReturnValid(t *testing.T) {
//	path := filepath.Join("testdata", "valid.yaml")
//
//	ok, err := ValidateSubmitFile(path)
//
//	assert.NoError(t, err)
//	assert.True(t, ok)
//}
//
//func TestValidateSubmitFile_WhenEmptyFile_ReturnInvalid(t *testing.T) {
//	path := filepath.Join("testdata", "empty.json")
//
//	ok, err := ValidateSubmitFile(path)
//
//	assert.Error(t, err)
//	assert.False(t, ok)
//}
//
//func TestValidateSubmitFile_WhenFileFormatting_ReturnInvalid(t *testing.T) {
//	path := filepath.Join("testdata", "invalid_format.yaml")
//
//	ok, err := ValidateSubmitFile(path)
//
//	assert.Error(t, err)
//	assert.False(t, ok)
//}
//
//func TestValidateSubmitFile_WhenUnknownField_ReturnInvalid(t *testing.T) {
//	path := filepath.Join("testdata", "invalid_field.yaml")
//
//	ok, err := ValidateSubmitFile(path)
//
//	assert.Error(t, err)
//	assert.False(t, ok)
//}
//
//func TestValidateSubmitFile_WhenMultiJobUnknownField_ReturnInvalid(t *testing.T) {
//	path := filepath.Join("testdata", "invalid_field_multi_job.yaml")
//
//	ok, err := ValidateSubmitFile(path)
//
//	assert.Error(t, err)
//	assert.False(t, ok)
//}
