package serve

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateDirWithIndexFallback(t *testing.T) {
	dir := CreateDirWithIndexFallback("/test")

	assert.Equal(t, dir, dirWithIndexFallback{dir: "/test"})
}

func TestDirWithIndexFallbackOpen(t *testing.T) {
	// Define test cases
	testCases := []struct {
		name         string
		fileNames    []string
		expectedFile string
		wantErr      bool
	}{
		{
			name:         "index.html exists and test.html exists",
			fileNames:    []string{"index.html", "test.html"},
			expectedFile: "test.html",
			wantErr:      false,
		},
		{
			name:         "index.html missing and test.html does not exist",
			fileNames:    []string{},
			expectedFile: "",
			wantErr:      true,
		},
		{
			name:         "test.html does not exist",
			fileNames:    []string{"index.html", "test2.html"},
			expectedFile: "index.html",
			wantErr:      false,
		},
	}

	// test cases are run to
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a temporary directory for testing
			tempDir := os.TempDir()

			// Create a new http.FileSystem using the function being tested
			httpTempDir := CreateDirWithIndexFallback(tempDir)

			defer os.RemoveAll(tempDir)

			// Create file to simulate absence or presence of file
			for _, fileName := range tc.fileNames {

				filePath, err := os.Create(tempDir + "/" + fileName)
				require.NoError(t, err)
				filePath.Close()

			}
			// All test cases are run against the existence of test.html
			file, err := httpTempDir.Open("test.html")
			if tc.wantErr {
				assert.Error(t, err)
			}
			if !tc.wantErr {
				assert.NoError(t, err)
				statFile, statErr := file.Stat()
				require.NoError(t, statErr)
				assert.Equal(t, statFile.Name(), tc.expectedFile)
			}
		})
	}
}
