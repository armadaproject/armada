package serve

import "net/http"

type dirWithIndexFallback struct {
	dir http.Dir
}

// CreateDirWithIndexFallback creates a http.FileSystem for "path" as the
// dir field in dirWithIndexFallback  struct
func CreateDirWithIndexFallback(path string) http.FileSystem {
	return dirWithIndexFallback{http.Dir(path)}
}

// Open function attempts to open the file supplied as the name argument
// in the directory of the dir field  in the dirWithIndexFallback struct
// If the file does not exist it attempts to open the index.html file and returns
// any resulting error from its operation
func (d dirWithIndexFallback) Open(name string) (http.File, error) {
	file, err := d.dir.Open(name)
	if err != nil {
		return d.dir.Open("index.html")
	}
	return file, err
}
