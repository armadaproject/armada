package serve

import "net/http"

type dirWithIndexFallback struct {
	dir http.Dir
}

func CreateDirWithIndexFallback(path string) http.FileSystem {
	return dirWithIndexFallback{http.Dir(path)}
}

func (d dirWithIndexFallback) Open(name string) (http.File, error) {
	file, err := d.dir.Open(name)
	if err != nil {
		return d.dir.Open("index.html")
	}
	return file, err
}
