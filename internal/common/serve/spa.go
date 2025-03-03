package serve

import (
	"errors"
	"io/fs"
	"net/http"
	"strings"
)

const indexHTMLPage = "index.html"

// SinglePageApplicationHandler handles requests for a single-page application front end. It prevents caching of
// index.html responses and defers handling of file paths which cannot be found to the front end by serving index.html
// in such cases.
func SinglePageApplicationHandler(fsys http.FileSystem) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if shouldServeIndexHTML(fsys, r.URL.Path) {
			r.URL.Path = "/"
			// Prevent caching when serving index.html. Its content determines the version of the JS/CSS
			// bundle, and we want to prevent the user from accessing a stale bundle.
			w.Header().Set("Cache-Control", "no-store, must-revalidate")
		}

		http.FileServer(fsys).ServeHTTP(w, r)
	})
}

func shouldServeIndexHTML(fsys http.FileSystem, urlPath string) bool {
	trimmedURLPath := strings.TrimPrefix(urlPath, "/")

	// Serve index.html when the file cannot be found - client-side routing in the SPA handles such cases.
	if _, err := fsys.Open(trimmedURLPath); errors.Is(err, fs.ErrNotExist) {
		return true
	}

	if trimmedURLPath == "" || trimmedURLPath == indexHTMLPage {
		return true
	}

	return false
}
