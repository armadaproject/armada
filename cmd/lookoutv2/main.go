package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
)

func homePage(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "HIT")
}

func handleRequests() {
	http.HandleFunc("/", homePage)
	log.Fatal(http.ListenAndServe(":10000", nil))
}

func main() {

	err := http.ListenAndServe(":10000")
}
