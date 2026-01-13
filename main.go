package main

import (
	"log"
	"net/http"
	"path/filepath"
)

func main() {
	// Serve static files
	publicDir := filepath.Join(".", "public")
	dataDir := filepath.Join(".", "data")
	
	// File server for public assets
	http.Handle("/", http.FileServer(http.Dir(publicDir)))
	http.Handle("/data/", http.StripPrefix("/data/", http.FileServer(http.Dir(dataDir))))
	
	log.Println("Starting server on :8000")
	log.Println("View at http://localhost:8000")
	
	if err := http.ListenAndServe(":8000", nil); err != nil {
		log.Fatal(err)
	}
}
