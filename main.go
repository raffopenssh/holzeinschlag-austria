package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

var (
	pipelineRunning bool
	pipelineMutex   sync.Mutex
)

func main() {
	// Serve static files
	publicDir := filepath.Join(".", "public")
	dataDir := filepath.Join(".", "data")
	processingDir := filepath.Join(".", "processing")

	// File server for public assets
	http.Handle("/", http.FileServer(http.Dir(publicDir)))
	http.Handle("/data/", http.StripPrefix("/data/", http.FileServer(http.Dir(dataDir))))

	// API endpoints
	http.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		
		statusFile := filepath.Join(processingDir, "status.json")
		data, err := os.ReadFile(statusFile)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "not_started",
				"message": "Processing pipeline has not been run yet",
			})
			return
		}
		w.Write(data)
	})

	http.HandleFunc("/api/start-pipeline", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		pipelineMutex.Lock()
		if pipelineRunning {
			pipelineMutex.Unlock()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "already_running",
				"message": "Pipeline is already running",
			})
			return
		}
		pipelineRunning = true
		pipelineMutex.Unlock()

		// Start pipeline in background
		go func() {
			defer func() {
				pipelineMutex.Lock()
				pipelineRunning = false
				pipelineMutex.Unlock()
			}()

			script := filepath.Join(processingDir, "run_pipeline.sh")
			logFile := filepath.Join(processingDir, "pipeline.log")
			
			log.Println("Starting processing pipeline...")
			
			f, err := os.Create(logFile)
			if err != nil {
				log.Printf("Failed to create log file: %v", err)
				return
			}
			defer f.Close()

			cmd := exec.Command("/bin/bash", script)
			cmd.Stdout = f
			cmd.Stderr = f
			cmd.Dir = processingDir

			if err := cmd.Run(); err != nil {
				log.Printf("Pipeline failed: %v", err)
			} else {
				log.Println("Pipeline completed successfully")
			}
		}()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "started",
			"message": "Processing pipeline started",
		})
	})

	http.HandleFunc("/api/pipeline-log", func(w http.ResponseWriter, r *http.Request) {
		logFile := filepath.Join(processingDir, "pipeline.log")
		data, err := os.ReadFile(logFile)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{
				"log": "No log file found",
			})
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Write(data)
	})

	log.Println("Starting server on :8000")
	log.Println("View at http://localhost:8000")

	if err := http.ListenAndServe(":8000", nil); err != nil {
		log.Fatal(err)
	}
}
