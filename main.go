package main

import (
	"crypto/sha256"
	"crypto/subtle"
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
	
	// Valid passwords (hashed)
	validPasswords = []string{
		"fridolin2026",
		"lutz2026",
	}
)

// basicAuth wraps a handler with HTTP Basic Authentication
func basicAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, password, ok := r.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="Holzeinschlag Austria"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		
		// Check password against valid passwords
		valid := false
		for _, validPwd := range validPasswords {
			// Use constant-time comparison to prevent timing attacks
			pwdHash := sha256.Sum256([]byte(password))
			validHash := sha256.Sum256([]byte(validPwd))
			if subtle.ConstantTimeCompare(pwdHash[:], validHash[:]) == 1 {
				valid = true
				break
			}
		}
		
		if !valid {
			w.Header().Set("WWW-Authenticate", `Basic realm="Holzeinschlag Austria"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		
		next.ServeHTTP(w, r)
	})
}

func main() {
	// Serve static files
	publicDir := filepath.Join(".", "public")
	dataDir := filepath.Join(".", "data")
	processingDir := filepath.Join(".", "processing")

	// Create a new mux for protected routes
	mux := http.NewServeMux()

	// File server for public assets
	mux.Handle("/", http.FileServer(http.Dir(publicDir)))
	mux.Handle("/data/", http.StripPrefix("/data/", http.FileServer(http.Dir(dataDir))))

	// API endpoints
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
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

	mux.HandleFunc("/api/start-pipeline", func(w http.ResponseWriter, r *http.Request) {
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

	mux.HandleFunc("/api/pipeline-log", func(w http.ResponseWriter, r *http.Request) {
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

	// Wrap entire mux with basic auth
	protectedHandler := basicAuth(mux)

	log.Println("Starting server on :8000 (password protected)")
	log.Println("View at http://localhost:8000")

	if err := http.ListenAndServe(":8000", protectedHandler); err != nil {
		log.Fatal(err)
	}
}
