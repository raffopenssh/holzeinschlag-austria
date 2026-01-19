package main

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	pipelineRunning bool
	pipelineMutex   sync.Mutex

	// Valid passwords
	validPasswords = []string{
		"fridolin2026",
		"lutz2026",
	}

	// Session tokens (in-memory, cleared on restart)
	sessions     = make(map[string]time.Time)
	sessionMutex sync.RWMutex
)

const sessionDuration = 24 * time.Hour

func generateToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func checkPassword(password string) bool {
	for _, validPwd := range validPasswords {
		pwdHash := sha256.Sum256([]byte(password))
		validHash := sha256.Sum256([]byte(validPwd))
		if subtle.ConstantTimeCompare(pwdHash[:], validHash[:]) == 1 {
			return true
		}
	}
	return false
}

func isValidSession(r *http.Request) bool {
	cookie, err := r.Cookie("session")
	if err != nil {
		log.Printf("No session cookie found: %v", err)
		return false
	}

	sessionMutex.RLock()
	expiry, exists := sessions[cookie.Value]
	sessionMutex.RUnlock()

	log.Printf("Session check: token=%s..., exists=%v, valid=%v", cookie.Value[:8], exists, exists && time.Now().Before(expiry))
	return exists && time.Now().Before(expiry)
}

func createSession(w http.ResponseWriter, r *http.Request) {
	token := generateToken()

	sessionMutex.Lock()
	sessions[token] = time.Now().Add(sessionDuration)
	sessionMutex.Unlock()

	// Check if behind HTTPS proxy
	isSecure := r.Header.Get("X-Forwarded-Proto") == "https" || r.TLS != nil

	log.Printf("Creating session: token=%s, secure=%v, X-Forwarded-Proto=%s", token[:8]+"...", isSecure, r.Header.Get("X-Forwarded-Proto"))

	sameSite := http.SameSiteLaxMode
	if isSecure {
		sameSite = http.SameSiteNoneMode
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "session",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   isSecure,
		SameSite: sameSite,
		MaxAge:   int(sessionDuration.Seconds()),
	})
}

var loginPage = `<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login - Holzeinschlag Österreich</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #2d5a27 0%, #1e3d1a 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .login-box {
            background: white;
            padding: 2.5rem;
            border-radius: 12px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.3);
            width: 100%;
            max-width: 360px;
        }
        h1 {
            color: #2d5a27;
            font-size: 1.4rem;
            margin-bottom: 0.5rem;
            text-align: center;
        }
        .subtitle {
            color: #7f8c8d;
            font-size: 0.85rem;
            text-align: center;
            margin-bottom: 1.5rem;
        }
        .form-group {
            margin-bottom: 1rem;
        }
        label {
            display: block;
            color: #2c3e50;
            font-size: 0.85rem;
            margin-bottom: 0.5rem;
        }
        input[type="password"] {
            width: 100%;
            padding: 0.75rem 1rem;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            font-size: 1rem;
            transition: border-color 0.2s;
        }
        input[type="password"]:focus {
            outline: none;
            border-color: #2d5a27;
        }
        button {
            width: 100%;
            padding: 0.875rem;
            background: #2d5a27;
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            transition: background 0.2s;
        }
        button:hover {
            background: #1e3d1a;
        }
        .error {
            color: #c0392b;
            font-size: 0.85rem;
            text-align: center;
            margin-top: 1rem;
            display: none;
        }
        .error.show { display: block; }
    </style>
</head>
<body>
    <div class="login-box">
        <h1>🌲 Holzeinschlag Österreich</h1>
        <p class="subtitle">Bitte Passwort eingeben</p>
        <form method="POST" action="/login">
            <div class="form-group">
                <label for="password">Passwort</label>
                <input type="password" id="password" name="password" required autofocus>
            </div>
            <button type="submit">Anmelden</button>
        </form>
        <p class="error {{if .Error}}show{{end}}">Falsches Passwort</p>
    </div>
</body>
</html>`

func main() {
	publicDir := filepath.Join(".", "public")
	dataDir := filepath.Join(".", "data")
	processingDir := filepath.Join(".", "processing")

	// Login page
	http.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.Header().Set("Content-Type", "text/html")
			w.Write([]byte(loginPage))
			return
		}

		if r.Method == "POST" {
			password := r.FormValue("password")
			if checkPassword(password) {
				createSession(w, r)
				http.Redirect(w, r, "/", http.StatusSeeOther)
				return
			}
			// Wrong password - show error
			w.Header().Set("Content-Type", "text/html")
			errorPage := `<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login - Holzeinschlag Österreich</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #2d5a27 0%, #1e3d1a 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .login-box {
            background: white;
            padding: 2.5rem;
            border-radius: 12px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.3);
            width: 100%;
            max-width: 360px;
        }
        h1 {
            color: #2d5a27;
            font-size: 1.4rem;
            margin-bottom: 0.5rem;
            text-align: center;
        }
        .subtitle {
            color: #7f8c8d;
            font-size: 0.85rem;
            text-align: center;
            margin-bottom: 1.5rem;
        }
        .form-group {
            margin-bottom: 1rem;
        }
        label {
            display: block;
            color: #2c3e50;
            font-size: 0.85rem;
            margin-bottom: 0.5rem;
        }
        input[type="password"] {
            width: 100%;
            padding: 0.75rem 1rem;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            font-size: 1rem;
            transition: border-color 0.2s;
        }
        input[type="password"]:focus {
            outline: none;
            border-color: #2d5a27;
        }
        button {
            width: 100%;
            padding: 0.875rem;
            background: #2d5a27;
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            transition: background 0.2s;
        }
        button:hover {
            background: #1e3d1a;
        }
        .error {
            color: #c0392b;
            font-size: 0.85rem;
            text-align: center;
            margin-top: 1rem;
        }
    </style>
</head>
<body>
    <div class="login-box">
        <h1>🌲 Holzeinschlag Österreich</h1>
        <p class="subtitle">Bitte Passwort eingeben</p>
        <form method="POST" action="/login">
            <div class="form-group">
                <label for="password">Passwort</label>
                <input type="password" id="password" name="password" required autofocus>
            </div>
            <button type="submit">Anmelden</button>
        </form>
        <p class="error">Falsches Passwort</p>
    </div>
</body>
</html>`
			w.Write([]byte(errorPage))
			return
		}
	})

	// Auth middleware for all other routes
	// Auth middleware disabled - public access
	authMiddleware := func(next http.Handler) http.Handler {
		return next
	}

	// Public files (SEO, social sharing)
	http.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(publicDir, "robots.txt"))
	})
	http.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(publicDir, "sitemap.xml"))
	})
	http.HandleFunc("/og-image.png", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(publicDir, "og-image.png"))
	})

	// Protected file servers
	http.Handle("/", authMiddleware(http.FileServer(http.Dir(publicDir))))
	http.Handle("/data/", authMiddleware(http.StripPrefix("/data/", http.FileServer(http.Dir(dataDir)))))

	// Protected API endpoints
	http.Handle("/api/status", authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		statusFile := filepath.Join(processingDir, "status.json")
		data, err := os.ReadFile(statusFile)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "not_started",
				"message": "Processing pipeline has not been run yet",
			})
			return
		}
		w.Write(data)
	})))

	http.Handle("/api/start-pipeline", authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		pipelineMutex.Lock()
		if pipelineRunning {
			pipelineMutex.Unlock()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "already_running",
				"message": "Pipeline is already running",
			})
			return
		}
		pipelineRunning = true
		pipelineMutex.Unlock()

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
			"status":  "started",
			"message": "Processing pipeline started",
		})
	})))

	http.Handle("/api/pipeline-log", authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	})))

	// Dynamic GPKG export with filtering
	http.Handle("/api/export", authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		yearsParam := r.URL.Query().Get("years")
		gemeindenParam := r.URL.Query().Get("gemeinden")

		// Build ogr2ogr command
		srcGpkg := filepath.Join(publicDir, "holzeinschlag_austria.gpkg")

		// Create temp output path (not file - ogr2ogr needs to create it)
		tmpPath := filepath.Join(os.TempDir(), fmt.Sprintf("export_%d.gpkg", time.Now().UnixNano()))
		defer os.Remove(tmpPath)

		// Build SQL for filtering
		var whereClause string
		if gemeindenParam != "" {
			isos := strings.Split(gemeindenParam, ",")
			quoted := make([]string, len(isos))
			for i, iso := range isos {
				quoted[i] = fmt.Sprintf("'%s'", strings.TrimSpace(iso))
			}
			whereClause = fmt.Sprintf("iso IN (%s)", strings.Join(quoted, ","))
		}

		// Build column selection based on years
		var selectCols string
		if yearsParam != "" {
			years := strings.Split(yearsParam, ",")
			cols := []string{"fid", "geom", "name", "iso", "state", "population"}
			for _, year := range years {
				y := strings.TrimSpace(year)
				cols = append(cols,
					fmt.Sprintf("loss_pixels_%s", y),
					fmt.Sprintf("loss_area_ha_%s", y),
					fmt.Sprintf("harvest_efm_%s", y),
					fmt.Sprintf("value_eur_%s", y),
					fmt.Sprintf("co2_tonnes_%s", y),
					fmt.Sprintf("ets_eur_%s", y),
					fmt.Sprintf("ets_per_capita_%s", y),
				)
			}
			selectCols = strings.Join(cols, ", ")
		} else {
			selectCols = "*"
		}

		// Build SQL query
		sql := fmt.Sprintf("SELECT %s FROM gemeinden", selectCols)
		if whereClause != "" {
			sql += " WHERE " + whereClause
		}

		// Run ogr2ogr
		cmd := exec.Command("ogr2ogr",
			"-f", "GPKG",
			tmpPath,
			srcGpkg,
			"-sql", sql,
			"-nln", "gemeinden",
		)
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("ogr2ogr error: %v, output: %s", err, string(output))
			http.Error(w, "Failed to generate export", http.StatusInternalServerError)
			return
		}

		// Read and send file
		data, err := os.ReadFile(tmpPath)
		if err != nil {
			http.Error(w, "Failed to read export file", http.StatusInternalServerError)
			return
		}

		// Generate filename
		filename := "holzeinschlag_austria"
		if gemeindenParam != "" {
			filename += "_selection"
		}
		if yearsParam != "" {
			filename += "_" + strings.ReplaceAll(yearsParam, ",", "-")
		}
		filename += ".gpkg"

		w.Header().Set("Content-Type", "application/geopackage+sqlite3")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.Write(data)
	})))

	log.Println("Starting server on :8000 (public access)")
	log.Println("View at http://localhost:8000")

	if err := http.ListenAndServe(":8000", nil); err != nil {
		log.Fatal(err)
	}
}
