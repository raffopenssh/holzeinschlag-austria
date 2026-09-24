package main

// Sibling-service contract (siedler-oesterreich "AHEAD" list + cadastre integration spec):
//   GET  /llm/kg/{kg_code}             municipal forest timeline per Katastralgemeinde
//   GET  /llm/kgs?codes=a,b            batch (<=500); ?all=1 lists covered KG codes
//   GET  /llm/manifest.json            coverage/schema manifest
//   GET  /llm.txt                      discovery doc
//   GET  /data/prices/state/{1-9}.json slim regional LK prices (derived in-process from the catalog)
// All served from in-memory JSON loaded at startup (no per-request disk/network I/O),
// with ETag/304, gzip and CORS via serveBytes().

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const serviceSlug = "holzeinschlag-at"

var stateByDigit = map[string]string{
	"1": "Burgenland", "2": "Kärnten", "3": "Niederösterreich", "4": "Oberösterreich",
	"5": "Salzburg", "6": "Steiermark", "7": "Tirol", "8": "Vorarlberg", "9": "Wien",
}

// slim series wanted by the game per state (HOLZ-2)
var statePriceSeries = []string{"LK_BLFIM2b", "LK_BLLA3aplus", "LK_BLKI2aplus", "LK_BLBU3plus", "LK_ISFI_FMO", "LK_BHH", "LK_BHW"}

type kgEntry struct {
	G string `json:"g"`
	N string `json:"n"`
}

type llmStore struct {
	mu        sync.RWMutex
	loadedAt  time.Time
	kg        map[string]kgEntry
	kgSrcTime string
	names     map[string]string
	states    map[string]string
	forestHa  map[string]float64
	netFluxHa map[string]float64
	lossYear  map[string]map[string]float64 // iso -> year -> ha
	lossTotal map[string]float64
	yearRows  map[string]map[string][]float64 // year -> iso -> row
	spruceNat map[string]float64              // year -> EUR/Efm national
	spruceSt  map[string]map[string]float64   // state -> year -> EUR/Efm (LK_BLFIM2b)
	spruceLat map[string]float64              // state -> latest
	statePr   map[string][]byte               // digit -> rendered JSON
	manifest  []byte
}

var store = &llmStore{}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func fnum(v any) (float64, bool) {
	f, ok := v.(float64)
	return f, ok
}

func loadStore(dataDir string) error {
	s := &llmStore{loadedAt: time.Now().UTC()}

	var kgm struct {
		UpdatedAt string             `json:"updated_at"`
		KG        map[string]kgEntry `json:"kg"`
	}
	if err := readJSON(filepath.Join(dataDir, "kg_gemeinde.json"), &kgm); err != nil {
		return fmt.Errorf("kg_gemeinde.json: %w", err)
	}
	s.kg, s.kgSrcTime = kgm.KG, kgm.UpdatedAt

	var lk struct {
		Names  map[string]string `json:"names"`
		States map[string]string `json:"states"`
	}
	if err := readJSON(filepath.Join(dataDir, "gemeinde_lookup.json"), &lk); err != nil {
		return err
	}
	s.names, s.states = lk.Names, lk.States

	var cf struct {
		Gemeinden map[string]map[string]any `json:"gemeinden"`
	}
	if err := readJSON(filepath.Join(dataDir, "carbon_flux_by_gemeinde.json"), &cf); err != nil {
		return err
	}
	s.forestHa, s.netFluxHa = map[string]float64{}, map[string]float64{}
	for iso, g := range cf.Gemeinden {
		if f, ok := fnum(g["forest_area_ha"]); ok {
			s.forestHa[iso] = f
		}
		if f, ok := fnum(g["net_flux_per_ha"]); ok {
			s.netFluxHa[iso] = f
		}
	}

	var ly struct {
		Gemeinden map[string]struct {
			Total float64                       `json:"total_area_ha"`
			Years map[string]map[string]float64 `json:"years"`
		} `json:"gemeinden"`
	}
	if err := readJSON(filepath.Join(dataDir, "gemeinde_yearly_loss.json"), &ly); err != nil {
		return err
	}
	s.lossYear, s.lossTotal = map[string]map[string]float64{}, map[string]float64{}
	for iso, g := range ly.Gemeinden {
		m := map[string]float64{}
		for y, r := range g.Years {
			m[y] = r["area_ha"]
		}
		s.lossYear[iso], s.lossTotal[iso] = m, g.Total
	}

	s.yearRows = map[string]map[string][]float64{}
	for y := 2001; y <= 2024; y++ {
		ys := strconv.Itoa(y)
		var rows map[string][]float64
		if err := readJSON(filepath.Join(dataDir, "year_"+ys+".json"), &rows); err != nil {
			return err
		}
		s.yearRows[ys] = rows
	}

	var tp struct {
		Prices map[string]struct {
			Yearly map[string]float64 `json:"yearly_avg"`
		} `json:"prices"`
	}
	if err := readJSON(filepath.Join(dataDir, "timber_prices.json"), &tp); err == nil {
		s.spruceNat = tp.Prices["spruce_fir_2b"].Yearly
	}

	// catalog -> per-state slim prices
	var cat struct {
		UpdatedAt string `json:"updated_at"`
		Pages     map[string]struct {
			Title  string                    `json:"title"`
			Series map[string]map[string]any `json:"series"`
		} `json:"pages"`
		StatePages map[string]map[string]string `json:"state_pages"`
	}
	s.statePr, s.spruceSt, s.spruceLat = map[string][]byte{}, map[string]map[string]float64{}, map[string]float64{}
	if err := readJSON(filepath.Join(dataDir, "timber_price_catalog.json"), &cat); err != nil {
		log.Printf("llm: price catalog unavailable: %v", err)
	} else {
		for digit, stName := range stateByDigit {
			series := map[string]any{}
			for kind, cid := range cat.StatePages[stName] {
				pg, ok := cat.Pages[cid]
				if !ok {
					continue
				}
				for _, code := range statePriceSeries {
					sr, ok := pg.Series[code]
					if !ok {
						continue
					}
					ya := map[string]float64{}
					if m, ok := sr["yearly_avg"].(map[string]any); ok {
						for y, v := range m {
							if f, ok := fnum(v); ok {
								ya[y] = f
							}
						}
					}
					var lo, hi any
					if v, ok := pg.Series[code+"_von"]; ok {
						lo = v["latest_value"]
					}
					if v, ok := pg.Series[code+"_bis"]; ok {
						hi = v["latest_value"]
					}
					series[code] = map[string]any{
						"name": sr["name"], "unit": sr["unit"], "kind": kind,
						"latest": sr["latest_value"], "latest_date": sr["last"],
						"range_low": lo, "range_high": hi, "yearly_avg": ya,
					}
					if code == "LK_BLFIM2b" {
						s.spruceSt[stName] = ya
						if f, ok := fnum(sr["latest_value"]); ok {
							s.spruceLat[stName] = f
						}
					}
				}
			}
			if len(series) == 0 {
				continue // e.g. Wien: no LK forest price report
			}
			asOf := ""
			if sp, ok := series["LK_BLFIM2b"].(map[string]any); ok {
				asOf, _ = sp["latest_date"].(string)
			}
			b, _ := json.Marshal(map[string]any{
				"state": digit, "state_name": stName, "as_of": asOf, "updated_at": cat.UpdatedAt,
				"source":  "LK " + stName + " Holzmarktberichte via preise.agrarforschung.at",
				"license": "CC-BY-4.0 (derived); original: Landwirtschaftskammern Österreich",
				"unit_glossary": map[string]string{"EUR/FM": "EUR per Festmeter (solid m³) without bark, ab Straße",
					"EURO/RM": "EUR per Raummeter (stacked m³) fuelwood", "range_low/high": "lower/upper band of LK price report"},
				"series": series,
			})
			s.statePr[digit] = b
		}
	}

	kgCovered := 0
	for _, e := range s.kg {
		if _, ok := s.yearRows["2024"][e.G]; ok {
			kgCovered++
		}
	}
	s.manifest, _ = json.Marshal(map[string]any{
		"service": serviceSlug, "base_url": "https://holzeinschlag-at.exe.xyz",
		"dataset":            "Forest loss, timber harvest, harvest value, carbon flux & timber prices — municipal, 2001-2024",
		"finest_granularity": "gemeinde",
		"join_keys":          []string{"kg_code", "gemeinde_code"},
		"kg_endpoint":        "/llm/kg/{kg_code}",
		"batch_endpoint":     "/llm/kgs?codes=a,b (<=500)",
		"kg_count":           kgCovered, "kg_total_register": len(s.kg), "gemeinde_count": len(s.yearRows["2024"]),
		"covered_kgs_url": "/llm/kgs?all=1",
		"metrics_schema": map[string]string{
			"forest_area_ha": "number", "loss_ha": "number", "loss_ha_2024": "number", "loss_total_ha": "number",
			"harvest_efm": "number", "harvest_value_eur": "number", "co2_t": "number",
			"net_flux_tco2e_ha": "number|null", "price_spruce_eur_efm": "number|null",
		},
		"history":         "one row per year 2001-2024, same keys as metrics (null where series absent; prices start 2010)",
		"as_of":           "2024-12-31",
		"updated_at":      s.loadedAt.Format(time.RFC3339),
		"kg_map_source":   "cadastre-process-api.exe.xyz /api/v1/lookup (BEV EDM register), fetched " + s.kgSrcTime,
		"other_endpoints": []string{"/data/prices/state/{1-9}.json", "POST /api/plot-context[?fast=1]", "/api/llm.txt", "/data/*"},
		"license":         "CC-BY-4.0",
		"cache":           "ETag + gzip on /llm/* and /data/*; Cache-Control max-age=3600",
	})

	store.mu.Lock()
	store.loadedAt, store.kg, store.kgSrcTime, store.names, store.states = s.loadedAt, s.kg, s.kgSrcTime, s.names, s.states
	store.forestHa, store.netFluxHa, store.lossYear, store.lossTotal = s.forestHa, s.netFluxHa, s.lossYear, s.lossTotal
	store.yearRows, store.spruceNat, store.spruceSt, store.spruceLat = s.yearRows, s.spruceNat, s.spruceSt, s.spruceLat
	store.statePr, store.manifest = s.statePr, s.manifest
	store.mu.Unlock()
	log.Printf("llm: loaded %d KGs (%d covered), %d Gemeinden, %d state price files", len(s.kg), kgCovered, len(s.names), len(s.statePr))
	return nil
}

func (s *llmStore) kgDoc(code string) (map[string]any, bool) {
	e, ok := s.kg[code]
	if !ok {
		return nil, false
	}
	iso := e.G
	rows2024, ok := s.yearRows["2024"][iso]
	if !ok || len(rows2024) < 5 {
		return nil, false
	}
	state := s.states[iso]
	priceOf := func(y string) any {
		if m, ok := s.spruceSt[state]; ok {
			if v, ok := m[y]; ok {
				return v
			}
		}
		if v, ok := s.spruceNat[y]; ok {
			return v
		}
		return nil
	}
	var fha, nf, lossTot any
	if v, ok := s.forestHa[iso]; ok {
		fha = v
	}
	if v, ok := s.netFluxHa[iso]; ok {
		nf = v
	}
	if v, ok := s.lossTotal[iso]; ok {
		lossTot = v
	}
	history := make([]map[string]any, 0, 24)
	for y := 2001; y <= 2024; y++ {
		ys := strconv.Itoa(y)
		r := s.yearRows[ys][iso]
		row := map[string]any{"as_of": ys + "-12-31", "forest_area_ha": fha, "loss_total_ha": nil, "net_flux_tco2e_ha": nil,
			"loss_ha": nil, "harvest_efm": nil, "harvest_value_eur": nil, "co2_t": nil, "price_spruce_eur_efm": priceOf(ys)}
		if len(r) >= 5 {
			row["loss_ha"], row["harvest_efm"], row["harvest_value_eur"], row["co2_t"] = r[1], r[2], r[3], r[4]
		} else if v, ok := s.lossYear[iso][ys]; ok {
			row["loss_ha"] = v
		}
		if ys == "2024" {
			row["loss_total_ha"], row["net_flux_tco2e_ha"] = lossTot, nf
		}
		history = append(history, row)
	}
	var priceLatest any
	if v, ok := s.spruceLat[state]; ok {
		priceLatest = v
	} else {
		priceLatest = priceOf("2024")
	}
	metrics := map[string]any{
		"forest_area_ha": fha, "loss_ha": rows2024[1], "loss_ha_2024": rows2024[1], "loss_total_ha": lossTot,
		"harvest_efm": rows2024[2], "harvest_value_eur": rows2024[3], "co2_t": rows2024[4],
		"net_flux_tco2e_ha": nf, "price_spruce_eur_efm": priceLatest,
	}
	return map[string]any{
		"service": serviceSlug, "dataset": "Municipal forest loss, timber harvest, harvest value, CO2 & carbon flux 2001-2024",
		"kg_code": code, "kg_name": e.N, "gemeinde_code": iso, "gemeinde_name": s.names[iso], "state": state,
		"granularity": "gemeinde", "as_of": "2024-12-31", "updated_at": s.loadedAt.Format(time.RFC3339),
		"source":  "Hansen GFC-2024 v1.12 (loss); Statistik Austria Holzeinschlag (harvest, downscaled); Harris et al./GFW carbon flux; LK Holzmarktberichte via preise.agrarforschung.at (prices)",
		"license": "CC-BY-4.0",
		"unit_glossary": map[string]string{
			"forest_area_ha": "ha, Hansen treecover2000 >= 30%", "loss_ha": "ha stand-replacing loss in the year (harvest, windthrow, beetle salvage)",
			"loss_ha_2024": "ha, latest year", "loss_total_ha": "ha cumulative 2001-2024",
			"harvest_efm": "Erntefestmeter, state total downscaled by loss share — model value", "harvest_value_eur": "EUR, harvest_efm × yearly sawlog price",
			"co2_t": "t CO2 embodied in the year's harvested wood (model)", "net_flux_tco2e_ha": "t CO2e per forest ha, cumulative 2001-2024, negative = net sink",
			"price_spruce_eur_efm": "EUR/Efm spruce/fir sawlog Media 2b (state LK series, national STAT fallback; series start 2010 → null before)",
		},
		"metrics": metrics, "history": history,
		"caveats": []string{"Every KG in a Gemeinde returns the same municipal block.", "Harvest/value/CO2 are downscaled model values, not measurements."},
	}, true
}

// ---- HTTP plumbing: ETag + gzip + CORS -------------------------------------

func etagOf(b []byte) string {
	h := sha256.Sum256(b)
	return `"` + hex.EncodeToString(h[:8]) + `"`
}

// serveBytes writes b with ETag/304, gzip (if accepted), CORS and caching headers.
func serveBytes(w http.ResponseWriter, r *http.Request, status int, ctype string, b []byte) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Headers", "Content-Type, If-None-Match")
	h.Set("Access-Control-Expose-Headers", "ETag, Last-Modified")
	h.Set("Vary", "Accept-Encoding")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	et := etagOf(b)
	h.Set("ETag", et)
	h.Set("Content-Type", ctype)
	if status == http.StatusOK {
		h.Set("Cache-Control", "public, max-age=3600")
		if inm := r.Header.Get("If-None-Match"); inm != "" && strings.Contains(inm, et) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	} else {
		h.Set("Cache-Control", "public, max-age=300")
	}
	if len(b) > 512 && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		h.Set("Content-Encoding", "gzip")
		w.WriteHeader(status)
		gz := gzip.NewWriter(w)
		gz.Write(b)
		gz.Close()
		return
	}
	h.Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		w.Write(b)
	}
}

func serveJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	b, _ := json.Marshal(v)
	serveBytes(w, r, status, "application/json; charset=utf-8", b)
}

// cachedFileServer serves static files from dir fully buffered with ETag/gzip.
// Files are small so buffering is fine; an mtime-keyed cache avoids re-reading.
type fileCacheEntry struct {
	mod  time.Time
	body []byte
}

func cachedFileServer(dir string) http.Handler {
	var mu sync.Mutex
	cache := map[string]fileCacheEntry{}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := filepath.Clean("/" + r.URL.Path)
		p := filepath.Join(dir, name)
		st, err := os.Stat(p)
		if err != nil || st.IsDir() {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			http.NotFound(w, r)
			return
		}
		if st.Size() > 32<<20 {
			http.ServeFile(w, r, p)
			return
		}
		mu.Lock()
		e, ok := cache[p]
		mu.Unlock()
		if !ok || !e.mod.Equal(st.ModTime()) {
			b, err := os.ReadFile(p)
			if err != nil {
				http.Error(w, "read error", http.StatusInternalServerError)
				return
			}
			e = fileCacheEntry{st.ModTime(), b}
			mu.Lock()
			if len(cache) > 64 {
				cache = map[string]fileCacheEntry{}
			}
			cache[p] = e
			mu.Unlock()
		}
		ct := "application/octet-stream"
		switch strings.ToLower(filepath.Ext(p)) {
		case ".json", ".geojson":
			ct = "application/json; charset=utf-8"
		case ".csv":
			ct = "text/csv; charset=utf-8"
		case ".txt":
			ct = "text/plain; charset=utf-8"
		}
		w.Header().Set("Last-Modified", st.ModTime().UTC().Format(http.TimeFormat))
		serveBytes(w, r, http.StatusOK, ct, e.body)
	})
}

// ---- handlers ---------------------------------------------------------------

func registerLLM(dataDir, publicDir string) {
	if err := loadStore(dataDir); err != nil {
		log.Printf("llm: load failed: %v (endpoints will 503)", err)
	}
	// hot-reload when the weekly price cron or a kg-map rebuild touches data files
	go func() {
		watched := []string{"timber_price_catalog.json", "timber_prices.json", "kg_gemeinde.json", "year_2024.json"}
		last := map[string]time.Time{}
		for {
			changed := false
			for _, f := range watched {
				if st, err := os.Stat(filepath.Join(dataDir, f)); err == nil {
					if t, ok := last[f]; ok && !t.Equal(st.ModTime()) {
						changed = true
					}
					last[f] = st.ModTime()
				}
			}
			if changed {
				if err := loadStore(dataDir); err != nil {
					log.Printf("llm: reload failed: %v", err)
				}
			}
			time.Sleep(10 * time.Minute)
		}
	}()

	notLoaded := func(w http.ResponseWriter, r *http.Request) bool {
		store.mu.RLock()
		ok := store.kg != nil
		store.mu.RUnlock()
		if !ok {
			w.Header().Set("Retry-After", "30")
			serveJSON(w, r, http.StatusServiceUnavailable, map[string]any{"ready": false, "retry_after_s": 30, "error": "data not loaded"})
		}
		return !ok
	}
	pad := func(c string) string {
		c = strings.TrimSpace(c)
		if len(c) > 0 && len(c) < 5 {
			c = strings.Repeat("0", 5-len(c)) + c
		}
		return c
	}

	http.HandleFunc("/llm/kg/", func(w http.ResponseWriter, r *http.Request) {
		if notLoaded(w, r) {
			return
		}
		code := pad(strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/llm/kg/"), ".json"))
		store.mu.RLock()
		doc, ok := store.kgDoc(code)
		store.mu.RUnlock()
		if !ok {
			serveJSON(w, r, http.StatusNotFound, map[string]any{"kg_code": code, "error": "no_data", "service": serviceSlug})
			return
		}
		serveJSON(w, r, http.StatusOK, doc)
	})

	http.HandleFunc("/llm/kgs", func(w http.ResponseWriter, r *http.Request) {
		if notLoaded(w, r) {
			return
		}
		store.mu.RLock()
		defer store.mu.RUnlock()
		if r.URL.Query().Get("all") == "1" {
			codes := make([]string, 0, len(store.kg))
			for c, e := range store.kg {
				if _, ok := store.yearRows["2024"][e.G]; ok {
					codes = append(codes, c)
				}
			}
			sort.Strings(codes)
			serveJSON(w, r, http.StatusOK, map[string]any{"service": serviceSlug, "kg_count": len(codes), "kg_codes": codes, "as_of": "2024-12-31"})
			return
		}
		raw := strings.Split(r.URL.Query().Get("codes"), ",")
		if len(raw) > 500 {
			serveJSON(w, r, http.StatusBadRequest, map[string]any{"error": "max 500 codes"})
			return
		}
		results, missing := []any{}, []string{}
		for _, c := range raw {
			c = pad(c)
			if c == "" {
				continue
			}
			if d, ok := store.kgDoc(c); ok {
				results = append(results, d)
			} else {
				missing = append(missing, c)
			}
		}
		serveJSON(w, r, http.StatusOK, map[string]any{"service": serviceSlug, "results": results, "missing": missing})
	})

	http.HandleFunc("/llm/manifest.json", func(w http.ResponseWriter, r *http.Request) {
		if notLoaded(w, r) {
			return
		}
		store.mu.RLock()
		b := store.manifest
		store.mu.RUnlock()
		serveBytes(w, r, http.StatusOK, "application/json; charset=utf-8", b)
	})

	http.HandleFunc("/llm.txt", func(w http.ResponseWriter, r *http.Request) {
		b, err := os.ReadFile(filepath.Join(publicDir, "llm.txt"))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		serveBytes(w, r, http.StatusOK, "text/plain; charset=utf-8", b)
	})

	http.HandleFunc("/data/prices/state/", func(w http.ResponseWriter, r *http.Request) {
		if notLoaded(w, r) {
			return
		}
		d := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/data/prices/state/"), ".json")
		store.mu.RLock()
		b, ok := store.statePr[d]
		store.mu.RUnlock()
		if !ok {
			serveJSON(w, r, http.StatusNotFound, map[string]any{"error": "no_data", "state": d, "hint": "state digit 1-8 (Bundesland, first digit of KG/Gemeinde code); Wien (9) has no LK forest price report"})
			return
		}
		serveBytes(w, r, http.StatusOK, "application/json; charset=utf-8", b)
	})
}

// ---- plot-context fast cache (HOLZ-3) --------------------------------------

type pcEntry struct {
	at   time.Time
	code int
	body []byte
}

type pcCache struct {
	mu sync.Mutex
	m  map[string]pcEntry
}

var plotCache = &pcCache{m: map[string]pcEntry{}}

func (c *pcCache) key(payload []byte) string {
	h := sha256.Sum256(bytes.TrimSpace(payload))
	return hex.EncodeToString(h[:])
}

func (c *pcCache) get(k string) (pcEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[k]
	if ok && time.Since(e.at) > 24*time.Hour {
		delete(c.m, k)
		return pcEntry{}, false
	}
	return e, ok
}

func (c *pcCache) put(k string, code int, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) > 5000 {
		for kk, e := range c.m {
			if time.Since(e.at) > time.Hour {
				delete(c.m, kk)
			}
		}
		if len(c.m) > 5000 {
			c.m = map[string]pcEntry{}
		}
	}
	c.m[k] = pcEntry{time.Now(), code, body}
}
