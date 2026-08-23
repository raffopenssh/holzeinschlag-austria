# Holzeinschlag Austria

Austria-wide forestry statistics app + **context API for external forestry systems**.
Go server (`main.go` → `./server`, systemd unit `holzeinschlag`, port 8000, public/no auth).
Public URL: https://holzeinschlag-at.exe.xyz (exe.dev proxy; NOT reachable from this VM itself).

## Layout
- `main.go` — HTTP server: static UI (`public/`), `/data/*` JSON, `/api/export` (GPKG),
  `/api/plot-context` (POST GeoJSON → plot forestry context), `/api/llm.txt` (API docs).
- `processing/plot_context.py` — plot-context engine: pixel-exact zonal stats from local
  Hansen/carbon rasters, 1km/5km ring comparison, municipality lookup, 2001–2024 timeline,
  regional LK prices. Called by the Go server via stdin/stdout JSON. GDAL gotchas: keep
  dataset refs alive (band-after-dataset-GC crash); `os._exit(0)` to suppress SWIG stderr noise.
- `processing/update_timber_prices.py` / `update_price_catalog.py` — refresh
  `data/timber_prices.json` (compact national) and `data/timber_price_catalog.json`
  (280 series: per-state LK sawlog/industrial/fuelwood ranges, pellets, futures) from
  preise.agrarforschung.at (`/api/getcontent?contentId=<publicName>`). Atomic writes,
  fail-safe (old file kept). Cron: `/etc/cron.d/holzeinschlag-prices` (Mon 05:17 UTC),
  log: `processing/price_update.log`.
- `data/` — precomputed JSON: per-municipality harvest (downscaled from Statistik Austria
  state totals via Hansen loss shares — model values, not measurements), carbon flux
  densities, yearly loss, boundaries, prices. `year_<Y>.json` compact row format:
  `[loss_px, loss_ha, harvest_efm, value_eur, co2_t, ets_eur, ets_pc]`.
- `raster/` — Hansen GFC-2024 lossyear + treecover2000 (30m, Austria clip),
  `raster/carbon_flux/` Harris et al. tiles (tCO2e/ha cumulative 2001–2024; negative = sink).
- `public/api/llm.txt` — **the API contract for the downstream forestry-management VM.
  Keep it in sync when changing plot-context output.**

## Division of labour (do not violate)
- THIS VM: history 2001–2024, economics (prices/harvest value), carbon, regional benchmarks.
- srtm-lidar-at.exe.xyz: LiDAR segmentation, per-tree change, terrain (read-only upstream).
- forestry-management VM (downstream consumer): stands, yield models, UI.

## Conventions
- Forest = Hansen treecover2000 ≥ 30%. "Loss" = stand-replacement disturbance incl. harvest.
- Areas in EPSG:3035; geometry I/O WGS84 lon/lat. Efm = Erntefestmeter.
- Rebuild: `go build -o server . && sudo systemctl restart holzeinschlag`.
- Test: `echo '<geojson>' | python3 processing/plot_context.py` or POST to
  `localhost:8000/api/plot-context` (serialized: one request at a time, 429 when busy).
