# Waldkarte Forest Stand Integration - Design Summary

## Executive Summary

Proposal to extend the existing municipality-level forest loss analysis to **individual forest stand level** using BFW Waldkarte data (315,201 forest stands).

**Key insight**: Use same aggregation algorithm, just with finer spatial units (stands instead of municipalities), then apply **constrained scaling** to maintain consistency with existing official-data-calibrated municipality totals.

---

## Current vs Proposed

| Aspect | Current (Municipality) | Proposed (Forest Stand) | Improvement |
|--------|----------------------|----------------------|-------------|
| **Spatial units** | 2,093 municipalities | 315,201 forest stands | **150× more** |
| **Avg unit size** | ~40 km² | ~0.3 ha (3,000 m²) | **150× finer** |
| **Question answerable** | "Which municipality had most harvest?" | "Which exact forest was cut?" | **Specific locations** |
| **Forest attributes** | No | Yes (type, age, size) | **Better insights** |
| **Processing time** | 11 min | 32 min (one-time) | **Acceptable cost** |
| **Storage** | 9 MB | 2 GB | **Cheap** |
| **Conservation** | By Bundesland (9 regions) | By Municipality (2,093 units) | **230× tighter constraint** |

---

## Algorithm Adaptation

### Current Approach (Simplified)

```python
# 1. Load rasters
lossyear = load('austria_lossyear.tif')      # Hansen data
gemeinde_ids = load('gemeinde_ids.tif')       # Municipality boundaries

# 2. Aggregate: Which municipality, which year?
combined = gemeinde_ids * 100 + lossyear
unique_keys, counts = np.unique(combined, return_counts=True)

# 3. Convert to harvest & scale by Bundesland
for gemeinde, pixels in results:
    harvest_raw = pixels * conversion_factor
    bundesland = get_bundesland(gemeinde)
    scaling_factor = official[bundesland] / sum_gemeinden[bundesland]
    harvest_scaled = harvest_raw * scaling_factor
```

**Output**: Municipality-level harvest (e.g., "Alberndorf: 42,891 Efm")

---

### Proposed Approach

```python
# 1. Load rasters (ONE NEW RASTER)
lossyear = load('austria_lossyear.tif')      # Hansen data (existing)
stand_ids = load('waldkarte_stand_ids.tif')  # ★ NEW: Forest stand boundaries

# 2. Aggregate: Which STAND, which year? (SAME PATTERN)
combined = stand_ids * 100 + lossyear        # ★ Use stand_ids instead of gemeinde_ids
unique_keys, counts = np.unique(combined, return_counts=True)

# 3. Group stands by municipality
for stand, pixels in results:
    gemeinde = stand_to_gemeinde_lookup[stand]
    raw_gemeinde_total[gemeinde] += pixels

# 4. Constrained scaling (NEW STEP)
for gemeinde in all_gemeinden:
    # Get existing scaled municipality total (from current approach)
    target = gemeinde_emissions_scaled[gemeinde][year]['harvest']
    
    # Calculate scaling factor
    raw_total = raw_gemeinde_total[gemeinde]
    factor = target / raw_total
    
    # Apply to each stand in this municipality
    for stand in stands_in(gemeinde):
        stand_harvest = stand_pixels * factor
    
    # GUARANTEED: sum(stand_harvest) == target (municipality total)
```

**Output**: Stand-level harvest (e.g., "Stand WK_31001_001 (Nadelwald, 3.2 ha): 28,594 Efm")

**Key property**: Stands sum to municipalities, municipalities sum to Bundesländer, Bundesländer sum to Austria

---

## What Changes in the Code

### 1. One-Time Preprocessing (NEW)

```python
# processing/rasterize_waldkarte.py (NEW FILE)

import geopandas as gpd
from osgeo import gdal

# Download Waldkarte from BFW WFS
waldkarte = download_waldkarte()  # 315k polygons

# Rasterize to match Hansen grid exactly
hansen_template = gdal.Open('raster/austria_lossyear.tif')
stand_raster = rasterize(
    waldkarte,
    template=hansen_template,
    attribute='stand_id',
    dtype=np.uint32
)

# Save
save('raster/waldkarte_stand_ids.tif', stand_raster)  # ~1.2 GB

# Also save stand metadata
waldkarte[['stand_id', 'gemeinde_iso', 'area_ha', 'forest_type']].to_file(
    'data/waldkarte_stands.gpkg'
)
```

**Runtime**: ~10 minutes (one-time)

### 2. Modified Aggregation (ADAPTED)

```python
# processing/aggregate_by_stand.py (ADAPTED FROM aggregate_by_gemeinde_yearly.py)

# Load rasters
lossyear = load('austria_lossyear.tif')
stand_ids = load('waldkarte_stand_ids.tif')  # ← CHANGED (was gemeinde_ids)

# IDENTICAL aggregation logic
mask = (lossyear > 0) & (stand_ids > 0)
combined = stand_ids[mask] * 100 + lossyear[mask]
unique_keys, counts = np.unique(combined, return_counts=True)

# Extract results (same pattern, different ID)
for key, count in zip(unique_keys, counts):
    stand_id = key // 100        # ← CHANGED (was gemeinde_id)
    year = 2000 + (key % 100)
    stand_pixels[stand_id][year] = count
```

**Runtime**: ~20 minutes (was ~10 min, 2× more units)

### 3. Constrained Scaling (NEW)

```python
# processing/scale_stands_to_gemeinden.py (NEW FILE)

# Load existing scaled municipality data
gemeinde_scaled = json.load('gemeinde_emissions_scaled.json')

# Load raw stand pixel counts
stand_pixels = json.load('stand_pixels_raw.json')

# Load stand → gemeinde mapping
stand_metadata = gpd.read_file('waldkarte_stands.gpkg')
stand_to_gemeinde = dict(zip(stand_metadata.stand_id, stand_metadata.gemeinde_iso))

# For each municipality and year
for gemeinde_iso in all_gemeinden:
    for year in years:
        # Target from existing scaled data (CONSTRAINT)
        target_harvest = gemeinde_scaled['gemeinden'][year][gemeinde_iso]['h']
        
        # Sum raw pixels from stands in this municipality
        stands = [s for s in stand_pixels if stand_to_gemeinde[s] == gemeinde_iso]
        raw_sum = sum(stand_pixels[s][year] for s in stands)
        
        # Calculate scaling factor
        factor = target_harvest / raw_sum if raw_sum > 0 else 0
        
        # Apply to each stand
        for stand_id in stands:
            raw = stand_pixels[stand_id][year]
            scaled_harvest = raw * factor
            
            # Also calculate derived values (CO2, value, etc.)
            stand_results[stand_id][year] = {
                'harvest_efm': scaled_harvest,
                'loss_area_ha': raw * 0.09,  # 30m pixels
                'gemeinde_iso': gemeinde_iso,
                # ... other metrics ...
            }
        
        # VALIDATION
        stand_sum = sum(stand_results[s][year]['harvest_efm'] for s in stands)
        assert abs(stand_sum - target_harvest) < 0.01  # Must match!
```

**Runtime**: ~2 minutes

### 4. API Endpoints (NEW)

```go
// server.go - Add new endpoints

// Get all stands in a municipality with harvest in a year
func handleStands(w http.ResponseWriter, r *http.Request) {
    gemeinde := r.URL.Query().Get("gemeinde")  // e.g., "31001"
    year := r.URL.Query().Get("year")          // e.g., "2023"
    minHarvest := parseFloat(r.URL.Query().Get("min_harvest"))  // Optional filter
    
    stands := loadStands(gemeinde, year)
    
    // Filter
    filtered := []Stand{}
    for _, stand := range stands {
        if stand.HarvestEfm >= minHarvest {
            filtered = append(filtered, stand)
        }
    }
    
    json.NewEncoder(w).Encode(filtered)
}

// Get single stand history
func handleStandDetail(w http.ResponseWriter, r *http.Request) {
    standID := mux.Vars(r)["id"]  // e.g., "WK_31001_001"
    
    stand := loadStandHistory(standID)  // All years
    json.NewEncoder(w).Encode(stand)
}

// Get GeoJSON for mapping
func handleStandsGeoJSON(w http.ResponseWriter, r *http.Request) {
    gemeinde := r.URL.Query().Get("gemeinde")
    year := r.URL.Query().Get("year")
    
    // Load from GeoPackage with spatial filter
    geojson := queryGeoPackage(
        "data/forest_stands.gpkg",
        "gemeinde_iso = ? AND year = ?",
        gemeinde, year
    )
    
    json.NewEncoder(w).Encode(geojson)
}
```

### 5. Frontend Integration (OPTIONAL, PROGRESSIVE)

```javascript
// public/app.js - Add stand-level view

// When municipality clicked
function onMunicipalityClick(gemeinde, year) {
    // Show existing municipality stats (no change)
    showMunicipalityStats(gemeinde, year);
    
    // NEW: Add "View Forest Stands" button
    const viewStandsBtn = document.createElement('button');
    viewStandsBtn.textContent = 'View Forest Stands';
    viewStandsBtn.onclick = () => loadStands(gemeinde, year);
    statsPanel.appendChild(viewStandsBtn);
}

// Load stand-level data
async function loadStands(gemeinde, year) {
    const response = await fetch(
        `/api/stands?gemeinde=${gemeinde}&year=${year}&min_harvest=100`
    );
    const stands = await response.json();
    
    // Show stand list
    displayStandList(stands);
    
    // Optionally load GeoJSON and show on map
    const geojson = await fetch(
        `/api/stands/geojson?gemeinde=${gemeinde}&year=${year}`
    ).then(r => r.json());
    
    addStandLayer(map, geojson);
}
```

---

## Validation Hierarchy

```
Austria Total (BFW official)
  │
  ├── Burgenland      ✓ Matches official reports
  ├── Kärnten         ✓ Matches official reports  
  ├── Niederösterreich  ✓ Matches official reports
  │   │
  │   ├── Alberndorf    ✓ Scaled to sum to Niederösterreich
  │   │   │
  │   │   ├── Stand WK_31001_001  ★ NEW: Constrained to sum to Alberndorf
  │   │   ├── Stand WK_31001_002  ★ NEW: Constrained to sum to Alberndorf  
  │   │   ├── Stand WK_31001_003  ★ NEW: Constrained to sum to Alberndorf
  │   │   └── ... (124 more stands)
  │   │
  │   ├── Wien
  │   └── ... (571 more gemeinden)
  │
  ├── Oberösterreich
  └── ... (6 more Bundesländer)
```

**Each level sums to parent level** - hierarchical conservation property

---

## Sample Output

### Current (Municipality Level)
```json
{
  "31001": {
    "name": "Alberndorf im Pulkautal",
    "state": "Niederösterreich",
    "harvest_efm": 42891,
    "loss_area_ha": 428.94,
    "year": 2023
  }
}
```

**Question**: "Where in Alberndorf was this harvest?" → **Cannot answer**

### Proposed (Forest Stand Level)
```json
{
  "WK_31001_001": {
    "stand_id": "WK_31001_001",
    "gemeinde_iso": "31001",
    "gemeinde_name": "Alberndorf im Pulkautal",
    "state": "Niederösterreich",
    "area_ha": 3.2,
    "forest_type": "Nadelwald",
    "age_class": "III (40-60 years)",
    "year": 2023,
    "loss_area_ha": 16.02,
    "harvest_efm": 28594,
    "pct_of_municipality": 66.67,
    "harvest_density_efm_ha": 1784.9,
    "intensity": "HIGH (>50% of stand area)",
    "geometry": {...}  // From GeoPackage
  },
  "WK_31001_002": {
    "stand_id": "WK_31001_002",
    "gemeinde_iso": "31001",
    "area_ha": 1.5,
    "forest_type": "Mischwald",
    "harvest_efm": 14297,
    "pct_of_municipality": 33.33,
    ...
  }
}
```

**Question**: "Where in Alberndorf was this harvest?" → **Stand WK_31001_001 (Nadelwald, north of village)**

**Validation**: 28,594 + 14,297 = 42,891 Efm ✓ (matches municipality total)

---

## Benefits

### For Analysis
1. **Spatial precision**: Identify exact forest locations
2. **Forest attributes**: Analyze by type (Nadel/Laub/Misch), age, size
3. **Temporal tracking**: Follow same stand over multiple years
4. **Distribution patterns**: Small vs large stand differences
5. **Hotspot identification**: Spatial clustering within municipalities

### For Validation
1. **Ground-truthing**: Easier to verify specific stands with satellite imagery
2. **Local knowledge**: Foresters can identify their stands
3. **Tighter constraints**: 2,093 conservation checks vs 9

### For Communication
1. **Specific examples**: "This 3.2 ha Nadelwald lost 66% in 2023"
2. **Visual impact**: Map individual forests, not just municipality boundaries
3. **Stakeholder engagement**: Forest owners can see their land

---

## Cost-Benefit Summary

### Costs
- **One-time setup**: 30 min processing
- **Storage**: +2 GB
- **Code complexity**: +3 files (~500 lines)
- **API**: +3 endpoints

### Benefits
- **Spatial precision**: 150× finer
- **Research value**: Stand-level insights  
- **Validation**: 230× more constraints
- **User questions**: "Which forest?" answerable

**ROI**: High - low cost for significant capability increase

---

## Risks & Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Waldkarte data unavailable | Low | High | Multiple BFW endpoints, request GPKG |
| Stand boundaries misaligned | Low | Medium | Use majority rule for edge pixels |
| Too much data for frontend | Medium | Medium | Progressive loading, lazy evaluation |
| Processing time too long | Low | Low | Acceptable for daily/weekly updates |
| Storage issues | Very Low | Low | 2GB is trivial on modern systems |

---

## Implementation Phases

### Phase 1: Proof of Concept (1 day)
- Download Waldkarte sample for 1 municipality
- Rasterize and aggregate
- Validate conservation property
- **Deliverable**: Working demo on test data

### Phase 2: Full Processing (2 days)
- Download all Waldkarte data
- Rasterize entire Austria
- Run full aggregation
- Apply constrained scaling
- **Deliverable**: Complete stand-level dataset

### Phase 3: API & Export (1 day)
- Add API endpoints
- Generate GeoPackage layer
- Create compact JSON format
- **Deliverable**: Queryable stand data

### Phase 4: Frontend (2 days)
- Add "View Stands" UI
- Stand detail cards
- Map overlay (optional)
- **Deliverable**: User-facing stand view

**Total**: ~1 week implementation

---

## Recommendation

✅ **PROCEED WITH IMPLEMENTATION**

**Justification**:
1. Low cost (30 min processing, 2GB storage)
2. High value (150× spatial precision)
3. Conservative (maintains all existing validation)
4. Extensible (can add attributes later)
5. Non-breaking (keeps municipality view as default)

**Suggested approach**:
1. Start with Phase 1 proof of concept
2. Validate algorithm on test municipality
3. If successful, proceed with full implementation
4. Frontend integration can be gradual

---

## Files to Create/Modify

### New Files
```
processing/
  rasterize_waldkarte.py          # Download & rasterize Waldkarte
  aggregate_by_stand.py           # Stand-level aggregation
  scale_stands_to_gemeinden.py    # Constrained scaling
  
data/
  waldkarte_stands.gpkg           # Stand metadata + geometry
  forest_stands.gpkg              # Full stand results with geometry
  stand_emissions_scaled.json     # Stand-level harvest data
  
raster/
  waldkarte_stand_ids.tif         # Stand ID raster (uint32)
```

### Modified Files
```
server.go                          # Add /api/stands endpoints
public/app.js                      # Add stand view (optional)
README.md                          # Document stand-level feature
```

### Unchanged Files  
```
processing/aggregate_by_gemeinde_yearly.py  # Keep for comparison
processing/scale_to_official.py             # Still needed for Bundesland scaling
data/gemeinde_emissions_scaled.json         # Source of truth for municipalities
```

---

## Questions Before Implementation

1. **Data access**: Try WFS download first, or contact BFW for bulk GPKG?
2. **Attributes**: Which stand attributes are priority? (area, type, age, ownership)
3. **Frontend**: Implement stand view immediately, or API-only first?
4. **Export format**: GeoPackage, GeoJSON, both?
5. **Update frequency**: Process stands with each Hansen update, or on-demand?
6. **Pilot scope**: Test on single municipality or small Bundesland first?

Ready to proceed when you confirm! 🚀
