# Algorithm Adaptation: Municipality → Forest Stand Level

## Current Algorithm (Municipality Level)

```
┌─────────────────────────────────────────────────────────────────┐
│                    Hansen Loss Raster (30m)                     │
│              11,000 × 14,000 pixels = 154M pixels               │
│                                                                 │
│  Each pixel: year of loss (0=no loss, 1=2001, ..., 24=2024)   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Spatial Join
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│               Municipality Raster (Gemeinde IDs)                │
│              11,000 × 14,000 pixels = 154M pixels               │
│                                                                 │
│           Each pixel: ISO code of municipality (1-9xxxx)        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Aggregate
                              │ numpy.unique(gemeinde * 100 + year)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Raw Pixel Counts per Gemeinde                 │
│                         ~2,093 records                          │
│                                                                 │
│  {"31001": {"2023": 4766 pixels, "2022": 4249 pixels, ...}}   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Convert to Efm
                              │ × conversion factors
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              Raw Harvest Estimates (Unscaled)                   │
│                                                                 │
│            Niederösterreich total: 3.2M Efm (raw)              │
│             Official (Bundesland): 4.1M Efm                    │
│                  → Scaling factor: 1.28x                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Scale by Bundesland
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│           Scaled Harvest Estimates (Final Output)               │
│                                                                 │
│  Alberndorf: 42,891 Efm (2023)                                 │
│  Wien: 125,000 Efm (2023)                                      │
│  ...all 2,093 municipalities                                   │
└─────────────────────────────────────────────────────────────────┘
```

**Key characteristics:**
- **Granularity**: Municipality (Gemeinde) level
- **Count**: 2,093 spatial units
- **Average size**: ~40 km² per unit
- **Aggregation**: Single numpy.unique() call
- **Scaling**: By Bundesland (9 regions)
- **Output**: Cannot identify specific forests

---

## Proposed Algorithm (Forest Stand Level)

```
┌─────────────────────────────────────────────────────────────────┐
│                    Hansen Loss Raster (30m)                     │
│              11,000 × 14,000 pixels = 154M pixels               │
│                                                                 │
│  Each pixel: year of loss (0=no loss, 1=2001, ..., 24=2024)   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Spatial Join (NEW)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│             Forest Stand Raster (Waldkarte IDs)   ★ NEW        │
│              11,000 × 14,000 pixels = 154M pixels               │
│                                                                 │
│        Each pixel: Stand ID (1-315201) from BFW Waldkarte      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Aggregate (similar to current)
                              │ numpy.unique(stand_id * 100 + year)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│            Raw Pixel Counts per Forest Stand   ★ NEW            │
│                       ~315,201 records                          │
│                                                                 │
│  {"WK_31001_001": {"2023": 178 px, "2022": 0 px},             │
│   "WK_31001_002": {"2023": 89 px, "2022": 45 px}, ...}        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Group by Gemeinde
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│          Raw Municipality Totals (from stands)   ★ NEW          │
│                                                                 │
│  Alberndorf (31001): 267 pixels raw                            │
│    = WK_31001_001 (178 px) + WK_31001_002 (89 px)             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Compare to existing scaled data
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│        Existing Scaled Municipality Data (from current app)     │
│                                                                 │
│  Alberndorf (31001): 42,891 Efm (2023)                         │
│  This is our CONSTRAINT - must match exactly                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Calculate constrained scaling
                              │ factor = 42,891 Efm / 267 pixels
                              │       = 160.64 Efm/pixel
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│         Apply Scaling to Each Stand   ★ NEW                     │
│                                                                 │
│  WK_31001_001: 178 px × 160.64 = 28,594 Efm                   │
│  WK_31001_002:  89 px × 160.64 = 14,297 Efm                   │
│                                  ─────────                      │
│  Sum:                            42,891 Efm ✓ (matches!)       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Enrichment
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│           Final Stand-Level Output   ★ NEW                      │
│                                                                 │
│  WK_31001_001 (Nadelwald, 3.2 ha):                            │
│    - 2023: 28,594 Efm (66.7% of municipality)                 │
│    - Loss area: 16.02 ha (HIGH intensity)                      │
│    - Density: 1,785 Efm/ha                                     │
│                                                                 │
│  WK_31001_002 (Mischwald, 1.5 ha):                            │
│    - 2023: 14,297 Efm (33.3% of municipality)                 │
│    - 2022:  7,229 Efm                                          │
│    - Multi-year stand history available                        │
└─────────────────────────────────────────────────────────────────┘
```

**Key characteristics:**
- **Granularity**: Forest stand (Waldbestand) level
- **Count**: ~315,201 spatial units
- **Average size**: ~0.3 ha per unit (150× finer!)
- **Aggregation**: Two-stage (stand, then municipality)
- **Scaling**: Constrained by existing scaled municipality data
- **Output**: Specific forest identification + attributes

---

## Key Algorithm Changes

### 1. Additional Rasterization Step

**NEW preprocessing required:**
```python
# One-time setup
waldkarte = gpd.read_file('waldkarte.gpkg')  # 315k polygons
stand_raster = rasterize(
    waldkarte,
    template=hansen_raster,  # Match grid exactly
    attribute='stand_id',
    dtype=np.uint32  # Up to 4.2B unique IDs
)
save('raster/waldkarte_stand_ids.tif', stand_raster)
```

**File size**: ~1.2 GB (uint32, 154M pixels)
**Processing time**: ~5-10 minutes (one-time)

### 2. Modified Aggregation

**Current code** (aggregate_by_gemeinde_yearly.py):
```python
# Current: Single aggregation
combined_keys = gemeinde_ids * 100 + lossyear
unique_keys, counts = np.unique(combined_keys, return_counts=True)

for key, count in zip(unique_keys, counts):
    gemeinde_id = key // 100
    year = 2000 + (key % 100)
    counts_dict[gemeinde_id][year] = count
```

**Proposed** (aggregate_by_stand.py):
```python
# Proposed: Same pattern, different ID
combined_keys = stand_ids * 100 + lossyear  # ★ Use stand_ids instead
unique_keys, counts = np.unique(combined_keys, return_counts=True)

for key, count in zip(unique_keys, counts):
    stand_id = key // 100                    # ★ Extract stand_id
    year = 2000 + (key % 100)
    counts_dict[stand_id][year] = count
    
    # Also accumulate by gemeinde for validation
    gemeinde = stand_to_gemeinde[stand_id]   # ★ Lookup from Waldkarte
    gemeinde_raw[gemeinde][year] += count
```

**Key insight**: The numpy aggregation logic is **identical**, just operating on different IDs!

### 3. Constrained Scaling (NEW)

**This is the main algorithmic addition:**

```python
# Load existing scaled municipality data (ground truth)
with open('gemeinde_emissions_scaled.json') as f:
    gemeinde_scaled = json.load(f)

# For each municipality and year
for gemeinde_iso in all_gemeinden:
    for year_str in years:
        # Get target from existing scaled data
        target_harvest = gemeinde_scaled['gemeinden'][year_str][gemeinde_iso]['h']
        
        # Sum raw pixels from stands in this municipality
        stands_in_gemeinde = [s for s in stands if s['gemeinde'] == gemeinde_iso]
        raw_pixel_sum = sum(s['years'][year_str]['pixels'] for s in stands_in_gemeinde)
        
        # Calculate municipality-specific scaling factor
        if raw_pixel_sum > 0:
            scaling_factor = target_harvest / raw_pixel_sum
        else:
            scaling_factor = 0
        
        # Apply to each stand
        for stand in stands_in_gemeinde:
            raw_pixels = stand['years'][year_str]['pixels']
            stand['years'][year_str]['harvest_efm'] = raw_pixels * scaling_factor
        
        # VALIDATION: Sum must equal target
        stand_sum = sum(s['years'][year_str]['harvest_efm'] for s in stands_in_gemeinde)
        assert abs(stand_sum - target_harvest) < 0.01  # Allow rounding error
```

**Mathematical guarantee**: Stand harvests always sum to municipality totals

---

## Validation Strategy

### Conservation Tests

```python
# Test 1: Stands → Municipality (NEW)
for gemeinde in all_gemeinden:
    for year in years:
        stand_sum = sum(stands in gemeinde)
        municipality_value = gemeinde_scaled[year]
        assert stand_sum ≈ municipality_value  # Must match!

# Test 2: Municipalities → Bundesland (EXISTING)
# Already validated in current approach
# No changes needed

# Test 3: Bundesländer → Austria (EXISTING)  
# Already validated in current approach
# No changes needed
```

**Hierarchical consistency**:
```
Forest Stands
    ↓ (constrained scaling)
Municipalities  ✓ Matches scaled data
    ↓ (existing scaling)
Bundesländer   ✓ Matches official reports
    ↓ (sum)
Austria        ✓ Matches BFW totals
```

### Spatial Tests

```python
# Test: Pixels should be within stand polygons
for stand_id in random.sample(stands, 100):
    pixels_with_loss = get_loss_pixels(stand_id)
    stand_polygon = waldkarte[stand_id].geometry
    
    for pixel in pixels_with_loss:
        pixel_center = pixel_to_coords(pixel)
        assert stand_polygon.contains(pixel_center)
```

---

## Performance Analysis

### Current (Municipality Level)
```
Rasterization:  N/A (already done)
Aggregation:    ~10 min (2,093 units)
Scaling:        ~1 min
Total:          ~11 min
Output size:    ~9 MB JSON
```

### Proposed (Forest Stand Level)
```
Rasterization:  ~10 min (one-time, 315k polygons)  ★ NEW
Aggregation:    ~20 min (315,201 units, ~2× slower) ★ CHANGED
Scaling:        ~2 min (more loops)                 ★ CHANGED
Total:          ~32 min first time, ~22 min updates
Output size:    ~300 MB JSON (full detail)          ★ LARGER
                ~50 MB JSON (compact format)
```

**Bottleneck**: Rasterization is one-time cost
**Runtime increase**: ~2× for ongoing updates (acceptable)
**Storage increase**: ~2 GB total (raster + results)

### Optimizations

1. **Lazy loading**: Don't load all stand data upfront
2. **Spatial indexing**: Use GeoPackage R-tree for queries
3. **Compact format**: Use abbreviated keys in JSON
4. **Caching**: Pre-compute common queries
5. **Progressive loading**: Load stands only when municipality selected

---

## Decision Matrix

| Aspect | Current (Municipality) | Proposed (Stand) | Trade-off |
|--------|----------------------|------------------|----------|
| **Spatial precision** | 40 km² avg | 0.3 ha avg | 🟢 150× finer |
| **Unit count** | 2,093 | 315,201 | 🟡 150× more |
| **Processing time** | 11 min | 32 min | 🟡 3× slower |
| **Storage** | 9 MB | ~2 GB | 🟡 200× larger |
| **API complexity** | Simple | Moderate | 🟡 More endpoints |
| **Query flexibility** | Limited | High | 🟢 Can filter/aggregate |
| **Validation** | Easy | Hierarchical | 🟡 More tests |
| **Conservation** | By Bundesland | By Municipality | 🟢 Tighter constraint |
| **Research value** | Moderate | High | 🟢 Stand-level insights |
| **User questions** | "Which municipality?" | "Which forest?" | 🟢 More specific |

**Overall**: Trade-off of 3× processing time + 2GB storage for **150× spatial precision**

---

## Recommendation

✅ **Implement stand-level downscaling**

Reasons:
1. **Processing time acceptable** (~30 min is fine for daily/weekly updates)
2. **Storage is cheap** (2GB is trivial on modern systems)
3. **Value is high** (answer "which exact forest" questions)
4. **Conservative** (maintains all existing validation)
5. **Extensible** (can add forest attributes later)
6. **Non-breaking** (keep municipality-level as default view)

Implementation order:
1. Start with pilot (single municipality)
2. Validate conservation properties
3. Run full Austria processing
4. Add API endpoints
5. Frontend integration (progressive loading)
