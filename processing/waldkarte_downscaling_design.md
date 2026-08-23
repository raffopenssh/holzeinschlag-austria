# Waldkarte Forest Stand Level Downscaling Design

## Current Approach (Municipality Level)

### Data Structure
- **Input**: Hansen 30m loss pixels aggregated to municipalities
- **Process**: 
  1. Rasterize municipality boundaries to match Hansen grid
  2. For each pixel with loss, identify containing municipality
  3. Count pixels per municipality per year
  4. Scale to match official Bundesländer totals
- **Output**: Harvest estimates per municipality (2,093 geometries)

### Current Algorithm Flow
```
Hansen Loss Pixels (30m) 
    ↓ [spatial join]
Municipality Raster
    ↓ [aggregate by ISO code]
Pixel counts per municipality
    ↓ [convert to Efm using conversion factors]
Harvest estimates per municipality
    ↓ [scale by Bundesland totals]
Final municipality-level results
```

### Current Files
- `aggregate_by_gemeinde_yearly.py` - Main aggregation script
- `scale_to_official.py` - Scaling to match official data
- Output: `gemeinde_emissions_scaled.json`

---

## Proposed Approach (Forest Stand Level)

### Waldkarte Dataset Characteristics

Based on BFW Waldkarte WFS:
- **Count**: ~315,201 forest stand polygons
- **Average size**: ~0.3 hectares (range: 0.1 - 50+ ha)
- **Coverage**: All of Austria's forest area (~3.9M ha)
- **Attributes** (typical forest stand data):
  - Stand ID (unique identifier)
  - Municipality (Gemeinde) - links to existing data
  - Stand area (hectares)
  - Forest type (Nadel/Laub/Misch)
  - Age class / Development stage
  - Site quality
  - Ownership type (optional)
  - Management unit (Forstbetrieb)

### Key Insights

1. **Hierarchical relationship**: Forest Stands ⊂ Municipalities ⊂ Bundesländer
2. **Conservation property**: Sum of stand harvests = Municipality harvest
3. **Spatial precision**: 30m pixels can be attributed to specific stands
4. **Downscaling constraint**: Must maintain scaled municipality totals

---

## Proposed Algorithm

### Two-Stage Downscaling

#### Stage 1: Pixel → Forest Stand Attribution
```
Hansen Loss Pixels (30m)
    ↓ [spatial join]
Forest Stand Polygons (Waldkarte)
    ↓ [aggregate by stand ID + year]
Raw pixel counts per stand per year
```

#### Stage 2: Constrained Scaling
```
Raw stand pixel counts
    ↓ [sum by municipality]
Raw municipality totals
    ↓ [compare to scaled municipality totals]
Calculate municipality-level scaling factors
    ↓ [apply to each stand within municipality]
Scaled stand-level estimates
```

### Mathematical Formulation

For each year and municipality:

1. **Raw stand pixels**: Let `p_{s,y}` = raw pixel count for stand `s` in year `y`
2. **Raw municipality total**: `P_{m,y} = Σ p_{s,y}` for all stands in municipality `m`
3. **Scaled municipality target**: `H_{m,y}` (from existing scaled data)
4. **Municipality scaling factor**: `f_{m,y} = H_{m,y} / P_{m,y}` (if P > 0)
5. **Scaled stand estimate**: `h_{s,y} = p_{s,y} × f_{m,y}`

**Constraint**: `Σ h_{s,y} = H_{m,y}` (scaled stands sum to scaled municipality)

### Data Structure

```python
{
  "years": [2001, 2002, ..., 2024],
  "stands": {
    "<stand_id>": {
      "id": "WK_12345",
      "gemeinde_iso": "31001",
      "gemeinde_name": "Alberndorf im Pulkautal",
      "state": "Niederösterreich",
      "area_ha": 2.5,
      "forest_type": "Nadelwald",  # or null if not available
      "years": {
        "2023": {
          "pixels": 15,
          "loss_area_ha": 1.35,
          "harvest_efm": 247,
          "value_eur": 25441,
          "co2_tonnes": 111,
          "ets_eur": 9435
        },
        ...
      },
      "total_pixels": 42,
      "total_loss_ha": 3.78,
      "total_harvest_efm": 692
    },
    ...
  },
  "by_gemeinde_summary": {
    "31001": {
      "stand_count": 127,
      "stands_with_loss": 23,
      "years": {
        "2023": {
          "stand_count_with_loss": 8,
          "total_pixels": 234,
          "total_harvest_efm": 42891
        }
      }
    }
  }
}
```

---

## Implementation Plan

### Phase 1: Data Acquisition (1-2 days)

**Option A: WFS Download (Preferred)**
```python
# Download all 315k stands via WFS with tiling
# - Split Austria into grid (e.g., 50km tiles)
# - Fetch each tile with BBOX filter
# - Combine into single GeoPackage
# - Total size estimate: ~200-500 MB
```

**Option B: Request GPKG from BFW**
- Email data.bfw.ac.at requesting Waldkarte GPKG
- Faster if available, but may take days/weeks

### Phase 2: Rasterization (1-2 hours)

```python
#!/usr/bin/env python3
import geopandas as gpd
from osgeo import gdal, ogr
import numpy as np

# 1. Load Waldkarte GPKG
stands = gpd.read_file('waldkarte.gpkg')
print(f"Loaded {len(stands)} forest stands")

# 2. Match Hansen grid parameters
hansen = gdal.Open('raster/austria_lossyear.tif')
gt = hansen.GetGeoTransform()
proj = hansen.GetProjection()
xsize = hansen.RasterXSize
ysize = hansen.RasterYSize

# 3. Create stand ID raster (same grid as Hansen)
stand_raster = rasterize_polygons(
    stands, 
    xsize, ysize, gt, proj,
    attribute='stand_id',  # or FID
    nodata=0
)

# 4. Save as GeoTIFF
save_raster(stand_raster, 'raster/waldkarte_stand_ids.tif', ...)
```

### Phase 3: Aggregation (2-3 hours)

```python
#!/usr/bin/env python3
import numpy as np
from collections import defaultdict

# Load rasters
lossyear = load_raster('austria_lossyear.tif').flatten()
stand_ids = load_raster('waldkarte_stand_ids.tif').flatten()
gemeinde_ids = load_raster('gemeinde_ids.tif').flatten()

# Valid pixels: loss > 0, stand > 0
mask = (lossyear > 0) & (stand_ids > 0)

valid_loss = lossyear[mask]
valid_stands = stand_ids[mask]
valid_gemeinden = gemeinde_ids[mask]

# Create combined key: stand_id * 100 + year_val
combined = valid_stands.astype(np.int64) * 100 + valid_loss

# Aggregate
unique_keys, counts = np.unique(combined, return_counts=True)

# Build results
stand_pixels = defaultdict(lambda: defaultdict(int))
stand_to_gemeinde = {}  # From Waldkarte attributes

for key, count in zip(unique_keys, counts):
    stand_id = key // 100
    year_val = key % 100
    year = 2000 + year_val
    stand_pixels[stand_id][year] = count
    # Also track gemeinde for this stand
```

### Phase 4: Constrained Scaling (1 hour)

```python
#!/usr/bin/env python3
import json
from collections import defaultdict

# Load existing scaled municipality data
with open('gemeinde_emissions_scaled.json') as f:
    gemeinde_scaled = json.load(f)

# Load raw stand pixel counts
with open('stand_pixels_raw.json') as f:
    stand_raw = json.load(f)

# For each year and municipality:
for year_str in years:
    for gemeinde_iso in gemeinde_list:
        # Get scaled municipality target
        target_harvest = gemeinde_scaled['gemeinden'][year_str][gemeinde_iso]['h']
        
        # Get all stands in this municipality
        stands_in_gemeinde = [s for s in stand_raw if s['gemeinde'] == gemeinde_iso]
        
        # Sum raw pixels
        raw_total = sum(s['years'].get(year_str, {}).get('pixels', 0) 
                       for s in stands_in_gemeinde)
        
        # Calculate scaling factor
        if raw_total > 0:
            factor = target_harvest / raw_total
        else:
            factor = 0
        
        # Apply to each stand
        for stand in stands_in_gemeinde:
            raw_pixels = stand['years'].get(year_str, {}).get('pixels', 0)
            scaled_harvest = raw_pixels * factor
            stand['years'][year_str]['harvest_efm'] = scaled_harvest
            # ... calculate other metrics ...
```

### Phase 5: Integration (1-2 days)

1. **API Endpoints**:
   ```
   GET /api/stands?gemeinde=31001&year=2023
   GET /api/stands/<stand_id>
   GET /api/stands/geojson?gemeinde=31001&year=2023&min_harvest=100
   ```

2. **Frontend Updates**:
   - Add "View Forest Stands" button when municipality selected
   - Show stand-level map overlay
   - Stand detail popup with:
     - Stand ID, area, forest type
     - Yearly harvest timeline
     - Percentage of municipality total

3. **GeoPackage Updates**:
   - Add `forest_stands` layer
   - Link to municipalities via `gemeinde_iso`
   - Enable spatial queries

---

## Validation Strategy

### 1. Conservation Tests
```python
# Test 1: Stands sum to municipality
for gemeinde_iso in all_gemeinden:
    for year in years:
        stand_sum = sum(stands[s][year]['harvest'] 
                       for s in stands_in_gemeinde(gemeinde_iso))
        gemeinde_val = gemeinde_scaled[gemeinde_iso][year]['harvest']
        assert abs(stand_sum - gemeinde_val) < 0.01  # Allow rounding

# Test 2: Municipalities sum to Bundesland
# (Already validated in current approach)
```

### 2. Spatial Coherence Tests
```python
# Test: Stand pixels should be within stand polygon
for stand_id in sample(stands, 100):
    stand_geom = get_stand_geometry(stand_id)
    pixels_with_loss = get_loss_pixels(stand_id)
    for pixel in pixels_with_loss:
        assert pixel.within(stand_geom)
```

### 3. Distribution Analysis
```python
# Analyze harvest distribution across stands
# - Histogram of stand sizes with loss
# - Correlation between stand area and harvest
# - Identify outliers (unusually high harvest rate)
```

---

## Performance Considerations

### Storage
- **Waldkarte GPKG**: ~300-500 MB
- **Stand ID raster**: ~1.2 GB (uint32, 11000×14000 pixels)
- **Stand results JSON**: ~150-300 MB (315k stands × 24 years)
- **Total additional storage**: ~2 GB

### Computation
- **Rasterization**: ~5-10 minutes (one-time)
- **Aggregation**: ~10-20 minutes (optimized with numpy)
- **Scaling**: ~1 minute
- **Total processing**: ~30 minutes (one-time setup)

### Query Performance
- **Municipality → Stands lookup**: Fast (indexed by gemeinde_iso)
- **Spatial queries**: Use GPKG spatial index
- **Frontend**: Load stands on-demand (not default)

---

## Risks & Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Waldkarte WFS unavailable | Medium | High | Try multiple BFW endpoints, request GPKG directly |
| Stand boundaries don't align with Hansen | Low | Medium | Use majority rule for edge pixels |
| Too much data for frontend | Medium | Medium | Implement progressive loading, clustering |
| Rasterization takes too long | Low | Low | Use gdal_rasterize CLI (faster than Python) |
| Missing stand attributes | Medium | Low | Use minimal set (ID, gemeinde, area) |

---

## Expected Benefits

### For Analysis
1. **Identify specific harvest locations** - "Which forest stands had most loss?"
2. **Fine-grained temporal patterns** - Track same stand over multiple years
3. **Forest type analysis** - If available: Nadel vs Laub harvest patterns
4. **Size distribution** - Small vs large stand harvest differences
5. **Spatial clustering** - Identify harvest hotspots within municipalities

### For Visualization
1. **Interactive stand-level map** - Zoom in to see individual forests
2. **Stand detail cards** - Click stand → see full history
3. **Comparison tool** - Compare nearby stands
4. **Export capabilities** - Download stand-level data for research

### For Validation
1. **Ground-truthing** - Easier to verify specific stands
2. **Satellite imagery comparison** - Match stands to Sentinel/Landsat
3. **Local knowledge integration** - Foresters can identify their stands

---

## Next Steps

1. **Confirm approach** with user
2. **Download Waldkarte data** via WFS or request
3. **Run pilot** on single municipality (e.g., Wien)
4. **Validate** conservation properties
5. **Full processing** for all Austria
6. **Frontend integration** with progressive loading
7. **Documentation** and examples

---

## Questions for User

1. **Data access**: Should we try WFS download, or do you have BFW contacts for direct GPKG?
2. **Attributes**: Which stand attributes are most important? (area, type, age, ...)
3. **Frontend**: Show stands by default, or only on-demand for selected municipalities?
4. **Export format**: GeoPackage layer, separate JSON, or both?
5. **Processing time**: Is 30 min one-time setup acceptable?
