#!/usr/bin/env python3
"""
Phase B Tasks 4-6: Aggregate Hansen forest loss by Bundesland
- Calculate forest loss area per pixel per year
- Aggregate loss pixels by Bundesland
- Create ratio: Hansen_loss_state / Official_harvest_state
"""

import json
import numpy as np
from pathlib import Path
from osgeo import gdal, ogr, osr
import sys

BASE_DIR = Path(__file__).parent.parent
RASTER_DIR = BASE_DIR / "raster"
DATA_DIR = BASE_DIR / "data"
STATUS_FILE = BASE_DIR / "processing" / "status.json"

# Hansen pixel resolution at 47°N (Austria's center latitude)
# Hansen data is ~30m resolution (1 arcsecond)
PIXEL_SIZE_M = 30.0  # meters
PIXEL_AREA_HA = (PIXEL_SIZE_M ** 2) / 10000  # hectares per pixel

def update_status(phase, task, status, progress=0, message=""):
    status_data = {}
    if STATUS_FILE.exists():
        with open(STATUS_FILE) as f:
            status_data = json.load(f)
    if phase not in status_data:
        status_data[phase] = {}
    status_data[phase][task] = {"status": status, "progress": progress, "message": message}
    with open(STATUS_FILE, "w") as f:
        json.dump(status_data, f, indent=2)

def load_state_boundaries():
    """Load Austrian state boundaries from GeoJSON"""
    geojson_path = DATA_DIR / "austria_states.geojson"
    ds = ogr.Open(str(geojson_path))
    layer = ds.GetLayer()
    
    states = {}
    for feature in layer:
        name = feature.GetField("name")
        geom = feature.GetGeometryRef()
        states[name] = geom.Clone()
    
    return states

def rasterize_state(state_name, state_geom, raster_ds):
    """Create a binary mask for a state matching the raster dimensions"""
    # Get raster properties
    gt = raster_ds.GetGeoTransform()
    width = raster_ds.RasterXSize
    height = raster_ds.RasterYSize
    proj = raster_ds.GetProjection()
    
    # Create memory raster for mask
    driver = gdal.GetDriverByName('MEM')
    mask_ds = driver.Create('', width, height, 1, gdal.GDT_Byte)
    mask_ds.SetGeoTransform(gt)
    mask_ds.SetProjection(proj)
    
    # Create memory layer with the state geometry
    mem_driver = ogr.GetDriverByName('Memory')
    mem_ds = mem_driver.CreateDataSource('')
    srs = osr.SpatialReference()
    srs.ImportFromWkt(proj)
    mem_layer = mem_ds.CreateLayer('state', srs, ogr.wkbPolygon)
    
    # Add the geometry
    feat = ogr.Feature(mem_layer.GetLayerDefn())
    feat.SetGeometry(state_geom)
    mem_layer.CreateFeature(feat)
    
    # Rasterize
    gdal.RasterizeLayer(mask_ds, [1], mem_layer, burn_values=[1])
    
    mask = mask_ds.GetRasterBand(1).ReadAsArray()
    return mask

def analyze_forest_loss():
    """Main analysis: aggregate forest loss by state and year"""
    update_status("analyze", "aggregate", "running", 0, "Starting analysis...")
    
    # Load clipped lossyear raster
    lossyear_path = RASTER_DIR / "austria_lossyear.tif"
    if not lossyear_path.exists():
        print(f"Error: {lossyear_path} not found")
        update_status("analyze", "aggregate", "error", 0, "Clipped raster not found")
        return None
    
    lossyear_ds = gdal.Open(str(lossyear_path))
    lossyear_band = lossyear_ds.GetRasterBand(1)
    lossyear_data = lossyear_band.ReadAsArray()
    
    print(f"Raster size: {lossyear_data.shape}")
    print(f"Loss year range: {lossyear_data[lossyear_data > 0].min()} - {lossyear_data.max()}")
    
    # Load state boundaries
    states = load_state_boundaries()
    print(f"Loaded {len(states)} states")
    
    # Load official harvest data
    with open(DATA_DIR / "holzeinschlag_full.json") as f:
        official_data = json.load(f)
    
    results = {
        "pixel_area_ha": PIXEL_AREA_HA,
        "years": list(range(2001, 2024)),  # Hansen covers 2001-2023 (values 1-23)
        "states": {},
        "austria_total": {}
    }
    
    # Aggregate by year for all of Austria
    austria_yearly = {}
    for year_val in range(1, 24):  # 1-23 = 2001-2023
        year = 2000 + year_val
        pixel_count = np.sum(lossyear_data == year_val)
        austria_yearly[year] = {
            "pixels": int(pixel_count),
            "area_ha": float(pixel_count * PIXEL_AREA_HA)
        }
    results["austria_total"] = austria_yearly
    
    # Aggregate by state
    n_states = len(states)
    for i, (state_name, state_geom) in enumerate(states.items()):
        progress = int((i / n_states) * 100)
        update_status("analyze", "aggregate", "running", progress, f"Processing {state_name}...")
        print(f"\nProcessing {state_name}...")
        
        # Create mask for this state
        mask = rasterize_state(state_name, state_geom, lossyear_ds)
        
        # Apply mask to loss data
        state_loss = lossyear_data * mask
        
        # Count pixels by year
        yearly_data = {}
        total_pixels = 0
        for year_val in range(1, 24):
            year = 2000 + year_val
            pixel_count = np.sum(state_loss == year_val)
            total_pixels += pixel_count
            yearly_data[year] = {
                "pixels": int(pixel_count),
                "area_ha": float(pixel_count * PIXEL_AREA_HA)
            }
        
        # Get official harvest data for this state
        official_harvest = official_data["states"].get(state_name, {})
        harvest_2024 = official_harvest.get("harvest_2024", 0)
        
        # Calculate Hansen to Official ratio (proxy for m³/ha)
        total_area_ha = total_pixels * PIXEL_AREA_HA
        if total_area_ha > 0:
            efm_per_ha = harvest_2024 / total_area_ha
        else:
            efm_per_ha = 0
        
        results["states"][state_name] = {
            "yearly": yearly_data,
            "total_pixels": int(total_pixels),
            "total_area_ha": float(total_area_ha),
            "official_harvest_2024": harvest_2024,
            "efm_per_ha_ratio": float(efm_per_ha),  # This is our downscaling factor
        }
        
        print(f"  Total loss pixels: {total_pixels:,}")
        print(f"  Total loss area: {total_area_ha:,.1f} ha")
        print(f"  Official harvest 2024: {harvest_2024:,.0f} Efm")
        print(f"  Implied Efm/ha: {efm_per_ha:,.1f}")
    
    # Save results
    output_path = DATA_DIR / "hansen_state_analysis.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    
    update_status("analyze", "aggregate", "complete", 100, f"Saved to {output_path}")
    print(f"\nResults saved to {output_path}")
    
    return results

def main():
    print("="*60)
    print("Hansen Forest Loss Analysis by Bundesland")
    print("="*60)
    
    results = analyze_forest_loss()
    
    if results:
        print("\n" + "="*60)
        print("Summary by State:")
        print("="*60)
        for state, data in sorted(results["states"].items(), key=lambda x: -x[1]["total_pixels"]):
            print(f"{state}: {data['total_pixels']:,} pixels, {data['total_area_ha']:,.0f} ha loss, {data['efm_per_ha_ratio']:.0f} Efm/ha")

if __name__ == "__main__":
    main()
