#!/usr/bin/env python3
"""
Fast aggregation of Hansen forest loss by Gemeinde (municipality)
Uses gdal_rasterize to create a single labeled raster, then numpy for counting
"""

import json
import subprocess
import numpy as np
from pathlib import Path
from osgeo import gdal
import sys

gdal.UseExceptions()

BASE_DIR = Path(__file__).parent.parent
RASTER_DIR = BASE_DIR / "raster"
DATA_DIR = BASE_DIR / "data"
STATUS_FILE = BASE_DIR / "processing" / "status.json"

PIXEL_SIZE_M = 30.0
PIXEL_AREA_HA = (PIXEL_SIZE_M ** 2) / 10000

# ISO code to state mapping
ISO_TO_STATE = {
    '1': 'Burgenland',
    '2': 'K\u00e4rnten',
    '3': 'Nieder\u00f6sterreich',
    '4': 'Ober\u00f6sterreich',
    '5': 'Salzburg',
    '6': 'Steiermark',
    '7': 'Tirol',
    '8': 'Vorarlberg',
    '9': 'Wien'
}

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

def get_state_from_iso(iso):
    if not iso:
        return None
    first_digit = str(iso)[0]
    return ISO_TO_STATE.get(first_digit)

def create_gemeinde_raster():
    """Create a raster where each pixel has the numeric ISO code of its municipality"""
    update_status("gemeinde", "rasterize", "running", 10, "Creating municipality raster...")
    
    lossyear_path = RASTER_DIR / "austria_lossyear.tif"
    gemeinde_raster_path = RASTER_DIR / "gemeinde_ids.tif"
    geojson_path = DATA_DIR / "austria_gemeinden.geojson"
    
    if gemeinde_raster_path.exists():
        print("Gemeinde raster already exists, skipping...")
        update_status("gemeinde", "rasterize", "complete", 100, "Raster exists")
        return gemeinde_raster_path
    
    # Get extent and resolution from lossyear raster
    ds = gdal.Open(str(lossyear_path))
    gt = ds.GetGeoTransform()
    width = ds.RasterXSize
    height = ds.RasterYSize
    
    xmin = gt[0]
    ymax = gt[3]
    xmax = xmin + width * gt[1]
    ymin = ymax + height * gt[5]  # gt[5] is negative
    
    print(f"Rasterizing municipalities to {width}x{height} raster...")
    print(f"Extent: {xmin}, {ymin}, {xmax}, {ymax}")
    
    # Use gdal_rasterize to burn ISO codes
    cmd = [
        "gdal_rasterize",
        "-a", "iso",  # Use ISO field as value
        "-te", str(xmin), str(ymin), str(xmax), str(ymax),
        "-ts", str(width), str(height),
        "-ot", "Int32",
        "-co", "COMPRESS=LZW",
        str(geojson_path),
        str(gemeinde_raster_path)
    ]
    
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        update_status("gemeinde", "rasterize", "error", 0, result.stderr)
        return None
    
    print(f"Created: {gemeinde_raster_path}")
    update_status("gemeinde", "rasterize", "complete", 100, "Raster created")
    return gemeinde_raster_path

def analyze_gemeinden():
    """Aggregate forest loss using the pre-rasterized municipality layer"""
    update_status("gemeinde", "aggregate", "running", 0, "Loading rasters...")
    
    # Load rasters
    lossyear_path = RASTER_DIR / "austria_lossyear.tif"
    gemeinde_raster_path = RASTER_DIR / "gemeinde_ids.tif"
    
    print("Loading lossyear raster...")
    lossyear_ds = gdal.Open(str(lossyear_path))
    lossyear_data = lossyear_ds.GetRasterBand(1).ReadAsArray()
    
    print("Loading gemeinde raster...")
    gemeinde_ds = gdal.Open(str(gemeinde_raster_path))
    gemeinde_data = gemeinde_ds.GetRasterBand(1).ReadAsArray()
    
    print(f"Raster shapes: lossyear={lossyear_data.shape}, gemeinde={gemeinde_data.shape}")
    
    # Load reference data
    with open(DATA_DIR / "austria_gemeinden.geojson") as f:
        geojson = json.load(f)
    
    with open(DATA_DIR / "hansen_state_analysis.json") as f:
        state_analysis = json.load(f)
    
    with open(DATA_DIR / "timber_values.json") as f:
        timber_values = json.load(f)
    
    weighted_avg_price = timber_values['prices']['weighted_avg_eur_efm']
    
    # Build ISO -> name mapping
    iso_to_name = {}
    for feat in geojson['features']:
        iso = feat['properties'].get('iso')
        name = feat['properties'].get('name')
        if iso:
            iso_to_name[int(iso)] = name
    
    # State pixels for distribution
    state_pixels = {name: data['total_pixels'] for name, data in state_analysis['states'].items()}
    
    update_status("gemeinde", "aggregate", "running", 20, "Counting pixels...")
    
    # Find all unique gemeinde IDs with forest loss
    # Forest loss pixels have lossyear > 0
    loss_mask = lossyear_data > 0
    gemeinde_with_loss = gemeinde_data * loss_mask
    
    # Get unique gemeinde IDs
    unique_ids = np.unique(gemeinde_with_loss)
    unique_ids = unique_ids[unique_ids > 0]  # Remove 0 (no data)
    
    print(f"Found {len(unique_ids)} municipalities with forest loss")
    
    results = {
        "pixel_area_ha": PIXEL_AREA_HA,
        "weighted_avg_price_eur": weighted_avg_price,
        "gemeinden": {}
    }
    
    # Count pixels per gemeinde
    update_status("gemeinde", "aggregate", "running", 40, "Aggregating by municipality...")
    
    n_ids = len(unique_ids)
    for i, gid in enumerate(unique_ids):
        if i % 200 == 0:
            progress = 40 + int((i / n_ids) * 50)
            update_status("gemeinde", "aggregate", "running", progress, f"Processing {i}/{n_ids}...")
            print(f"Progress: {i}/{n_ids}")
        
        gid = int(gid)
        name = iso_to_name.get(gid, f"Unknown-{gid}")
        state = get_state_from_iso(gid)
        
        # Count pixels where this gemeinde has loss
        gemeinde_mask = gemeinde_data == gid
        loss_pixels = int(np.sum(gemeinde_mask & loss_mask))
        
        if loss_pixels == 0:
            continue
        
        loss_area_ha = loss_pixels * PIXEL_AREA_HA
        
        # Distribute state harvest
        state_total_pixels = state_pixels.get(state, 1)
        state_data = timber_values['states'].get(state, {})
        state_harvest = state_data.get('harvest_2024', 0)
        state_value = state_data.get('estimated_value_eur', 0)
        
        if state_total_pixels > 0:
            pixel_ratio = loss_pixels / state_total_pixels
            estimated_harvest = state_harvest * pixel_ratio
            estimated_value = state_value * pixel_ratio
        else:
            estimated_harvest = 0
            estimated_value = 0
        
        efm_per_ha = estimated_harvest / loss_area_ha if loss_area_ha > 0 else 0
        
        results["gemeinden"][str(gid)] = {
            "name": name,
            "state": state,
            "iso": str(gid),
            "loss_pixels": loss_pixels,
            "loss_area_ha": round(loss_area_ha, 2),
            "estimated_harvest_efm": round(estimated_harvest, 0),
            "estimated_value_eur": round(estimated_value, 0),
            "efm_per_ha": round(efm_per_ha, 1)
        }
    
    # Add zero entries for municipalities without loss
    for iso_str, name in iso_to_name.items():
        iso_key = str(iso_str)
        if iso_key not in results["gemeinden"]:
            state = get_state_from_iso(iso_key)
            results["gemeinden"][iso_key] = {
                "name": name,
                "state": state,
                "iso": iso_key,
                "loss_pixels": 0,
                "loss_area_ha": 0,
                "estimated_harvest_efm": 0,
                "estimated_value_eur": 0,
                "efm_per_ha": 0
            }
    
    # Save full results
    output_path = DATA_DIR / "gemeinde_analysis.json"
    with open(output_path, "w") as f:
        json.dump(results, f)
    
    # Create compact map data
    map_data = {}
    for iso, data in results["gemeinden"].items():
        map_data[iso] = {
            "n": data["name"],
            "p": data["loss_pixels"],
            "h": int(data["estimated_harvest_efm"]),
            "v": int(data["estimated_value_eur"]),
            "i": data["efm_per_ha"]
        }
    
    map_output = DATA_DIR / "gemeinde_map.json"
    with open(map_output, "w") as f:
        json.dump(map_data, f)
    
    update_status("gemeinde", "aggregate", "complete", 100, f"Processed {len(results['gemeinden'])} municipalities")
    
    # Summary
    gemeinden_with_loss = [g for g in results["gemeinden"].values() if g["loss_pixels"] > 0]
    print(f"\n" + "="*70)
    print(f"Summary:")
    print(f"  Total municipalities: {len(results['gemeinden'])}")
    print(f"  With forest loss: {len(gemeinden_with_loss)}")
    
    top10 = sorted(gemeinden_with_loss, key=lambda x: -x['estimated_harvest_efm'])[:10]
    print(f"\nTop 10 by estimated harvest:")
    for g in top10:
        print(f"  {g['name']} ({g['state']}): {g['estimated_harvest_efm']:,.0f} Efm, \u20ac{g['estimated_value_eur']:,.0f}")
    
    print(f"\nSaved to: {output_path}")
    print(f"Map data: {map_output}")
    
    return results

def main():
    print("="*70)
    print("Fast Gemeinde Analysis - Hansen Forest Loss")
    print("="*70)
    
    # Step 1: Create rasterized municipality layer
    gemeinde_raster = create_gemeinde_raster()
    if not gemeinde_raster:
        sys.exit(1)
    
    # Step 2: Aggregate
    results = analyze_gemeinden()

if __name__ == "__main__":
    main()
