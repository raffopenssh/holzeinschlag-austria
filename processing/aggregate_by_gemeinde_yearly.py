#!/usr/bin/env python3
"""
Aggregate Hansen forest loss by Gemeinde (municipality) AND year.
Calculates actual forest loss area from raster pixel counts.

Hansen lossyear encoding: 0=no loss, 1=2001, 2=2002, ..., 23=2023

Highly optimized version using numpy unique for aggregation.
"""

import json
import numpy as np
from pathlib import Path
from osgeo import gdal
import sys
from collections import defaultdict

gdal.UseExceptions()

BASE_DIR = Path(__file__).parent.parent
RASTER_DIR = BASE_DIR / "raster"
DATA_DIR = BASE_DIR / "data"
STATUS_FILE = BASE_DIR / "processing" / "status.json"

PIXEL_SIZE_M = 30.0
PIXEL_AREA_HA = (PIXEL_SIZE_M ** 2) / 10000  # ~0.09 ha per pixel

# ISO code first digit to state mapping
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

def main():
    print("="*70)
    print("Gemeinde Yearly Forest Loss Analysis (Optimized)")
    print("Calculating actual forest loss area per municipality and year")
    print("="*70)
    
    update_status("gemeinde_yearly", "aggregate", "running", 0, "Loading rasters...")
    
    # Load rasters
    lossyear_path = RASTER_DIR / "austria_lossyear.tif"
    gemeinde_raster_path = RASTER_DIR / "gemeinde_ids.tif"
    
    if not gemeinde_raster_path.exists():
        print("ERROR: gemeinde_ids.tif not found. Run aggregate_by_gemeinde_fast.py first.")
        update_status("gemeinde_yearly", "aggregate", "error", 0, "Municipality raster not found")
        sys.exit(1)
    
    print("Loading lossyear raster...")
    lossyear_ds = gdal.Open(str(lossyear_path))
    lossyear_data = lossyear_ds.GetRasterBand(1).ReadAsArray().flatten()
    
    print("Loading gemeinde raster...")
    gemeinde_ds = gdal.Open(str(gemeinde_raster_path))
    gemeinde_data = gemeinde_ds.GetRasterBand(1).ReadAsArray().flatten()
    
    print(f"Total pixels: {len(lossyear_data):,}")
    print(f"Pixel area: {PIXEL_AREA_HA:.4f} ha")
    
    # Load municipality reference data
    with open(DATA_DIR / "austria_gemeinden.geojson") as f:
        geojson = json.load(f)
    
    # Build ISO -> name mapping
    iso_to_name = {}
    for feat in geojson['features']:
        iso = feat['properties'].get('iso')
        name = feat['properties'].get('name')
        if iso:
            iso_to_name[int(iso)] = name
    
    print(f"Loaded {len(iso_to_name)} municipalities")
    
    update_status("gemeinde_yearly", "aggregate", "running", 20, "Creating combined keys...")
    
    # Years: Hansen uses 1-23 for 2001-2023
    years = list(range(2001, 2025))  # 2001-2024
    
    # Only process pixels with loss (lossyear > 0) and valid gemeinde (> 0)
    valid_mask = (lossyear_data > 0) & (gemeinde_data > 0)
    
    valid_gemeinde = gemeinde_data[valid_mask]
    valid_lossyear = lossyear_data[valid_mask]
    
    print(f"Pixels with loss in valid municipalities: {len(valid_gemeinde):,}")
    
    update_status("gemeinde_yearly", "aggregate", "running", 40, "Aggregating with numpy...")
    
    # Create combined key: gemeinde_id * 100 + year_value
    # Since max year_value is 23 and we multiply gemeinde by 100, keys are unique
    combined_keys = valid_gemeinde.astype(np.int64) * 100 + valid_lossyear.astype(np.int64)
    
    # Use numpy unique to count occurrences of each (gemeinde, year) combination
    unique_keys, counts = np.unique(combined_keys, return_counts=True)
    
    print(f"Unique (gemeente, year) combinations: {len(unique_keys):,}")
    
    update_status("gemeinde_yearly", "aggregate", "running", 70, "Building results dictionary...")
    
    # Build counts dictionary
    # Key format: gemeinde_id * 100 + year_val -> pixel_count
    counts_dict = defaultdict(lambda: defaultdict(int))
    year_totals = defaultdict(int)
    
    for key, count in zip(unique_keys, counts):
        gemeinde_id = int(key // 100)
        year_val = int(key % 100)
        year = 2000 + year_val
        counts_dict[gemeinde_id][year] = int(count)
        year_totals[year] += int(count)
    
    # Build results
    update_status("gemeinde_yearly", "aggregate", "running", 80, "Building final results...")
    
    results = {
        "description": "Forest loss area per municipality and year (calculated from Hansen raster)",
        "pixel_area_ha": PIXEL_AREA_HA,
        "years": years,
        "summary": {},
        "gemeinden": {}
    }
    
    # Yearly totals
    print("\nYearly totals:")
    for year in years:
        pixel_count = year_totals.get(year, 0)
        area_ha = round(pixel_count * PIXEL_AREA_HA, 2)
        results["summary"][str(year)] = {
            "total_pixels": pixel_count,
            "total_area_ha": area_ha
        }
        print(f"  {year}: {pixel_count:>10,} pixels, {area_ha:>12,.1f} ha")
    
    # Build gemeente results
    for iso_int, name in iso_to_name.items():
        iso_str = str(iso_int)
        state = get_state_from_iso(iso_str)
        
        yearly_data = {}
        total_pixels = 0
        
        if iso_int in counts_dict:
            for year, pixels in counts_dict[iso_int].items():
                area_ha = round(pixels * PIXEL_AREA_HA, 2)
                yearly_data[str(year)] = {
                    "pixels": pixels,
                    "area_ha": area_ha
                }
                total_pixels += pixels
        
        results["gemeinden"][iso_str] = {
            "name": name,
            "state": state,
            "iso": iso_str,
            "total_pixels": total_pixels,
            "total_area_ha": round(total_pixels * PIXEL_AREA_HA, 2),
            "years": yearly_data
        }
    
    # Save full results
    update_status("gemeinde_yearly", "aggregate", "running", 90, "Saving results...")
    
    output_path = DATA_DIR / "gemeinde_yearly_loss.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\nSaved full results to: {output_path}")
    
    # Create compact map data format
    update_status("gemeinde_yearly", "aggregate", "running", 95, "Creating compact map data...")
    
    map_data = {
        "years": years,
        "pixel_area_ha": PIXEL_AREA_HA,
        "by_year": {}
    }
    
    for year in years:
        year_str = str(year)
        year_data = {}
        for iso, gemeente in results["gemeinden"].items():
            if year_str in gemeente["years"]:
                yd = gemeente["years"][year_str]
                year_data[iso] = {
                    "n": gemeente["name"],
                    "p": yd["pixels"],
                    "a": yd["area_ha"]
                }
            else:
                year_data[iso] = {
                    "n": gemeente["name"],
                    "p": 0,
                    "a": 0
                }
        map_data["by_year"][year_str] = year_data
    
    map_output = DATA_DIR / "gemeinde_yearly_map.json"
    with open(map_output, "w") as f:
        json.dump(map_data, f)
    
    print(f"Saved map data to: {map_output}")
    
    update_status("gemeinde_yearly", "aggregate", "complete", 100, 
                  f"Processed {len(results['gemeinden'])} municipalities, {len(years)} years")
    
    # Summary
    gemeinden_with_loss = [g for g in results["gemeinden"].values() if g["total_pixels"] > 0]
    total_pixels_all = sum(g["total_pixels"] for g in results["gemeinden"].values())
    total_area_all = sum(g["total_area_ha"] for g in results["gemeinden"].values())
    
    print("\n" + "="*70)
    print("Summary:")
    print(f"  Total municipalities: {len(results['gemeinden'])}")
    print(f"  With forest loss: {len(gemeinden_with_loss)}")
    print(f"  Total pixels: {total_pixels_all:,}")
    print(f"  Total forest loss area: {total_area_all:,.1f} ha")
    print("="*70)
    
    # Top 10 by total area
    top10 = sorted(gemeinden_with_loss, key=lambda x: -x['total_area_ha'])[:10]
    print("\nTop 10 municipalities by total forest loss (2001-2023):")
    for g in top10:
        print(f"  {g['name']} ({g['state']}): {g['total_area_ha']:,.1f} ha")

if __name__ == "__main__":
    main()
