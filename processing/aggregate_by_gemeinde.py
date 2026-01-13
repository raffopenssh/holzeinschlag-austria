#!/usr/bin/env python3
"""
Aggregate Hansen forest loss by Gemeinde (municipality)
Distribute state-level harvest data proportionally based on Hansen loss pixels
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

# Hansen pixel resolution
PIXEL_SIZE_M = 30.0
PIXEL_AREA_HA = (PIXEL_SIZE_M ** 2) / 10000

# ISO code to state mapping (first digit of ISO code)
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
    """Get state name from ISO code (first digit)"""
    if not iso:
        return None
    first_digit = str(iso)[0]
    return ISO_TO_STATE.get(first_digit)

def rasterize_geometry(geom, raster_ds):
    """Create a binary mask for a geometry matching the raster dimensions"""
    gt = raster_ds.GetGeoTransform()
    width = raster_ds.RasterXSize
    height = raster_ds.RasterYSize
    proj = raster_ds.GetProjection()
    
    driver = gdal.GetDriverByName('MEM')
    mask_ds = driver.Create('', width, height, 1, gdal.GDT_Byte)
    mask_ds.SetGeoTransform(gt)
    mask_ds.SetProjection(proj)
    
    mem_driver = ogr.GetDriverByName('Memory')
    mem_ds = mem_driver.CreateDataSource('')
    srs = osr.SpatialReference()
    srs.ImportFromWkt(proj)
    mem_layer = mem_ds.CreateLayer('geom', srs, ogr.wkbPolygon)
    
    feat = ogr.Feature(mem_layer.GetLayerDefn())
    feat.SetGeometry(geom)
    mem_layer.CreateFeature(feat)
    
    gdal.RasterizeLayer(mask_ds, [1], mem_layer, burn_values=[1])
    
    mask = mask_ds.GetRasterBand(1).ReadAsArray()
    return mask

def analyze_gemeinden():
    """Main analysis: aggregate forest loss by municipality"""
    update_status("gemeinde", "aggregate", "running", 0, "Loading data...")
    
    # Load clipped lossyear raster
    lossyear_path = RASTER_DIR / "austria_lossyear.tif"
    if not lossyear_path.exists():
        print(f"Error: {lossyear_path} not found")
        update_status("gemeinde", "aggregate", "error", 0, "Clipped raster not found")
        return None
    
    lossyear_ds = gdal.Open(str(lossyear_path))
    lossyear_data = lossyear_ds.GetRasterBand(1).ReadAsArray()
    
    print(f"Raster size: {lossyear_data.shape}")
    
    # Load state analysis for harvest distribution
    with open(DATA_DIR / "hansen_state_analysis.json") as f:
        state_analysis = json.load(f)
    
    # Load timber values for economic data
    with open(DATA_DIR / "timber_values.json") as f:
        timber_values = json.load(f)
    
    weighted_avg_price = timber_values['prices']['weighted_avg_eur_efm']
    
    # Load municipality boundaries
    geojson_path = DATA_DIR / "austria_gemeinden.geojson"
    ds = ogr.Open(str(geojson_path))
    layer = ds.GetLayer()
    
    n_features = layer.GetFeatureCount()
    print(f"Processing {n_features} municipalities...")
    
    # Aggregate state pixels for distribution ratios
    state_pixels = {}
    for state_name, state_data in state_analysis['states'].items():
        state_pixels[state_name] = state_data['total_pixels']
    
    results = {
        "pixel_area_ha": PIXEL_AREA_HA,
        "weighted_avg_price_eur": weighted_avg_price,
        "gemeinden": {}
    }
    
    # Process each municipality
    for i, feature in enumerate(layer):
        if i % 100 == 0:
            progress = int((i / n_features) * 100)
            update_status("gemeinde", "aggregate", "running", progress, f"Processing {i}/{n_features}...")
            print(f"Progress: {i}/{n_features} ({progress}%)")
        
        name = feature.GetField("name")
        iso = feature.GetField("iso")
        state = get_state_from_iso(iso)
        geom = feature.GetGeometryRef()
        
        if not geom or not state:
            continue
        
        # Create mask for this municipality
        try:
            mask = rasterize_geometry(geom, lossyear_ds)
        except Exception as e:
            print(f"  Error rasterizing {name}: {e}")
            continue
        
        # Apply mask to loss data
        gemeinde_loss = lossyear_data * mask
        
        # Count total loss pixels
        total_pixels = int(np.sum(gemeinde_loss > 0))
        
        if total_pixels == 0:
            # No forest loss detected - skip or record as zero
            results["gemeinden"][iso] = {
                "name": name,
                "state": state,
                "iso": iso,
                "loss_pixels": 0,
                "loss_area_ha": 0,
                "estimated_harvest_efm": 0,
                "estimated_value_eur": 0,
                "efm_per_ha": 0
            }
            continue
        
        # Calculate loss area
        loss_area_ha = total_pixels * PIXEL_AREA_HA
        
        # Get state totals for distribution
        state_total_pixels = state_pixels.get(state, 1)
        state_data = timber_values['states'].get(state, {})
        state_harvest = state_data.get('harvest_2024', 0)
        state_value = state_data.get('estimated_value_eur', 0)
        
        # Distribute state harvest proportionally by Hansen pixels
        if state_total_pixels > 0:
            pixel_ratio = total_pixels / state_total_pixels
            estimated_harvest = state_harvest * pixel_ratio
            estimated_value = state_value * pixel_ratio
        else:
            estimated_harvest = 0
            estimated_value = 0
        
        # Calculate intensity metrics
        efm_per_ha = estimated_harvest / loss_area_ha if loss_area_ha > 0 else 0
        
        results["gemeinden"][iso] = {
            "name": name,
            "state": state,
            "iso": iso,
            "loss_pixels": total_pixels,
            "loss_area_ha": round(loss_area_ha, 2),
            "estimated_harvest_efm": round(estimated_harvest, 0),
            "estimated_value_eur": round(estimated_value, 0),
            "efm_per_ha": round(efm_per_ha, 1)
        }
    
    # Save results
    output_path = DATA_DIR / "gemeinde_analysis.json"
    with open(output_path, "w") as f:
        json.dump(results, f)
    
    # Also create a smaller summary for the map (just key metrics)
    map_data = {}
    for iso, data in results["gemeinden"].items():
        map_data[iso] = {
            "n": data["name"],
            "p": data["loss_pixels"],
            "h": round(data["estimated_harvest_efm"]),
            "v": round(data["estimated_value_eur"]),
            "i": round(data["efm_per_ha"], 1)
        }
    
    map_output = DATA_DIR / "gemeinde_map.json"
    with open(map_output, "w") as f:
        json.dump(map_data, f)
    
    update_status("gemeinde", "aggregate", "complete", 100, f"Processed {n_features} municipalities")
    print(f"\nResults saved to {output_path}")
    print(f"Map data saved to {map_output}")
    
    # Print summary
    gemeinden_with_loss = [g for g in results["gemeinden"].values() if g["loss_pixels"] > 0]
    print(f"\nSummary:")
    print(f"  Total municipalities: {len(results['gemeinden'])}")
    print(f"  With forest loss: {len(gemeinden_with_loss)}")
    
    # Top 10 by harvest
    top10 = sorted(gemeinden_with_loss, key=lambda x: -x['estimated_harvest_efm'])[:10]
    print(f"\nTop 10 municipalities by estimated harvest:")
    for g in top10:
        print(f"  {g['name']} ({g['state']}): {g['estimated_harvest_efm']:,.0f} Efm, \u20ac{g['estimated_value_eur']:,.0f}")
    
    return results

def main():
    print("="*70)
    print("Hansen Forest Loss Analysis by Gemeinde (Municipality)")
    print("="*70)
    
    results = analyze_gemeinden()

if __name__ == "__main__":
    main()
