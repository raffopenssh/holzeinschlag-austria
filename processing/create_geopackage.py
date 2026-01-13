#!/usr/bin/env python3
"""
Create GeoPackage with all municipality data for all years.
Combines GeoJSON boundaries with emissions/harvest data.
"""

import json
import subprocess
from pathlib import Path
import tempfile
import shutil

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
PUBLIC_DIR = BASE_DIR / "public"

def main():
    print("Creating GeoPackage export...")
    
    # Load base GeoJSON
    with open(DATA_DIR / "austria_gemeinden.geojson") as f:
        geojson = json.load(f)
    
    # Load metadata
    with open(DATA_DIR / "emissions_meta.json") as f:
        meta = json.load(f)
    
    # Load lookup
    with open(DATA_DIR / "gemeinde_lookup.json") as f:
        lookup = json.load(f)
    
    # Load all year data
    years = meta['years']
    year_data = {}
    for year in years:
        with open(DATA_DIR / f"year_{year}.json") as f:
            year_data[year] = json.load(f)
    
    # Enrich GeoJSON features with all year data
    for feature in geojson['features']:
        iso = str(feature['properties'].get('iso', ''))
        
        # Add lookup data
        feature['properties']['name'] = lookup['names'].get(iso, '')
        feature['properties']['state'] = lookup['states'].get(iso, '')
        feature['properties']['population'] = lookup['population'].get(iso, 0)
        
        # Add data for each year
        for year in years:
            d = year_data[year].get(iso, [0, 0, 0, 0, 0, 0, 0])
            if isinstance(d, list) and len(d) >= 7:
                feature['properties'][f'loss_pixels_{year}'] = d[0]
                feature['properties'][f'loss_area_ha_{year}'] = d[1]
                feature['properties'][f'harvest_efm_{year}'] = d[2]
                feature['properties'][f'value_eur_{year}'] = d[3]
                feature['properties'][f'co2_tonnes_{year}'] = d[4]
                feature['properties'][f'ets_eur_{year}'] = d[5]
                feature['properties'][f'ets_per_capita_{year}'] = d[6]
            else:
                feature['properties'][f'loss_pixels_{year}'] = 0
                feature['properties'][f'loss_area_ha_{year}'] = 0
                feature['properties'][f'harvest_efm_{year}'] = 0
                feature['properties'][f'value_eur_{year}'] = 0
                feature['properties'][f'co2_tonnes_{year}'] = 0
                feature['properties'][f'ets_eur_{year}'] = 0
                feature['properties'][f'ets_per_capita_{year}'] = 0
    
    # Write enriched GeoJSON to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as f:
        json.dump(geojson, f)
        temp_geojson = f.name
    
    # Output path
    output_gpkg = PUBLIC_DIR / "holzeinschlag_austria.gpkg"
    
    # Remove existing file
    if output_gpkg.exists():
        output_gpkg.unlink()
    
    # Convert to GeoPackage
    cmd = [
        'ogr2ogr',
        '-f', 'GPKG',
        str(output_gpkg),
        temp_geojson,
        '-nln', 'gemeinden'
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return
    
    # Cleanup
    Path(temp_geojson).unlink()
    
    # File size
    size_mb = output_gpkg.stat().st_size / 1024 / 1024
    print(f"Created: {output_gpkg}")
    print(f"Size: {size_mb:.1f} MB")
    print(f"Features: {len(geojson['features'])}")
    print(f"Years: {years[0]} - {years[-1]}")

if __name__ == "__main__":
    main()
