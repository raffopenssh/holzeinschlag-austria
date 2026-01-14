#!/usr/bin/env python3
"""
Background job to calculate carbon flux per municipality.

This job aggregates WRI/GFW carbon flux raster data by Austrian municipality
to create more realistic ETS calculations based on actual carbon dynamics.

The carbon flux data includes:
- Gross emissions (from forest loss)
- Gross removals (from forest growth)
- Net flux (emissions - removals)

All values are cumulative for 2001-2024 in Mg CO2e/ha.

Usage:
    python carbon_flux_job.py [--status] [--reset]
"""

import json
import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import numpy as np
from osgeo import gdal
gdal.UseExceptions()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/exedev/holzeinschlag-austria/processing/carbon_flux.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RASTER_DIR = BASE_DIR / "raster"
CARBON_DIR = RASTER_DIR / "carbon_flux"
STATUS_FILE = DATA_DIR / "carbon_flux_status.json"

def load_status():
    """Load or initialize job status."""
    if STATUS_FILE.exists():
        with open(STATUS_FILE) as f:
            return json.load(f)
    return {
        "status": "not_started",
        "started_at": None,
        "updated_at": None,
        "progress_pct": 0,
        "current_step": None,
        "gemeinden_processed": 0,
        "total_gemeinden": 0,
        "errors": [],
        "results_file": None
    }

def save_status(status):
    """Save job status."""
    status["updated_at"] = datetime.now().isoformat()
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f, indent=2)

def check_dependencies():
    """Check if required files exist."""
    required = [
        CARBON_DIR / "net_flux_50N_010E.tif",
        CARBON_DIR / "gross_emissions_50N_010E.tif",
        CARBON_DIR / "gross_removals_50N_010E.tif",
        RASTER_DIR / "gemeinde_ids.tif",
        DATA_DIR / "gemeinde_lookup.json"
    ]
    
    missing = [str(f) for f in required if not f.exists()]
    if missing:
        logger.error(f"Missing required files: {missing}")
        return False
    return True

def aggregate_carbon_flux():
    """Main aggregation function using windowed reading for efficiency."""
    
    status = load_status()
    status["status"] = "running"
    status["started_at"] = datetime.now().isoformat()
    status["current_step"] = "loading_rasters"
    save_status(status)
    
    logger.info("Loading raster metadata...")
    
    # Open gemeinde IDs raster
    gemeinde_ds = gdal.Open(str(RASTER_DIR / "gemeinde_ids.tif"))
    gemeinde_gt = gemeinde_ds.GetGeoTransform()
    gem_cols = gemeinde_ds.RasterXSize
    gem_rows = gemeinde_ds.RasterYSize
    
    # Gemeinde extent
    gem_xmin = gemeinde_gt[0]
    gem_ymax = gemeinde_gt[3]
    gem_xres = gemeinde_gt[1]
    gem_yres = gemeinde_gt[5]  # negative
    
    gem_xmax = gem_xmin + gem_cols * gem_xres
    gem_ymin = gem_ymax + gem_rows * gem_yres
    
    logger.info(f"Gemeinde raster: {gem_cols}x{gem_rows} pixels")
    logger.info(f"Gemeinde extent: {gem_xmin:.4f}-{gem_xmax:.4f}E, {gem_ymin:.4f}-{gem_ymax:.4f}N")
    
    # Open carbon flux rasters
    net_flux_ds = gdal.Open(str(CARBON_DIR / "net_flux_50N_010E.tif"))
    gross_emis_ds = gdal.Open(str(CARBON_DIR / "gross_emissions_50N_010E.tif"))
    gross_rem_ds = gdal.Open(str(CARBON_DIR / "gross_removals_50N_010E.tif"))
    
    flux_gt = net_flux_ds.GetGeoTransform()
    flux_xmin = flux_gt[0]
    flux_ymax = flux_gt[3]
    flux_xres = flux_gt[1]
    flux_yres = flux_gt[5]
    
    logger.info(f"Flux raster extent: {flux_xmin}-{flux_xmin+10}E, {flux_ymax-10}-{flux_ymax}N")
    
    # Get no-data values
    net_flux_nodata = net_flux_ds.GetRasterBand(1).GetNoDataValue()
    gross_emis_nodata = gross_emis_ds.GetRasterBand(1).GetNoDataValue()
    gross_rem_nodata = gross_rem_ds.GetRasterBand(1).GetNoDataValue()
    
    # Load gemeente lookup for names
    with open(DATA_DIR / "gemeinde_lookup.json") as f:
        lookup = json.load(f)
    
    status["current_step"] = "processing_blocks"
    save_status(status)
    
    # Process in blocks for memory efficiency
    block_size = 2000
    
    # Accumulators for each gemeinde
    # net_flux, gross_emissions, gross_removals, count
    gemeinde_data = defaultdict(lambda: {"net": 0.0, "emis": 0.0, "rem": 0.0, "count": 0})
    
    total_blocks = ((gem_rows + block_size - 1) // block_size) * ((gem_cols + block_size - 1) // block_size)
    processed_blocks = 0
    
    logger.info(f"Processing {total_blocks} blocks...")
    
    for row_start in range(0, gem_rows, block_size):
        row_end = min(row_start + block_size, gem_rows)
        rows_to_read = row_end - row_start
        
        for col_start in range(0, gem_cols, block_size):
            col_end = min(col_start + block_size, gem_cols)
            cols_to_read = col_end - col_start
            
            # Read block of gemeinde IDs
            gem_block = gemeinde_ds.GetRasterBand(1).ReadAsArray(
                col_start, row_start, cols_to_read, rows_to_read
            )
            
            # Calculate the geographic extent of this block
            block_xmin = gem_xmin + col_start * gem_xres
            block_ymax = gem_ymax + row_start * gem_yres
            
            # Calculate corresponding flux raster coordinates
            flux_col_start = int((block_xmin - flux_xmin) / flux_xres)
            flux_row_start = int((flux_ymax - block_ymax) / (-flux_yres))
            
            # Read corresponding flux blocks
            # Flux raster has same resolution, so same size
            if (flux_col_start >= 0 and flux_col_start + cols_to_read <= net_flux_ds.RasterXSize and
                flux_row_start >= 0 and flux_row_start + rows_to_read <= net_flux_ds.RasterYSize):
                
                net_block = net_flux_ds.GetRasterBand(1).ReadAsArray(
                    flux_col_start, flux_row_start, cols_to_read, rows_to_read
                )
                emis_block = gross_emis_ds.GetRasterBand(1).ReadAsArray(
                    flux_col_start, flux_row_start, cols_to_read, rows_to_read
                )
                rem_block = gross_rem_ds.GetRasterBand(1).ReadAsArray(
                    flux_col_start, flux_row_start, cols_to_read, rows_to_read
                )
                
                # Create masks for valid data
                gem_valid = gem_block > 0
                net_valid = ~np.isnan(net_block) if net_flux_nodata is None else (net_block != net_flux_nodata) & ~np.isnan(net_block)
                emis_valid = ~np.isnan(emis_block) if gross_emis_nodata is None else (emis_block != gross_emis_nodata) & ~np.isnan(emis_block)
                rem_valid = ~np.isnan(rem_block) if gross_rem_nodata is None else (rem_block != gross_rem_nodata) & ~np.isnan(rem_block)
                
                # Aggregate by gemeinde ID
                for gem_id in np.unique(gem_block[gem_valid]):
                    if gem_id <= 0:
                        continue
                    
                    mask = gem_block == gem_id
                    
                    # Net flux
                    net_mask = mask & net_valid
                    if np.any(net_mask):
                        gemeinde_data[int(gem_id)]["net"] += float(np.sum(net_block[net_mask]))
                        gemeinde_data[int(gem_id)]["count"] += int(np.sum(net_mask))
                    
                    # Gross emissions
                    emis_mask = mask & emis_valid
                    if np.any(emis_mask):
                        gemeinde_data[int(gem_id)]["emis"] += float(np.sum(emis_block[emis_mask]))
                    
                    # Gross removals
                    rem_mask = mask & rem_valid
                    if np.any(rem_mask):
                        gemeinde_data[int(gem_id)]["rem"] += float(np.sum(rem_block[rem_mask]))
            
            processed_blocks += 1
            
            # Update progress
            if processed_blocks % 10 == 0:
                progress = int((processed_blocks / total_blocks) * 100)
                status["progress_pct"] = progress
                status["gemeinden_processed"] = len(gemeinde_data)
                save_status(status)
                
                if processed_blocks % 50 == 0:
                    logger.info(f"Progress: {progress}% ({len(gemeinde_data)} gemeinden)")
    
    # Calculate final results
    logger.info("Calculating final results...")
    
    # Pixel area: 0.00025° ≈ 27.8m at 47°N
    # Area = cos(47°) * (111km/degree)² * 0.00025² ≈ 0.0576 km² ≈ 5.76 ha per pixel
    # Wait, that's wrong. Let me recalculate:
    # 0.00025° longitude at 47°N ≈ 0.00025 * 111 * cos(47°) km ≈ 0.019 km ≈ 19m
    # 0.00025° latitude ≈ 0.00025 * 111 km ≈ 0.028 km ≈ 28m  
    # Pixel area ≈ 19 * 28 m² ≈ 532 m² ≈ 0.053 ha
    # This is close to 30m resolution (0.09 ha)
    # Using WRI's standard: ~0.09 ha per pixel for 30m
    pixel_area_ha = 0.053  # More accurate for Austria's latitude
    
    results = {}
    for gem_id, data in gemeinde_data.items():
        iso = str(gem_id)
        count = data["count"]
        
        if count == 0:
            continue
        
        # Total area with flux data
        area_ha = count * pixel_area_ha
        
        # Net flux: sum of per-hectare values * pixel area = total Mg CO2e
        # The raster values are Mg CO2e/ha, so:
        # Total = sum(value_per_ha * pixel_area) = sum(values) * pixel_area
        net_flux_total = data["net"] * pixel_area_ha
        gross_emis_total = data["emis"] * pixel_area_ha
        gross_rem_total = data["rem"] * pixel_area_ha
        
        # Per-hectare averages
        net_flux_per_ha = data["net"] / count if count > 0 else 0
        gross_emis_per_ha = data["emis"] / count if count > 0 else 0
        gross_rem_per_ha = data["rem"] / count if count > 0 else 0
        
        results[iso] = {
            "name": lookup.get("names", {}).get(iso, f"Gemeinde_{iso}"),
            "state": lookup.get("states", {}).get(iso, ""),
            "forest_pixels": count,
            "forest_area_ha": round(area_ha, 2),
            # Cumulative 2001-2024 values in tonnes (Mg) CO2e
            "net_flux_tonnes": round(net_flux_total, 2),
            "gross_emissions_tonnes": round(gross_emis_total, 2),
            "gross_removals_tonnes": round(gross_rem_total, 2),
            # Per-hectare values
            "net_flux_per_ha": round(net_flux_per_ha, 4),
            "gross_emissions_per_ha": round(gross_emis_per_ha, 4),
            "gross_removals_per_ha": round(gross_rem_per_ha, 4),
            # Is this municipality a net source or sink?
            "is_net_source": net_flux_total > 0
        }
    
    # Calculate Austria totals
    austria_totals = {
        "total_forest_area_ha": sum(r["forest_area_ha"] for r in results.values()),
        "total_net_flux_tonnes": sum(r["net_flux_tonnes"] for r in results.values()),
        "total_gross_emissions_tonnes": sum(r["gross_emissions_tonnes"] for r in results.values()),
        "total_gross_removals_tonnes": sum(r["gross_removals_tonnes"] for r in results.values()),
        "net_source_count": sum(1 for r in results.values() if r["is_net_source"]),
        "net_sink_count": sum(1 for r in results.values() if not r["is_net_source"]),
    }
    
    logger.info(f"Austria totals: {austria_totals['total_net_flux_tonnes']:,.0f} tonnes net flux")
    logger.info(f"  Gross emissions: {austria_totals['total_gross_emissions_tonnes']:,.0f} tonnes")
    logger.info(f"  Gross removals: {austria_totals['total_gross_removals_tonnes']:,.0f} tonnes")
    logger.info(f"  Net sources: {austria_totals['net_source_count']} gemeinden")
    logger.info(f"  Net sinks: {austria_totals['net_sink_count']} gemeinden")
    
    # Save results
    output_file = DATA_DIR / "carbon_flux_by_gemeinde.json"
    output_data = {
        "description": "Net carbon flux per municipality, cumulative 2001-2024",
        "source": "WRI/GFW Global Forest Carbon Flux (Harris et al. 2021, Gibbs et al. 2025)",
        "data_version": "v20250430",
        "units": {
            "net_flux_tonnes": "Tonnes (Mg) CO2e total for municipality (2001-2024)",
            "gross_emissions_tonnes": "Tonnes CO2e emitted from forest loss",
            "gross_removals_tonnes": "Tonnes CO2e removed by forest growth",
            "per_ha": "Average tonnes CO2e per hectare of forest",
            "positive_flux": "Net source (emissions > removals)",
            "negative_flux": "Net sink (removals > emissions)"
        },
        "methodology": "Aggregated 30m WRI carbon flux rasters by municipality boundary",
        "generated_at": datetime.now().isoformat(),
        "austria_totals": austria_totals,
        "gemeinden": results
    }
    
    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2)
    
    logger.info(f"Results saved to {output_file}")
    
    # Update status
    status["status"] = "completed"
    status["progress_pct"] = 100
    status["results_file"] = str(output_file)
    status["gemeinden_processed"] = len(results)
    save_status(status)
    
    return results

def main():
    if "--status" in sys.argv:
        status = load_status()
        print(json.dumps(status, indent=2))
        return
    
    if "--reset" in sys.argv:
        status = load_status()
        status["status"] = "not_started"
        status["progress_pct"] = 0
        status["errors"] = []
        save_status(status)
        print("Status reset")
        return
    
    if not check_dependencies():
        sys.exit(1)
    
    logger.info("Starting carbon flux aggregation job...")
    start_time = time.time()
    
    try:
        results = aggregate_carbon_flux()
        if results:
            logger.info(f"Job completed. Processed {len(results)} gemeinden")
            logger.info(f"Total time: {time.time() - start_time:.1f} seconds")
        else:
            logger.error("Job failed - no results")
    except Exception as e:
        logger.exception(f"Job failed with error: {e}")
        status = load_status()
        status["status"] = "failed"
        status["errors"].append(str(e))
        save_status(status)
        sys.exit(1)

if __name__ == "__main__":
    main()
