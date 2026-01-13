#!/usr/bin/env python3
"""
Merge Hansen tiles (west and east) and clip to Austria extent.
This combines the 50N_000E (0-10°E) and 50N_010E (10-20°E) tiles
to provide complete coverage of Austria including Vorarlberg.
"""

import subprocess
from pathlib import Path
import json

BASE_DIR = Path(__file__).parent.parent
RASTER_DIR = BASE_DIR / "raster"
DATA_DIR = BASE_DIR / "data"
STATUS_FILE = BASE_DIR / "processing" / "status.json"

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

def run_cmd(cmd, description):
    print(f"\n{description}...")
    print(f"  Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr}")
        return False
    print(f"  Done")
    return True

def main():
    print("="*70)
    print("Merging Hansen Tiles for Complete Austria Coverage")
    print("="*70)
    
    # Austria bounds with buffer
    # Austria: ~46.4°N-49°N, ~9.5°E-17.2°E
    xmin, ymin, xmax, ymax = 9.5, 46.3, 17.2, 49.1
    
    # Paths
    west_lossyear = RASTER_DIR / "hansen_lossyear_west.tif"  # 0-10°E
    east_lossyear = RASTER_DIR / "hansen_lossyear.tif"       # 10-20°E
    merged_lossyear = RASTER_DIR / "austria_lossyear_merged.tif"
    final_lossyear = RASTER_DIR / "austria_lossyear_new.tif"
    
    west_treecover = RASTER_DIR / "hansen_treecover2000_west.tif"
    east_treecover = RASTER_DIR / "hansen_treecover2000.tif"
    merged_treecover = RASTER_DIR / "austria_treecover2000_merged.tif"
    final_treecover = RASTER_DIR / "austria_treecover2000_new.tif"
    
    # Step 1: Merge lossyear tiles
    update_status("merge", "lossyear", "running", 10, "Building VRT...")
    print("\n--- Merging Lossyear Tiles ---")
    
    # Create VRT (virtual raster) from both tiles
    vrt_lossyear = RASTER_DIR / "temp_lossyear.vrt"
    cmd = ["gdalbuildvrt", str(vrt_lossyear), str(west_lossyear), str(east_lossyear)]
    if not run_cmd(cmd, "Building lossyear VRT"):
        return
    
    # Clip to Austria extent
    update_status("merge", "lossyear", "running", 30, "Clipping to Austria...")
    cmd = [
        "gdalwarp",
        "-te", str(xmin), str(ymin), str(xmax), str(ymax),
        "-co", "COMPRESS=LZW",
        "-co", "BIGTIFF=YES",
        str(vrt_lossyear),
        str(merged_lossyear)
    ]
    if not run_cmd(cmd, "Clipping lossyear to Austria"):
        return
    
    update_status("merge", "lossyear", "complete", 50, "Lossyear merged")
    
    # Step 2: Merge treecover tiles
    update_status("merge", "treecover", "running", 60, "Building VRT...")
    print("\n--- Merging Treecover Tiles ---")
    
    vrt_treecover = RASTER_DIR / "temp_treecover.vrt"
    cmd = ["gdalbuildvrt", str(vrt_treecover), str(west_treecover), str(east_treecover)]
    if not run_cmd(cmd, "Building treecover VRT"):
        return
    
    update_status("merge", "treecover", "running", 80, "Clipping to Austria...")
    cmd = [
        "gdalwarp",
        "-te", str(xmin), str(ymin), str(xmax), str(ymax),
        "-co", "COMPRESS=LZW",
        "-co", "BIGTIFF=YES",
        str(vrt_treecover),
        str(merged_treecover)
    ]
    if not run_cmd(cmd, "Clipping treecover to Austria"):
        return
    
    update_status("merge", "treecover", "complete", 100, "Treecover merged")
    
    # Cleanup VRT files
    vrt_lossyear.unlink(missing_ok=True)
    vrt_treecover.unlink(missing_ok=True)
    
    print("\n" + "="*70)
    print("Merge complete!")
    print(f"  Lossyear: {merged_lossyear}")
    print(f"  Treecover: {merged_treecover}")
    print("="*70)
    
    # Show info
    subprocess.run(["gdalinfo", "-mm", str(merged_lossyear)])

if __name__ == "__main__":
    main()
