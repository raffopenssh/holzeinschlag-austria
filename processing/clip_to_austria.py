#!/usr/bin/env python3
"""
Phase B Task 3: Clip Hansen rasters to Austria boundary
Uses GDAL to clip the large Hansen tiles to Austria extent
"""

import subprocess
import json
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
RASTER_DIR = BASE_DIR / "raster"
DATA_DIR = BASE_DIR / "data"
STATUS_FILE = BASE_DIR / "processing" / "status.json"

# Austria bounding box (with small buffer)
AUSTRIA_BBOX = "9.5 46.3 17.2 49.1"  # minX minY maxX maxY

def update_status(phase, task, status, progress=0, message=""):
    """Update processing status file"""
    status_data = {}
    if STATUS_FILE.exists():
        with open(STATUS_FILE) as f:
            status_data = json.load(f)
    
    if phase not in status_data:
        status_data[phase] = {}
    
    status_data[phase][task] = {
        "status": status,
        "progress": progress,
        "message": message
    }
    
    with open(STATUS_FILE, "w") as f:
        json.dump(status_data, f, indent=2)

def clip_raster(input_path, output_path, task_name):
    """Clip raster to Austria boundary using gdalwarp"""
    print(f"\nClipping {task_name}...")
    
    if output_path.exists():
        print(f"  Output exists, skipping: {output_path}")
        update_status("clip", task_name, "complete", 100, "File exists")
        return True
    
    if not input_path.exists():
        print(f"  Error: Input not found: {input_path}")
        update_status("clip", task_name, "error", 0, "Input file missing")
        return False
    
    update_status("clip", task_name, "running", 50, "Clipping raster...")
    
    # Use gdalwarp to clip
    # -te: target extent (xmin ymin xmax ymax)
    # -co: compression options
    cmd = [
        "gdalwarp",
        "-te", *AUSTRIA_BBOX.split(),
        "-co", "COMPRESS=LZW",
        "-co", "TILED=YES",
        str(input_path),
        str(output_path)
    ]
    
    print(f"  Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"  Complete: {output_path}")
        update_status("clip", task_name, "complete", 100, "Clipped successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  Error: {e.stderr}")
        update_status("clip", task_name, "error", 0, e.stderr)
        return False

def main():
    print("="*60)
    print("Clip Hansen rasters to Austria")
    print("="*60)
    
    rasters = [
        ("lossyear", "hansen_lossyear.tif", "austria_lossyear.tif"),
        ("treecover2000", "hansen_treecover2000.tif", "austria_treecover2000.tif"),
    ]
    
    for name, input_name, output_name in rasters:
        input_path = RASTER_DIR / input_name
        output_path = RASTER_DIR / output_name
        clip_raster(input_path, output_path, name)
    
    print("\n" + "="*60)
    print("Clipping complete!")
    print("="*60)

if __name__ == "__main__":
    main()
