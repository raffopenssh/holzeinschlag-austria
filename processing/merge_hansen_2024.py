#!/usr/bin/env python3
"""
Merge Hansen 2024 v1.12 tiles and clip to Austria extent.
Replace the existing austria_lossyear.tif with the new 2024 data.
"""

import subprocess
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
RASTER_DIR = BASE_DIR / "raster"

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
    print("Merging Hansen 2024 Tiles for Complete Austria Coverage")
    print("="*70)
    
    # Austria bounds
    xmin, ymin, xmax, ymax = 9.5, 46.3, 17.2, 49.1
    
    # Paths
    west_2024 = RASTER_DIR / "hansen_lossyear_2024_west.tif"
    east_2024 = RASTER_DIR / "hansen_lossyear_2024_east.tif"
    output = RASTER_DIR / "austria_lossyear_2024.tif"
    
    # Create VRT
    vrt_path = RASTER_DIR / "temp_2024.vrt"
    cmd = ["gdalbuildvrt", str(vrt_path), str(west_2024), str(east_2024)]
    if not run_cmd(cmd, "Building VRT"):
        return
    
    # Clip to Austria
    cmd = [
        "gdalwarp",
        "-te", str(xmin), str(ymin), str(xmax), str(ymax),
        "-co", "COMPRESS=LZW",
        str(vrt_path),
        str(output)
    ]
    if not run_cmd(cmd, "Clipping to Austria"):
        return
    
    # Clean up VRT
    vrt_path.unlink(missing_ok=True)
    
    # Backup old file and replace
    old_file = RASTER_DIR / "austria_lossyear.tif"
    backup = RASTER_DIR / "austria_lossyear_v2023.tif"
    
    if old_file.exists():
        print(f"\nBacking up old file to {backup}")
        old_file.rename(backup)
    
    print(f"Renaming {output} to {old_file}")
    output.rename(old_file)
    
    print("\n" + "="*70)
    print("Merge complete! Now using Hansen 2024 v1.12 data")
    print("="*70)
    
    # Show info
    subprocess.run(["gdalinfo", "-mm", str(old_file)])

if __name__ == "__main__":
    main()
