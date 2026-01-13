#!/usr/bin/env python3
"""
Download Hansen Global Forest Change 2024 v1.12 data for Austria.
This version includes loss data through 2024 (lossyear value 24 = 2024).
"""

import os
import urllib.request
from pathlib import Path

RASTER_DIR = Path(__file__).parent.parent / "raster"

# Hansen GFC 2024 v1.12 tiles
HANSEN_BASE = "https://storage.googleapis.com/earthenginepartners-hansen/GFC-2024-v1.12"

TILES = {
    "lossyear_2024_east": f"{HANSEN_BASE}/Hansen_GFC-2024-v1.12_lossyear_50N_010E.tif",
    "lossyear_2024_west": f"{HANSEN_BASE}/Hansen_GFC-2024-v1.12_lossyear_50N_000E.tif",
}

def download_with_progress(url, dest, task_name):
    print(f"Downloading {task_name}...")
    print(f"  URL: {url}")
    print(f"  Dest: {dest}")
    
    def progress_hook(count, block_size, total_size):
        percent = int(count * block_size * 100 / total_size)
        percent = min(percent, 100)
        if count % 100 == 0:
            print(f"\r  Progress: {percent}%", end="", flush=True)
    
    try:
        urllib.request.urlretrieve(url, dest, progress_hook)
        print(f"\n  Complete: {dest}")
        file_size = os.path.getsize(dest)
        print(f"  Size: {file_size / 1024 / 1024:.1f} MB")
        return True
    except Exception as e:
        print(f"\n  Error: {e}")
        return False

def main():
    RASTER_DIR.mkdir(exist_ok=True)
    
    print("="*60)
    print("Hansen Global Forest Change 2024 v1.12 - Download")
    print("Includes forest loss data through 2024")
    print("="*60)
    
    for name, url in TILES.items():
        dest = RASTER_DIR / f"hansen_{name}.tif"
        if dest.exists():
            print(f"\n{name}: Already exists, skipping")
            continue
        
        success = download_with_progress(url, dest, name)
        if not success:
            return False
    
    print("\n" + "="*60)
    print("Download complete!")
    print("="*60)
    return True

if __name__ == "__main__":
    main()
