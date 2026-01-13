#!/usr/bin/env python3
"""
Download Hansen Global Forest Change data for western Austria (Vorarlberg region)
The 50N_000E tile covers 0-10°E longitude, 40-50°N latitude
This completes coverage of Austria which extends west to ~9.5°E
"""

import os
import sys
import urllib.request
import json
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
RASTER_DIR = BASE_DIR / "raster"
STATUS_FILE = BASE_DIR / "processing" / "status.json"

# Hansen GFC 2023 v1.11 tiles - Western tile (0-10°E)
HANSEN_BASE = "https://storage.googleapis.com/earthenginepartners-hansen/GFC-2023-v1.11"

TILES = {
    "lossyear_west": f"{HANSEN_BASE}/Hansen_GFC-2023-v1.11_lossyear_50N_000E.tif",
    "treecover2000_west": f"{HANSEN_BASE}/Hansen_GFC-2023-v1.11_treecover2000_50N_000E.tif",
}

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

def download_with_progress(url, dest, task_name):
    """Download file with progress updates"""
    print(f"Downloading {task_name}...")
    print(f"  URL: {url}")
    print(f"  Dest: {dest}")
    
    def progress_hook(count, block_size, total_size):
        percent = int(count * block_size * 100 / total_size)
        percent = min(percent, 100)
        if count % 100 == 0:
            update_status("download", task_name, "running", percent, f"Downloading: {percent}%")
            print(f"\r  Progress: {percent}%", end="", flush=True)
    
    update_status("download", task_name, "running", 0, "Starting download")
    
    try:
        urllib.request.urlretrieve(url, dest, progress_hook)
        print(f"\n  Complete: {dest}")
        file_size = os.path.getsize(dest)
        update_status("download", task_name, "complete", 100, f"Downloaded {file_size / 1024 / 1024:.1f} MB")
        return True
    except Exception as e:
        print(f"\n  Error: {e}")
        update_status("download", task_name, "error", 0, str(e))
        return False

def main():
    RASTER_DIR.mkdir(exist_ok=True)
    
    print("="*60)
    print("Hansen Global Forest Change - Western Tile Download")
    print("Covers: 0-10°E longitude (includes Vorarlberg)")
    print("="*60)
    
    for name, url in TILES.items():
        dest = RASTER_DIR / f"hansen_{name}.tif"
        if dest.exists():
            print(f"\n{name}: Already exists, skipping")
            update_status("download", name, "complete", 100, "File exists")
            continue
        
        success = download_with_progress(url, dest, name)
        if not success:
            sys.exit(1)
    
    print("\n" + "="*60)
    print("Download complete!")
    print("="*60)

if __name__ == "__main__":
    main()
