#!/bin/bash
# Background processing pipeline for Hansen data analysis
# Run with: nohup ./run_pipeline.sh > pipeline.log 2>&1 &

set -e

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR/processing"

echo "========================================"
echo "Hansen Forest Loss Processing Pipeline"
echo "Started: $(date)"
echo "========================================"

# Phase A: Download Hansen tiles
echo ""
echo "PHASE A: Downloading Hansen data..."
python3 download_hansen.py

# Phase B: Clip to Austria
echo ""
echo "PHASE B: Clipping rasters to Austria..."
python3 clip_to_austria.py

# Phase B: Aggregate by state
echo ""
echo "PHASE B: Aggregating by state..."
python3 aggregate_by_state.py

echo ""
echo "========================================"
echo "Pipeline complete: $(date)"
echo "========================================"
