#!/usr/bin/env python3
"""
Create compact data files for fast web loading.
Splits data by year and removes redundant info.
"""

import json
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"

def main():
    # Load full emissions data
    with open(DATA_DIR / "gemeinde_emissions.json") as f:
        data = json.load(f)
    
    # Create metadata file (small, load first)
    metadata = {
        "years": data['years_available'],
        "summary": data['summary'],
        "methodology": data['methodology'],
        "units": data['units']
    }
    
    with open(DATA_DIR / "emissions_meta.json", "w") as f:
        json.dump(metadata, f)
    print(f"Created emissions_meta.json")
    
    # Create lookup for municipality names (only need this once)
    names = {}
    states = {}
    pop = {}
    
    # Use 2023 data for names/states/pop
    for iso, g in data['gemeinden']['2023'].items():
        names[iso] = g['n']
        states[iso] = g['s']
        pop[iso] = g['pop']
    
    lookup = {"names": names, "states": states, "population": pop}
    with open(DATA_DIR / "gemeinde_lookup.json", "w") as f:
        json.dump(lookup, f)
    print(f"Created gemeinde_lookup.json ({len(names)} municipalities)")
    
    # Create per-year data files (compact, only variable data)
    for year in data['years_available']:
        year_data = {}
        for iso, g in data['gemeinden'][year].items():
            # Only include non-zero entries and compact format
            if g['lp'] > 0:
                year_data[iso] = [
                    g['lp'],      # 0: loss pixels
                    g['la'],      # 1: loss area ha
                    g['h'],       # 2: harvest
                    g['v'],       # 3: value
                    g['co2'],     # 4: co2
                    g['ets'],     # 5: ets
                    g['ets_pc']   # 6: ets per capita
                ]
        
        filename = f"year_{year}.json"
        with open(DATA_DIR / filename, "w") as f:
            json.dump(year_data, f)
        
        size = (DATA_DIR / filename).stat().st_size
        print(f"Created {filename}: {len(year_data)} municipalities, {size/1024:.1f} KB")

if __name__ == "__main__":
    main()
