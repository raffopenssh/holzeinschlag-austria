#!/usr/bin/env python3
"""
Create compact per-year JSON files from scaled emissions data.
Format: { "iso": [loss_pixels, loss_area_ha, harvest_efm, value_eur, co2_t, ets_eur, ets_per_capita], ... }
"""

import json
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"

def main():
    # Load scaled emissions data
    with open(DATA_DIR / "gemeinde_emissions_scaled.json") as f:
        scaled = json.load(f)
    
    # Load lookup data for names etc
    with open(DATA_DIR / "gemeinde_lookup.json") as f:
        lookup = json.load(f)
    
    # Create compact year files
    for year_str in scaled['years_available']:
        year_data = scaled['gemeinden'].get(year_str, {})
        summary = scaled['summary'].get(year_str, {})
        
        compact = {}
        for iso, gem in year_data.items():
            # [loss_pixels, loss_area_ha, harvest_efm, value_eur, co2_t, ets_eur, ets_per_capita]
            compact[iso] = [
                gem.get('lp', 0),      # loss pixels
                gem.get('la', 0),      # loss area ha
                gem.get('h', 0),       # harvest Efm (SCALED)
                gem.get('v', 0),       # value EUR (SCALED)
                gem.get('co2', 0),     # CO2 tonnes (SCALED)
                gem.get('ets', 0),     # ETS liability (SCALED)
                gem.get('ets_pc', 0),  # ETS per capita (SCALED)
            ]
        
        output_path = DATA_DIR / f"year_{year_str}.json"
        with open(output_path, 'w') as f:
            json.dump(compact, f)
        
        print(f"{year_str}: {len(compact)} municipalities, "
              f"harvest={summary.get('total_harvest_efm', 0):,.0f} Efm, "
              f"CO2={summary.get('total_co2_tonnes', 0):,.0f} t")
    
    # Update emissions_meta.json with scaled summary
    meta = {
        "years": [y for y in scaled['years_available']],
        "summary": scaled['summary'],
        "methodology": scaled['methodology'],
        "units": scaled['units']
    }
    
    with open(DATA_DIR / "emissions_meta.json", 'w') as f:
        json.dump(meta, f, indent=2)
    
    print(f"\nUpdated emissions_meta.json")
    print("All year files regenerated with scaled harvest data")

if __name__ == "__main__":
    main()
