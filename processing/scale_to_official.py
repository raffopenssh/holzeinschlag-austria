#!/usr/bin/env python3
"""
Scale municipality harvest estimates to match official Bundesländer totals.

For each year and state:
1. Sum current municipality estimates
2. Get official state total from ministry reports
3. Calculate scaling factor
4. Apply to all municipalities in that state

For years without official data, interpolate or use Austria-wide average.
"""

import json
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"

def load_data():
    """Load all required data files."""
    with open(DATA_DIR / "gemeinde_emissions.json") as f:
        emissions = json.load(f)
    
    with open(DATA_DIR / "historical_harvest.json") as f:
        official = json.load(f)
    
    with open(DATA_DIR / "timber_values.json") as f:
        timber_values = json.load(f)
    
    return emissions, official, timber_values

def get_state_mapping():
    """Map various state name formats."""
    return {
        "Burgenland": "Burgenland",
        "Kärnten": "Kärnten", 
        "Niederösterreich": "Niederösterreich",
        "Oberösterreich": "Oberösterreich",
        "Salzburg": "Salzburg",
        "Steiermark": "Steiermark",
        "Tirol": "Tirol",
        "Vorarlberg": "Vorarlberg",
        "Wien": "Wien",
    }

def interpolate_missing_years(official):
    """
    Fill in missing years by interpolation or extrapolation.
    Available: 2011, 2012, 2014, 2017-2024
    Missing: 2001-2010, 2013, 2015, 2016
    """
    states = ["Burgenland", "Kärnten", "Niederösterreich", "Oberösterreich", 
              "Salzburg", "Steiermark", "Tirol", "Vorarlberg", "Wien"]
    
    result = dict(official)  # Copy existing
    
    # For 2013: interpolate between 2012 and 2014
    if "2012" in official and "2014" in official:
        result["2013"] = {
            "year": 2013,
            "austria_total": (official["2012"]["austria_total"] + official["2014"]["austria_total"]) / 2,
            "states": {}
        }
        for state in states:
            v2012 = official["2012"]["states"].get(state, 0)
            v2014 = official["2014"]["states"].get(state, 0)
            result["2013"]["states"][state] = (v2012 + v2014) / 2
    
    # For 2015, 2016: interpolate between 2014 and 2017
    if "2014" in official and "2017" in official:
        for year, weight in [("2015", 1/3), ("2016", 2/3)]:
            result[year] = {
                "year": int(year),
                "austria_total": official["2014"]["austria_total"] * (1-weight) + official["2017"]["austria_total"] * weight,
                "states": {}
            }
            for state in states:
                v2014 = official["2014"]["states"].get(state, 0)
                v2017 = official["2017"]["states"].get(state, 0)
                result[year]["states"][state] = v2014 * (1-weight) + v2017 * weight
    
    # For 2001-2010: extrapolate backward from 2011 using Austria average ~17-18M Efm
    # Use 2011 state proportions
    if "2011" in official:
        austria_2011 = official["2011"]["austria_total"]
        state_shares_2011 = {s: v/austria_2011 for s, v in official["2011"]["states"].items()}
        
        # Historical Austria totals (approximate, from various sources)
        historical_totals = {
            2001: 14500000,  # Lower harvest years
            2002: 14800000,
            2003: 16200000,  # Windstorm year
            2004: 16500000,
            2005: 16100000,
            2006: 16800000,
            2007: 21100000,  # Kyrill storm
            2008: 19200000,
            2009: 16300000,
            2010: 17500000,
        }
        
        for year in range(2001, 2011):
            year_str = str(year)
            total = historical_totals.get(year, 17000000)
            result[year_str] = {
                "year": year,
                "austria_total": total,
                "states": {s: total * share for s, share in state_shares_2011.items()}
            }
    
    return result

def calculate_scaling_factors(emissions, official_full):
    """Calculate per-state, per-year scaling factors."""
    scaling_factors = {}
    
    for year_str in emissions['years_available']:
        year_data = emissions['gemeinden'].get(year_str, {})
        
        # Sum current estimates by state
        state_sums = defaultdict(float)
        for iso, gem in year_data.items():
            state = gem.get('s', '')
            harvest = gem.get('h', 0)
            state_sums[state] += harvest
        
        # Get official totals
        official_year = official_full.get(year_str, {})
        official_states = official_year.get('states', {})
        
        # Calculate scaling factors
        scaling_factors[year_str] = {}
        for state, current_sum in state_sums.items():
            official_total = official_states.get(state, 0)
            if current_sum > 0 and official_total > 0:
                factor = official_total / current_sum
            else:
                factor = 1.0
            scaling_factors[year_str][state] = factor
            
        print(f"\n{year_str}:")
        for state in sorted(state_sums.keys()):
            curr = state_sums[state]
            off = official_states.get(state, 0)
            factor = scaling_factors[year_str].get(state, 1.0)
            print(f"  {state:20} Current: {curr:>12,.0f}  Official: {off:>12,.0f}  Factor: {factor:>6.1f}x")
    
    return scaling_factors

def apply_scaling(emissions, scaling_factors, timber_prices):
    """Apply scaling factors and recalculate derived values."""
    
    # CO2 factors
    CO2_PER_EFM_LOW_QUALITY = 0.9
    
    # State damage fractions
    state_damage_fractions = {
        "Burgenland": 0.40,
        "Kärnten": 0.55,
        "Niederösterreich": 0.50,
        "Oberösterreich": 0.55,
        "Salzburg": 0.55,
        "Steiermark": 0.55,
        "Tirol": 0.50,
        "Vorarlberg": 0.45,
        "Wien": 0.40,
    }
    
    # ETS prices
    ETS_PRICES = {
        2001: 0, 2002: 0, 2003: 0, 2004: 0,
        2005: 22, 2006: 17, 2007: 0.7, 2008: 22,
        2009: 13, 2010: 15, 2011: 15, 2012: 8,
        2013: 5, 2014: 6, 2015: 8, 2016: 5,
        2017: 6, 2018: 16, 2019: 25, 2020: 25,
        2021: 53, 2022: 81, 2023: 85,
    }
    
    scaled_emissions = {
        "years_available": emissions['years_available'],
        "units": emissions['units'],
        "methodology": {
            **emissions['methodology'],
            "scaling": "Harvest scaled to match official Bundesländer totals from ministry reports"
        },
        "summary": {},
        "gemeinden": {}
    }
    
    for year_str in emissions['years_available']:
        year = int(year_str)
        year_factors = scaling_factors.get(year_str, {})
        year_data = emissions['gemeinden'].get(year_str, {})
        
        ets_price = ETS_PRICES.get(year, 0)
        timber_price = timber_prices.get(year, 100)
        
        scaled_year = {}
        year_totals = {
            'loss_pixels': 0, 'loss_area': 0, 'harvest': 0, 
            'value': 0, 'co2': 0, 'ets': 0, 'count': 0
        }
        
        for iso, gem in year_data.items():
            state = gem.get('s', '')
            factor = year_factors.get(state, 1.0)
            
            # Scale harvest
            old_harvest = gem.get('h', 0)
            new_harvest = old_harvest * factor
            
            # Recalculate derived values
            new_value = new_harvest * timber_price
            
            damage_fraction = state_damage_fractions.get(state, 0.50)
            low_quality = new_harvest * damage_fraction
            co2 = low_quality * CO2_PER_EFM_LOW_QUALITY
            ets = co2 * ets_price
            
            pop = gem.get('pop', 0)
            ets_pc = ets / pop if pop > 0 else 0
            
            scaled_year[iso] = {
                "n": gem.get('n', ''),
                "s": state,
                "lp": gem.get('lp', 0),
                "la": gem.get('la', 0),
                "h": round(new_harvest, 0),
                "v": round(new_value, 0),
                "p": timber_price,
                "pop": pop,
                "lq": round(low_quality, 0),
                "co2": round(co2, 0),
                "ets": round(ets, 0),
                "ets_pc": round(ets_pc, 2)
            }
            
            # Accumulate totals
            year_totals['loss_pixels'] += gem.get('lp', 0)
            year_totals['loss_area'] += gem.get('la', 0)
            year_totals['harvest'] += new_harvest
            year_totals['value'] += new_value
            year_totals['co2'] += co2
            year_totals['ets'] += ets
            if gem.get('lp', 0) > 0:
                year_totals['count'] += 1
        
        scaled_emissions['gemeinden'][year_str] = scaled_year
        scaled_emissions['summary'][year_str] = {
            "total_loss_pixels": year_totals['loss_pixels'],
            "total_loss_area_ha": round(year_totals['loss_area'], 1),
            "total_harvest_efm": round(year_totals['harvest'], 0),
            "total_value_eur": round(year_totals['value'], 0),
            "ets_price_eur_tco2": ets_price,
            "timber_price_eur_efm": timber_price,
            "total_co2_tonnes": round(year_totals['co2'], 0),
            "total_ets_liability_eur": round(year_totals['ets'], 0),
            "gemeinde_count": year_totals['count']
        }
    
    return scaled_emissions

def main():
    print("="*70)
    print("Scaling Municipality Harvest to Official Bundesländer Totals")
    print("="*70)
    
    emissions, official, timber_values = load_data()
    
    # Get timber prices by year
    timber_prices = {
        2001: 75, 2002: 70, 2003: 72, 2004: 76, 2005: 80,
        2006: 82, 2007: 90, 2008: 95, 2009: 70, 2010: 82,
        2011: 94, 2012: 96, 2013: 97, 2014: 98, 2015: 92,
        2016: 90, 2017: 91, 2018: 88, 2019: 77, 2020: 73,
        2021: 100, 2022: 113, 2023: 103,
    }
    
    # Fill in missing years
    print("\nInterpolating missing years...")
    official_full = interpolate_missing_years(official)
    
    # Calculate scaling factors
    print("\nCalculating scaling factors per state per year...")
    scaling_factors = calculate_scaling_factors(emissions, official_full)
    
    # Apply scaling
    print("\n" + "="*70)
    print("Applying scaling factors...")
    scaled = apply_scaling(emissions, scaling_factors, timber_prices)
    
    # Save scaled emissions
    output_path = DATA_DIR / "gemeinde_emissions_scaled.json"
    with open(output_path, 'w') as f:
        json.dump(scaled, f)
    print(f"\nSaved scaled emissions to: {output_path}")
    
    # Also save the interpolated official data
    official_path = DATA_DIR / "historical_harvest_full.json"
    with open(official_path, 'w') as f:
        json.dump(official_full, f, indent=2)
    print(f"Saved interpolated official data to: {official_path}")
    
    # Verify totals
    print("\n" + "="*70)
    print("Verification - Comparing scaled totals to official:")
    print("="*70)
    print(f"{'Year':<6} {'Scaled Total':<18} {'Official':<18} {'Difference':<12}")
    print("-"*60)
    
    for year_str in ['2017', '2018', '2019', '2020', '2021', '2022', '2023']:
        scaled_total = scaled['summary'].get(year_str, {}).get('total_harvest_efm', 0)
        official_total = official_full.get(year_str, {}).get('austria_total', 0)
        diff = scaled_total - official_total
        pct = (diff / official_total * 100) if official_total > 0 else 0
        print(f"{year_str:<6} {scaled_total:>15,.0f}    {official_total:>15,.0f}    {diff:>+10,.0f} ({pct:+.1f}%)")

if __name__ == "__main__":
    main()
