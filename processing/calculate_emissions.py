#!/usr/bin/env python3
"""
Calculate CO2 emissions and ETS liability from actual forest loss data.

Uses:
- gemeinde_yearly_loss.json: Actual forest loss area per municipality per year
- timber_values.json: State-level harvest and value data
- population.json: Municipality population data
- Historical ETS prices

Outputs:
- gemeinde_emissions.json: Full emissions data per municipality per year
"""

import json
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"

# Historical EU ETS prices (EUR/tCO2)
# Source: EU ETS data, rounded annual averages
ETS_PRICES = {
    2001: 0,      # ETS not yet active (Phase 1 started 2005)
    2002: 0,
    2003: 0,
    2004: 0,
    2005: 22,     # ETS Phase 1 starts
    2006: 17,
    2007: 0.7,    # Price collapse
    2008: 22,     # Phase 2 starts
    2009: 13,
    2010: 15,
    2011: 15,
    2012: 8,
    2013: 5,      # Price crash
    2014: 6,
    2015: 8,
    2016: 5,
    2017: 6,
    2018: 16,     # Price recovery begins
    2019: 25,
    2020: 25,
    2021: 53,     # Strong increase
    2022: 81,     # Record highs
    2023: 85,
    2024: 70,     # Projected
    # Future ETS2 projections (transport/buildings sector)
    2027: 45,     # ETS2 starts, price cap expected
    2028: 50,
    2029: 55,
    2030: 60,
}

# CO2 emission factors
# Average standing biomass in Austrian forests: ~150 tC/ha
# This converts to ~550 tCO2/ha if fully cleared
# But harvesting typically removes only merchantable wood, not roots/soil carbon
# Using conservative estimate: 50-100 tCO2/ha for harvest-related emissions

# More refined approach:
# - Efm (harvest volume) to CO2 using wood density and carbon content
# - 1 m³ wood ≈ 0.45 t dry matter ≈ 0.9 tCO2 (for combustion/decay)
# - But sustainable forestry: new growth offsets this over rotation
# - Key issue: DAMAGE WOOD often goes to low-value uses (energy, pulp)

CO2_PER_EFM_LOW_QUALITY = 0.9  # tCO2/m³ for wood that ends up burned/pulped
CO2_PER_EFM_SUSTAINABLE = 0.1  # tCO2/m³ for sustainably harvested sawlogs (offsets by growth)

def main():
    print("="*70)
    print("CO2 Emissions & ETS Liability Calculator")
    print("="*70)
    
    # Load actual forest loss data
    with open(DATA_DIR / "gemeinde_yearly_loss.json") as f:
        loss_data = json.load(f)
    
    # Load timber values for state-level data
    with open(DATA_DIR / "timber_values.json") as f:
        timber_values = json.load(f)
    
    # Load population data
    try:
        with open(DATA_DIR / "population.json") as f:
            pop_data = json.load(f)
    except:
        pop_data = {}
        print("Warning: No population data found")
    
    # Load historical harvest data if available
    try:
        with open(DATA_DIR / "historical_harvest.json") as f:
            historical = json.load(f)
    except:
        historical = None
        print("Warning: No historical harvest data found")
    
    # Load state analysis for harvest ratios
    with open(DATA_DIR / "hansen_state_analysis.json") as f:
        hansen_state = json.load(f)
    
    # Get state damage wood fractions (approximated from 2024 data)
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
    
    # Calculate weighted average timber price from timber_values
    weighted_avg_price = timber_values['prices']['weighted_avg_eur_efm']
    
    # Get historical timber prices by year (approximated)
    historical_prices = {
        2001: 75, 2002: 70, 2003: 72, 2004: 76, 2005: 80,
        2006: 82, 2007: 90, 2008: 95, 2009: 70, 2010: 82,
        2011: 94, 2012: 96, 2013: 97, 2014: 98, 2015: 92,
        2016: 90, 2017: 91, 2018: 88, 2019: 77, 2020: 73,
        2021: 100, 2022: 113, 2023: 103, 2024: 100,
    }
    
    years = loss_data['years']
    
    # Results structure
    results = {
        "description": "Per-year municipality forest loss with emissions and ETS costs",
        "years_available": [str(y) for y in years],
        "units": {
            "loss_area": "ha (hectares)",
            "loss_pixels": "count (30m Hansen pixels)",
            "co2": "tonnes CO2",
            "ets_price": "EUR/tCO2",
            "ets_liability": "EUR",
            "population": "inhabitants"
        },
        "methodology": {
            "co2_factor": "Low-quality wood (pulp, paper, energy) assumed to emit 0.9 tCO2/m³",
            "low_quality_fraction": "State-level fraction based on damage wood shares (40-55%)",
            "ets_prices": "Historical EU ETS prices, 2027+ projected for ETS2",
            "forest_loss": "Actual pixel counts from Hansen GFC lossyear raster"
        },
        "summary": {},
        "gemeinden": {}
    }
    
    # Process each year
    for year in years:
        year_str = str(year)
        ets_price = ETS_PRICES.get(year, 0)
        timber_price = historical_prices.get(year, weighted_avg_price)
        
        # Get state-level Efm/ha ratios
        state_efm_ha = {}
        for state, data in hansen_state['states'].items():
            if data['total_area_ha'] > 0:
                state_efm_ha[state] = data['efm_per_ha_ratio']
            else:
                state_efm_ha[state] = 28  # Austria average
        
        year_total_loss_ha = 0
        year_total_pixels = 0
        year_total_harvest = 0
        year_total_value = 0
        year_total_co2 = 0
        year_total_ets = 0
        year_gemeinden = {}
        
        for iso, gemeente in loss_data['gemeinden'].items():
            # Get loss data for this year
            year_loss = gemeente['years'].get(year_str, {})
            loss_pixels = year_loss.get('pixels', 0)
            loss_area_ha = year_loss.get('area_ha', 0)
            
            state = gemeente['state']
            name = gemeente['name']
            
            # Estimate harvest based on loss area and state Efm/ha ratio
            efm_per_ha = state_efm_ha.get(state, 28)
            estimated_harvest = loss_area_ha * efm_per_ha
            estimated_value = estimated_harvest * timber_price
            
            # CO2 emissions: fraction of harvest that goes to low-value uses
            damage_fraction = state_damage_fractions.get(state, 0.50)
            low_quality_volume = estimated_harvest * damage_fraction
            co2_emissions = low_quality_volume * CO2_PER_EFM_LOW_QUALITY
            
            # ETS liability
            ets_liability = co2_emissions * ets_price
            
            # Population for per-capita calculations
            pop = pop_data.get(iso, {}).get('population', 0)
            ets_per_capita = ets_liability / pop if pop > 0 else 0
            
            # Store gemeente data
            year_gemeinden[iso] = {
                "n": name,
                "s": state,
                "lp": loss_pixels,           # loss pixels this year
                "la": round(loss_area_ha, 2), # loss area (ha) this year
                "h": round(estimated_harvest, 0),  # estimated harvest (Efm)
                "v": round(estimated_value, 0),    # estimated value (EUR)
                "p": timber_price,            # timber price that year
                "pop": pop,                   # population
                "lq": round(low_quality_volume, 0),  # low quality volume
                "co2": round(co2_emissions, 0),      # CO2 emissions (tonnes)
                "ets": round(ets_liability, 0),      # ETS liability (EUR)
                "ets_pc": round(ets_per_capita, 2)   # ETS per capita (EUR)
            }
            
            # Accumulate totals
            year_total_loss_ha += loss_area_ha
            year_total_pixels += loss_pixels
            year_total_harvest += estimated_harvest
            year_total_value += estimated_value
            year_total_co2 += co2_emissions
            year_total_ets += ets_liability
        
        # Store year summary
        results['summary'][year_str] = {
            "total_loss_pixels": year_total_pixels,
            "total_loss_area_ha": round(year_total_loss_ha, 1),
            "total_harvest_efm": round(year_total_harvest, 0),
            "total_value_eur": round(year_total_value, 0),
            "ets_price_eur_tco2": ets_price,
            "timber_price_eur_efm": timber_price,
            "total_co2_tonnes": round(year_total_co2, 0),
            "total_ets_liability_eur": round(year_total_ets, 0),
            "gemeinde_count": len([g for g in year_gemeinden.values() if g['lp'] > 0])
        }
        
        # Store year gemeente data
        results['gemeinden'][year_str] = year_gemeinden
        
        print(f"{year}: {year_total_loss_ha:,.0f} ha loss, {year_total_co2:,.0f} tCO2, €{year_total_ets:,.0f} ETS @ €{ets_price}/t")
    
    # Save results
    output_path = DATA_DIR / "gemeinde_emissions.json"
    with open(output_path, "w") as f:
        json.dump(results, f)
    
    print(f"\nSaved to: {output_path}")
    
    # Summary stats
    print("\n" + "="*70)
    print("Summary (2018-2023, high ETS price period):")
    print("="*70)
    
    recent_years = ['2018', '2019', '2020', '2021', '2022', '2023']
    for y in recent_years:
        s = results['summary'].get(y, {})
        print(f"{y}: {s.get('total_loss_area_ha', 0):>10,.0f} ha | {s.get('total_co2_tonnes', 0):>12,.0f} tCO2 | €{s.get('total_ets_liability_eur', 0):>14,.0f} ETS")

if __name__ == "__main__":
    main()
