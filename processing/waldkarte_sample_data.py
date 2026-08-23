#!/usr/bin/env python3
"""
Mock Waldkarte forest stand data structure for design validation.

Shows how the downscaling would work with realistic sample data.
"""

import json
import random
from collections import defaultdict

# Mock existing municipality data (from gemeinde_emissions_scaled.json)
MUNICIPALITY_DATA = {
    "31001": {  # Alberndorf im Pulkautal
        "name": "Alberndorf im Pulkautal",
        "state": "Niederösterreich",
        "years": {
            "2023": {
                "harvest_efm": 42891,  # Scaled to official data
                "loss_pixels": 4766,
                "loss_area_ha": 428.94
            },
            "2022": {
                "harvest_efm": 38245,
                "loss_pixels": 4249,
                "loss_area_ha": 382.41
            }
        }
    },
    "60101": {  # Graz
        "name": "Graz",
        "state": "Steiermark",
        "years": {
            "2023": {
                "harvest_efm": 125000,
                "loss_pixels": 13889,
                "loss_area_ha": 1250.00
            }
        }
    }
}

# Mock Waldkarte forest stand data
# In reality, this would come from BFW WFS
FOREST_STANDS_MOCK = [
    # Stands in Alberndorf im Pulkautal (31001)
    {
        "stand_id": "WK_31001_001",
        "gemeinde_iso": "31001",
        "area_ha": 3.2,
        "forest_type": "Nadelwald",
        "age_class": "III",  # 40-60 years
        "raw_pixels": {  # Before scaling
            "2023": 178,
            "2022": 0,
        }
    },
    {
        "stand_id": "WK_31001_002",
        "gemeinde_iso": "31001",
        "area_ha": 1.5,
        "forest_type": "Mischwald",
        "age_class": "IV",
        "raw_pixels": {
            "2023": 89,
            "2022": 45,
        }
    },
    {
        "stand_id": "WK_31001_003",
        "gemeinde_iso": "31001",
        "area_ha": 5.1,
        "forest_type": "Nadelwald",
        "age_class": "V",  # 80+ years
        "raw_pixels": {
            "2023": 0,
            "2022": 234,
        }
    },
    # More stands in same municipality...
    # (In reality: ~127 stands for this municipality)
    
    # Stands in Graz (60101)
    {
        "stand_id": "WK_60101_001",
        "gemeinde_iso": "60101",
        "area_ha": 12.5,
        "forest_type": "Laubwald",
        "age_class": "IV",
        "raw_pixels": {
            "2023": 556,
        }
    },
]

def calculate_scaling_factor(gemeinde_iso, year):
    """
    Calculate scaling factor to match official municipality total.
    
    Factor = (Official Harvest) / (Sum of raw pixels in municipality)
    """
    # Get official target
    official_data = MUNICIPALITY_DATA[gemeinde_iso]
    target_harvest = official_data["years"][year]["harvest_efm"]
    target_pixels = official_data["years"][year]["loss_pixels"]
    
    # Sum raw pixels from all stands
    raw_pixel_sum = sum(
        stand["raw_pixels"].get(year, 0)
        for stand in FOREST_STANDS_MOCK
        if stand["gemeinde_iso"] == gemeinde_iso
    )
    
    if raw_pixel_sum > 0:
        factor = target_harvest / raw_pixel_sum
        pixel_check = target_pixels / raw_pixel_sum
    else:
        factor = 0
        pixel_check = 0
    
    return factor, raw_pixel_sum, pixel_check

def downscale_to_stands(gemeinde_iso, year):
    """
    Apply constrained downscaling to forest stands.
    """
    # Calculate scaling factor
    factor, raw_sum, pixel_check = calculate_scaling_factor(gemeinde_iso, year)
    
    print(f"\n{'='*70}")
    print(f"Downscaling: {MUNICIPALITY_DATA[gemeinde_iso]['name']} ({year})")
    print(f"{'='*70}")
    print(f"Official harvest target: {MUNICIPALITY_DATA[gemeinde_iso]['years'][year]['harvest_efm']:,.0f} Efm")
    print(f"Raw pixel sum: {raw_sum:,} pixels")
    print(f"Scaling factor: {factor:.2f} Efm/pixel")
    print(f"Pixel check: {pixel_check:.4f} (should be ~1.0)")
    print()
    
    # Apply to each stand
    stands_scaled = []
    total_check = 0
    
    print(f"{'Stand ID':<20} {'Area (ha)':<12} {'Type':<12} {'Raw Px':<10} {'Scaled Efm':<12}")
    print("-" * 70)
    
    for stand in FOREST_STANDS_MOCK:
        if stand["gemeinde_iso"] != gemeinde_iso:
            continue
        
        raw_pixels = stand["raw_pixels"].get(year, 0)
        if raw_pixels == 0:
            continue
        
        # Apply scaling
        scaled_harvest = raw_pixels * factor
        
        # Calculate other metrics (same formulas as current approach)
        pixel_area_ha = raw_pixels * 0.09  # 30m pixels
        
        stand_result = {
            "stand_id": stand["stand_id"],
            "gemeinde_iso": gemeinde_iso,
            "area_ha": stand["area_ha"],
            "forest_type": stand["forest_type"],
            "age_class": stand["age_class"],
            "year": year,
            "raw_pixels": raw_pixels,
            "loss_area_ha": round(pixel_area_ha, 2),
            "scaled_harvest_efm": round(scaled_harvest, 0),
            "harvest_density_efm_ha": round(scaled_harvest / pixel_area_ha, 1) if pixel_area_ha > 0 else 0,
            "pct_of_municipality": round(100 * scaled_harvest / MUNICIPALITY_DATA[gemeinde_iso]['years'][year]['harvest_efm'], 2)
        }
        
        stands_scaled.append(stand_result)
        total_check += scaled_harvest
        
        print(f"{stand_result['stand_id']:<20} {stand_result['area_ha']:<12.1f} {stand_result['forest_type']:<12} "
              f"{stand_result['raw_pixels']:<10} {stand_result['scaled_harvest_efm']:<12,.0f}")
    
    print("-" * 70)
    print(f"{'TOTAL':<20} {'':<12} {'':<12} {raw_sum:<10} {total_check:<12,.0f}")
    print()
    print(f"Validation: {total_check:,.0f} vs {MUNICIPALITY_DATA[gemeinde_iso]['years'][year]['harvest_efm']:,.0f}")
    print(f"Difference: {abs(total_check - MUNICIPALITY_DATA[gemeinde_iso]['years'][year]['harvest_efm']):.2f} Efm (rounding)")
    
    return stands_scaled

def show_stand_detail(stand_result):
    """
    Show detailed information for a single stand.
    """
    print(f"\n{'='*70}")
    print(f"Stand Detail: {stand_result['stand_id']}")
    print(f"{'='*70}")
    print(f"Location: {MUNICIPALITY_DATA[stand_result['gemeinde_iso']]['name']}, "
          f"{MUNICIPALITY_DATA[stand_result['gemeinde_iso']]['state']}")
    print(f"Stand area: {stand_result['area_ha']} ha")
    print(f"Forest type: {stand_result['forest_type']}")
    print(f"Age class: {stand_result['age_class']}")
    print()
    print(f"Year {stand_result['year']} harvest:")
    print(f"  Loss area: {stand_result['loss_area_ha']} ha ({stand_result['raw_pixels']} pixels)")
    print(f"  Estimated harvest: {stand_result['scaled_harvest_efm']:,.0f} Efm")
    print(f"  Harvest density: {stand_result['harvest_density_efm_ha']} Efm/ha")
    print(f"  % of municipality: {stand_result['pct_of_municipality']}%")
    print()
    print(f"Interpretation:")
    print(f"  - This stand lost {stand_result['loss_area_ha']} ha in {stand_result['year']}")
    print(f"  - Represents {stand_result['pct_of_municipality']}% of total harvest in "
          f"{MUNICIPALITY_DATA[stand_result['gemeinde_iso']]['name']}")
    
    # Intensity assessment
    if stand_result['loss_area_ha'] / stand_result['area_ha'] > 0.5:
        print(f"  - HIGH INTENSITY: >50% of stand area harvested")
    elif stand_result['loss_area_ha'] / stand_result['area_ha'] > 0.2:
        print(f"  - MODERATE: 20-50% of stand area harvested")
    else:
        print(f"  - LOW: <20% of stand area (selective harvest/thinning)")

def compare_approaches():
    """
    Compare current municipality-level vs proposed stand-level approach.
    """
    print("\n" + "="*70)
    print("COMPARISON: Municipality-Level vs Stand-Level Downscaling")
    print("="*70)
    
    print("\n## Current Approach (Municipality-Level)")
    print("-" * 70)
    print("Alberndorf im Pulkautal (2023):")
    print(f"  Total harvest: 42,891 Efm")
    print(f"  Loss area: 428.94 ha")
    print(f"  Average density: {42891/428.94:.1f} Efm/ha")
    print("\nLimitation: Cannot answer:")
    print("  - Which specific forests were harvested?")
    print("  - What types of forests (Nadel/Laub/Misch)?")
    print("  - How is harvest distributed across stands?")
    print("  - Are small or large stands more affected?")
    
    print("\n## Proposed Approach (Stand-Level)")
    print("-" * 70)
    stands = downscale_to_stands("31001", "2023")
    
    if stands:
        print("\nCan now answer:")
        print(f"  ✓ {len(stands)} specific forest stands had harvest")
        print(f"  ✓ Stand WK_31001_001 (Nadelwald, 3.2 ha) contributed {stands[0]['pct_of_municipality']}%")
        print(f"  ✓ Harvest density ranges from {min(s['harvest_density_efm_ha'] for s in stands):.1f} to {max(s['harvest_density_efm_ha'] for s in stands):.1f} Efm/ha")
        print(f"  ✓ Can export stand-level GeoJSON for mapping")
        print(f"  ✓ Can track same stand over multiple years")
    
    print("\n" + "="*70)
    print("Key Advantage: Spatial precision while maintaining official totals")
    print("="*70)

def generate_output_formats():
    """
    Show what output formats would look like.
    """
    stands = downscale_to_stands("31001", "2023")
    
    print("\n" + "="*70)
    print("Output Format Examples")
    print("="*70)
    
    # JSON format
    print("\n### JSON Structure:")
    print(json.dumps({
        "stand": stands[0] if stands else {}
    }, indent=2))
    
    # API endpoint format
    print("\n### API Endpoints:")
    print("GET /api/stands?gemeinde=31001&year=2023")
    print("  → Returns all stands with harvest in municipality for that year")
    print("\nGET /api/stands/WK_31001_001")
    print("  → Returns full history for single stand")
    print("\nGET /api/stands/geojson?gemeinde=31001&year=2023&min_harvest=1000")
    print("  → Returns GeoJSON for mapping (with harvest filter)")
    
    # Frontend card format
    if stands:
        print("\n### Frontend Stand Card:")
        print("┌─────────────────────────────────────────────────────┐")
        print(f"│ Forest Stand: {stands[0]['stand_id']:<30} │")
        print("├─────────────────────────────────────────────────────┤")
        print(f"│ Type: {stands[0]['forest_type']:<36} │")
        print(f"│ Area: {stands[0]['area_ha']:.1f} ha{' '*38}│")
        print(f"│ 2023 Harvest: {stands[0]['scaled_harvest_efm']:,.0f} Efm{' '*25}│")
        print(f"│ Loss area: {stands[0]['loss_area_ha']} ha{' '*33}│")
        print("│                                                     │")
        print("│ [View on map] [Show history] [Export data]         │")
        print("└─────────────────────────────────────────────────────┘")

if __name__ == "__main__":
    print("\n" + "="*70)
    print("Waldkarte Forest Stand Downscaling - Design Validation")
    print("Demonstration with Mock Data")
    print("="*70)
    
    # Main demonstration
    compare_approaches()
    
    # Show stand detail
    stands = downscale_to_stands("31001", "2023")
    if stands:
        show_stand_detail(stands[0])
    
    # Output formats
    generate_output_formats()
    
    print("\n" + "="*70)
    print("Next Steps:")
    print("="*70)
    print("1. Download Waldkarte data from BFW WFS")
    print("2. Rasterize to match Hansen grid")
    print("3. Run aggregation on real data")
    print("4. Validate conservation properties")
    print("5. Integrate into web app")
    print("="*70)
