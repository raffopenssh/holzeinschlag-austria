#!/usr/bin/env python3
"""
Plot-context resolver for external forestry-management systems.

stdin : GeoJSON Geometry/Feature/FeatureCollection (Polygon|MultiPolygon|Point, WGS84)
stdout: JSON — pixel-exact plot forest history (Hansen GFC-2024, 30m),
        surroundings comparison (1km/5km rings), municipality/state/Austria
        benchmarks, unified 2001-2024 timeline, timber prices, carbon flux.
"""
import json, math, sys
from pathlib import Path
import numpy as np
from osgeo import gdal, ogr, osr

gdal.UseExceptions(); ogr.UseExceptions(); osr.UseExceptions()

ROOT = Path(__file__).resolve().parent.parent
DATA, RAST = ROOT / "data", ROOT / "raster"
YEARS = [str(y) for y in range(2001, 2025)]

def load(name):
    with open(DATA / name) as f:
        return json.load(f)

SRC = osr.SpatialReference(); SRC.ImportFromEPSG(4326); SRC.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
DST = osr.SpatialReference(); DST.ImportFromEPSG(3035); DST.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
TO_3035 = osr.CoordinateTransformation(SRC, DST)
TO_4326 = osr.CoordinateTransformation(DST, SRC)

def area_ha(g):
    g2 = g.Clone(); g2.Transform(TO_3035); return g2.GetArea() / 10000.0

def ring(geom, inner_m, outer_m):
    g = geom.Clone(); g.Transform(TO_3035)
    r = g.Buffer(outer_m).Difference(g.Buffer(inner_m) if inner_m > 0 else g)
    r.Transform(TO_4326); return r

def mask_for(ds, geom):
    """Rasterize geom onto ds grid, return (mask bool array, window offsets, px_area_ha array-scalar per row)."""
    gt = ds.GetGeoTransform()
    env = geom.GetEnvelope()  # minx,maxx,miny,maxy
    x0 = max(0, int((env[0] - gt[0]) / gt[1]) - 1); x1 = min(ds.RasterXSize, int((env[1] - gt[0]) / gt[1]) + 2)
    y0 = max(0, int((env[3] - gt[3]) / gt[5]) - 1); y1 = min(ds.RasterYSize, int((env[2] - gt[3]) / gt[5]) + 2)
    if x1 <= x0 or y1 <= y0:
        return None
    w, h = x1 - x0, y1 - y0
    mem = gdal.GetDriverByName("MEM").Create("", w, h, 1, gdal.GDT_Byte)
    mem.SetGeoTransform((gt[0] + x0 * gt[1], gt[1], 0, gt[3] + y0 * gt[5], 0, gt[5]))
    mem.SetProjection(ds.GetProjection())
    drv = ogr.GetDriverByName("Memory").CreateDataSource("m")
    lyr = drv.CreateLayer("m", SRC)
    ft = ogr.Feature(lyr.GetLayerDefn()); ft.SetGeometry(geom); lyr.CreateFeature(ft)
    gdal.RasterizeLayer(mem, [1], lyr, burn_values=[1], options=["ALL_TOUCHED=FALSE"])
    m = mem.ReadAsArray().astype(bool)
    if not m.any():
        return None
    # per-row pixel area (ha): 30m-ish, lat dependent
    lats = gt[3] + (y0 + np.arange(h) + 0.5) * gt[5]
    px_ha = (abs(gt[1]) * 111320 * np.cos(np.radians(lats)) * abs(gt[5]) * 110540) / 10000.0
    return m, (x0, y0, w, h), px_ha[:, None]

def zonal(geom):
    """Forest stats for geometry from local Hansen + carbon-flux rasters."""
    out = {}
    ly = gdal.Open(str(RAST / "austria_lossyear.tif"))
    r = mask_for(ly, geom)
    if r is None:
        return None
    m, (x0, y0, w, h), px_ha = r
    loss = ly.GetRasterBand(1).ReadAsArray(x0, y0, w, h)
    tc_ds = gdal.Open(str(RAST / "austria_treecover2000.tif"))
    tc = tc_ds.GetRasterBand(1).ReadAsArray(x0, y0, w, h)
    A = float((m * px_ha).sum())
    forest_m = m & (tc >= 30)
    fA = float((forest_m * px_ha).sum())
    out["area_ha"] = round(A, 2)
    out["forest_area_2000_ha"] = round(fA, 2)
    out["forest_share_2000_pct"] = round(100 * fA / A, 1) if A else None
    out["treecover2000_mean_pct"] = round(float(tc[m].mean()), 1)
    per_year, cum = {}, 0.0
    for yr in range(1, 25):
        lha = float((px_ha * ((loss == yr) & m)).sum())
        per_year[str(2000 + yr)] = round(lha, 3)
        cum += lha
    out["loss_ha_by_year"] = per_year
    out["loss_total_2001_2024_ha"] = round(cum, 2)
    out["loss_pct_of_forest2000"] = round(100 * cum / fA, 1) if fA else None
    # carbon flux (Harris et al., Mg CO2e/ha over 2001-2024) — tile 50N_010E covers 40-50N/10-20E
    for name in ("net_flux", "gross_emissions", "gross_removals"):
        try:
            ds = gdal.Open(str(RAST / "carbon_flux" / f"{name}_50N_010E.tif"))
            rr = mask_for(ds, geom)
            if rr:
                mm, (cx, cy, cw, ch), _ = rr
                v = ds.GetRasterBand(1).ReadAsArray(cx, cy, cw, ch)
                out[f"{name}_tCO2e_per_ha_2001_2024"] = round(float(v[mm].mean()), 1)
        except Exception:
            out[f"{name}_tCO2e_per_ha_2001_2024"] = None
    return out

def regional_prices(state):
    """Latest LK price ranges for the plot's state from the full catalogue."""
    try:
        cat = load("timber_price_catalog.json")
    except FileNotFoundError:
        return None
    pages = cat.get("state_pages", {}).get(state)
    if not pages:
        return None
    out = {"state": state, "source": "LK " + state + " via preise.agrarforschung.at",
           "updated_at": cat.get("updated_at"), "assortments": {}}
    for kind, cid in pages.items():
        page = cat["pages"].get(cid, {})
        for code, s in page.get("series", {}).items():
            if code.endswith(("_von", "_bis")):
                continue
            lo = page["series"].get(code + "_von", {}).get("latest_value")
            hi = page["series"].get(code + "_bis", {}).get("latest_value")
            out["assortments"][code] = {
                "kind": kind, "name": s.get("name"), "unit": s.get("unit"),
                "latest": s.get("latest_value"), "latest_date": s.get("last"),
                "range_low": lo, "range_high": hi,
                "yearly_avg": s.get("yearly_avg"),
            }
    return out

def fail(msg, code):
    """Print a JSON error and exit with a code the Go server maps to an HTTP status:
    2 -> 400 bad input, 3 -> 422 outside Austria."""
    print(json.dumps({"error": msg}))
    sys.stdout.flush()
    import os
    os._exit(code)

def main():
    fast = "--fast" in sys.argv[1:]
    try:
        raw = json.load(sys.stdin)
    except Exception:
        fail("request body is not valid JSON", 2)
    if not isinstance(raw, dict):
        fail("expected a GeoJSON Geometry, Feature or FeatureCollection object", 2)
    if raw.get("type") == "FeatureCollection":
        feats = raw.get("features") or []
        if not feats:
            fail("FeatureCollection has no features", 2)
        raw = feats[0]
    if raw.get("type") == "Feature":
        raw = raw.get("geometry")
        if not raw:
            fail("Feature has no geometry", 2)
    try:
        geom = ogr.CreateGeometryFromJson(json.dumps(raw))
    except Exception:
        geom = None
    if geom is None:
        fail("invalid GeoJSON geometry", 2)
    try:
        geom.CloseRings()
        if not geom.IsValid():
            geom = geom.MakeValid()
        if geom is None or geom.IsEmpty():
            fail("invalid GeoJSON geometry (empty after repair)", 2)
    except Exception as e:
        fail(f"invalid GeoJSON geometry: {e}", 2)
    if geom.GetGeometryName() in ("POINT", "MULTIPOINT"):
        g3 = geom.Clone(); g3.Transform(TO_3035); g3 = g3.Buffer(100); g3.Transform(TO_4326)
        geom, point_input = g3, True
    else:
        point_input = False
    plot_ha = area_ha(geom)

    # --- municipalities intersecting the plot ---
    lookup, carbon = load("gemeinde_lookup.json"), load("carbon_flux_by_gemeinde.json")
    loss_g, meta = load("gemeinde_yearly_loss.json"), load("emissions_meta.json")
    yearly = {y: load(f"year_{y}.json") for y in YEARS}
    hits = []
    env = geom.GetEnvelope()  # minx,maxx,miny,maxy — cheap bbox prefilter before exact intersect
    for f in load("austria_gemeinden.geojson")["features"]:
        b = f.get("bbox")
        if b and (b[2] < env[0] or b[0] > env[1] or b[3] < env[2] or b[1] > env[3]):
            continue
        g = ogr.CreateGeometryFromJson(json.dumps(f["geometry"]))
        if g.Intersects(geom):
            ih = area_ha(g.Intersection(geom))
            if ih > 1e-6:
                hits.append((f["properties"], ih))
    if not hits:
        fail("plot outside Austria (no municipality intersected)", 3)
    hits.sort(key=lambda t: -t[1])

    munis = []
    for props, ih in hits:
        iso = props["iso"]; c = carbon["gemeinden"].get(iso, {})
        munis.append({
            "iso": iso, "name": props["name"], "state": lookup["states"].get(iso),
            "population": lookup["population"].get(iso),
            "plot_share": round(ih / plot_ha, 4) if plot_ha else 1.0,
            "forest_area_ha": c.get("forest_area_ha"),
            "forest_loss_total_2001_2024_ha": loss_g["gemeinden"].get(iso, {}).get("total_area_ha"),
            "carbon_net_flux_tCO2e_per_ha_2001_2024": c.get("net_flux_per_ha"),
            "is_net_carbon_source": c.get("is_net_source"),
        })
    main_iso, main_state = munis[0]["iso"], munis[0]["state"]

    # --- pixel-exact zonal stats: plot (+ rings unless fast) ---
    plot_z = zonal(geom)
    if fast:
        # HOLZ-3 contract: plot-only, no rings, no municipal timeline; cheap + concurrent.
        slim = None
        if plot_z:
            slim = {
                "area_ha": plot_z["area_ha"],
                "forest_area_2000_ha": plot_z["forest_area_2000_ha"],
                "forest_share_2000_pct": plot_z["forest_share_2000_pct"],
                "loss_ha_by_year": plot_z["loss_ha_by_year"],
                "loss_total": plot_z["loss_total_2001_2024_ha"],
                "loss_total_2001_2024_ha": plot_z["loss_total_2001_2024_ha"],
                "loss_pct_of_forest2000": plot_z["loss_pct_of_forest2000"],
                "net_flux_tco2e_ha": plot_z.get("net_flux_tCO2e_per_ha_2001_2024"),
                "gross_emissions": plot_z.get("gross_emissions_tCO2e_per_ha_2001_2024"),
                "gross_removals": plot_z.get("gross_removals_tCO2e_per_ha_2001_2024"),
            }
        print(json.dumps({
            "fast": True,
            "input": {"type": "point (100m buffer applied)" if point_input else "polygon", "plot_area_ha": round(plot_ha, 3)},
            "plot": slim,
            "municipality_codes": [m["iso"] for m in munis],
            "state": main_state,
            "units": {"loss_*": "ha", "net_flux_tco2e_ha/gross_*": "tCO2e per ha, cumulative 2001-2024 (negative = sink)"},
            "sources": {"forest_loss": "Hansen GFC-2024 v1.12, 30m; forest = treecover2000>=30%",
                        "carbon": "Harris et al. forest carbon flux, Mg CO2e/ha cumulative 2001-2024"},
        }))
        return
    ring1 = zonal(ring(geom, 0, 1000))
    ring5 = zonal(ring(geom, 1000, 5000))

    # --- unified timeline ---
    prices = load("timber_prices.json")
    spruce = prices["prices"].get("spruce_fir_2b", {}).get("yearly_avg", {})
    au_sum = meta["summary"]
    timeline = []
    for y in YEARS:
        row_m = yearly[y].get(main_iso)
        muni_fha = munis[0]["forest_area_ha"] or None
        entry = {
            "year": int(y),
            "plot": {"loss_ha": plot_z["loss_ha_by_year"].get(y) if plot_z else None},
            "surroundings_5km": {"loss_pct_of_forest": round(100 * ring5["loss_ha_by_year"][y] / ring5["forest_area_2000_ha"], 3) if ring5 and ring5["forest_area_2000_ha"] else None},
            "municipality": {
                "loss_ha": row_m[1] if row_m else None,
                "harvest_efm": row_m[2] if row_m else None,
                "harvest_value_eur": row_m[3] if row_m else None,
                "co2_tonnes": row_m[4] if row_m else None,
                "harvest_efm_per_forest_ha": round(row_m[2] / muni_fha, 2) if row_m and muni_fha else None,
            },
            "austria": {
                "harvest_efm": au_sum.get(y, {}).get("total_harvest_efm"),
                "loss_ha": au_sum.get(y, {}).get("total_loss_area_ha"),
            },
            "price_spruce_eur_efm": spruce.get(y),
        }
        if plot_z and plot_z["forest_area_2000_ha"]:
            entry["plot"]["loss_pct_of_forest"] = round(100 * entry["plot"]["loss_ha"] / plot_z["forest_area_2000_ha"], 3)
        timeline.append(entry)

    # --- comparison block (plot vs neighbourhood vs admin units) ---
    def rate(z):  # avg annual loss % of forest2000
        if not z or not z.get("forest_area_2000_ha"):
            return None
        return round(100 * z["loss_total_2001_2024_ha"] / z["forest_area_2000_ha"] / 24, 3)
    comparison = {
        "annual_forest_loss_rate_pct": {
            "plot": rate(plot_z), "ring_0_1km": rate(ring1), "ring_1_5km": rate(ring5),
            "municipality": round(100 * (munis[0]["forest_loss_total_2001_2024_ha"] or 0) /
                                  (munis[0]["forest_area_ha"] or 1) / 24, 3) if munis[0]["forest_area_ha"] else None,
        },
        "carbon_net_flux_tCO2e_per_ha_2001_2024": {
            "plot": plot_z.get("net_flux_tCO2e_per_ha_2001_2024") if plot_z else None,
            "ring_1_5km": ring5.get("net_flux_tCO2e_per_ha_2001_2024") if ring5 else None,
            "municipality": munis[0]["carbon_net_flux_tCO2e_per_ha_2001_2024"],
        },
        "interpretation": "negative net flux = net sink. Loss rate above surroundings suggests recent harvest/disturbance on the plot; below suggests conservation or younger stand.",
    }

    print(json.dumps({
        "input": {"type": "point (100m buffer applied)" if point_input else "polygon", "plot_area_ha": round(plot_ha, 3)},
        "plot": plot_z,
        "surroundings": {"ring_0_1km": ring1, "ring_1_5km": ring5},
        "comparison": comparison,
        "municipalities": munis,
        "state": main_state,
        "timeline": timeline,
        "timber_prices_eur_per_efm": prices,
        "regional_prices": regional_prices(main_state),
        "sources": {
            "forest_loss": "Hansen GFC-2024 v1.12, 30m; loss year 2001-2024; forest = treecover2000>=30%",
            "carbon": "Harris et al. forest carbon flux, Mg CO2e/ha cumulative 2001-2024",
            "harvest": "Statistik Austria Holzeinschlag (state totals) downscaled to municipalities via Hansen loss shares",
            "prices": "preise.agrarforschung.at, EUR/Efm sawlog",
        },
        "caveats": [
            "Hansen 'loss' includes harvest, windthrow, bark beetle salvage — not only deforestation.",
            "Municipal harvest values are model-downscaled, not measured locally.",
            "30m pixels: plot-level values for areas <5 ha are indicative only.",
        ],
    }))

if __name__ == "__main__":
    main()
    sys.stdout.flush()
    import os
    os._exit(0)  # skip SWIG destructor noise on stdout
