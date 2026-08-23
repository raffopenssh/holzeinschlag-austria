#!/usr/bin/env python3
"""
Fetch the FULL wood price catalogue from preise.agrarforschung.at
(32 content pages, ~280 series) into data/timber_price_catalog.json.

Structure:
{
  "updated_at": ..., "source": ...,
  "pages": { <contentId>: { title, source, area, series: {
      <attr_code>: { name, unit, first, last, latest_value,
                     yearly_avg: {YYYY: v}, monthly: {YYYY-MM: v} } } } },
  "state_pages": { "Steiermark": {"sawlog": "saegerundholz_stmk", ...}, ... }
}
Notes: *_von/_bis attr codes are price-range bounds of their base series.
Atomic write; exits non-zero (keeping old file) on failure.
"""
import json, sys, tempfile, urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / "data"
OUT = DATA / "timber_price_catalog.json"
BASE = "https://preise.agrarforschung.at/api"

STATES = {"bgld": "Burgenland", "ktn": "Kärnten", "noe": "Niederösterreich",
          "ooe": "Oberösterreich", "sbg": "Salzburg", "stmk": "Steiermark",
          "t": "Tirol", "vbg": "Vorarlberg"}
KINDS = {"saegerundholz": "sawlog", "industrierundholz": "industrial_roundwood",
         "brennholz": "fuelwood"}
NATIONAL = ["saegeholz_monatl", "industrierundholz_monatl", "brennholz_monatl",
            "energieholzkodex", "pellets", "holzprodukte",
            "schnittholz_chicago", "schnittholz_schwedische_kiefer"]

def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "holzeinschlag-at forestry context"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)

def main():
    pages_ids = list(NATIONAL)
    state_pages = defaultdict(dict)
    for kind, label in KINDS.items():
        for ab, state in STATES.items():
            cid = f"{kind}_{ab}"
            pages_ids.append(cid)
            state_pages[state][label] = cid

    pages, failures = {}, []
    for cid in pages_ids:
        try:
            d = get(f"{BASE}/getcontent?contentId={cid}")["data"]
            attrs_meta = d.get("attributes", {})
            series = {}
            for rec in d.get("records", []):
                if rec.get("value") is None:
                    continue
                s = series.setdefault(rec["attr_code"], {
                    "name": attrs_meta.get(rec["attr_code"], {}).get("name"),
                    "unit": rec.get("unit_name"), "monthly": {}})
                s["monthly"][rec["ts_start"][:7]] = float(rec["value"])
            for code, s in series.items():
                months = sorted(s["monthly"])
                yearly = defaultdict(list)
                for m in months:
                    yearly[m[:4]].append(s["monthly"][m])
                s.update(first=months[0], last=months[-1],
                         latest_value=s["monthly"][months[-1]],
                         yearly_avg={y: round(sum(v) / len(v), 2) for y, v in sorted(yearly.items())})
                s["monthly"] = {m: s["monthly"][m] for m in months}
            area = d.get("areas") or {}
            area_name = next(iter(area.values()), {}).get("name") if area else None
            pages[cid] = {"title": d.get("title"), "source": d.get("source"),
                          "area": area_name, "series": series}
        except Exception as e:
            failures.append(f"{cid}: {e}")

    if len(pages) < 25:  # sanity: expect ~32
        print(f"ERROR: only {len(pages)} pages fetched; failures: {failures}", file=sys.stderr)
        sys.exit(1)

    out = {
        "source": "preise.agrarforschung.at (Statistik Austria, LK Landwirtschaftskammern, WIFO/HWWI/CME)",
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "notes": [
            "attr codes ending in _von/_bis are lower/upper bounds of the base series' price range",
            "units vary: EUR/FM (solid m3), EURO/M3, EUR/RM (stacked m3), Cent/kg, EURO/T, USD/1000 Board Feet, index",
            "LK state series = chamber-reported ranges; STAT national series = Statistik Austria averages",
        ],
        "failures": failures,
        "pages": pages,
        "state_pages": dict(state_pages),
    }
    with tempfile.NamedTemporaryFile("w", dir=DATA, suffix=".tmp", delete=False) as f:
        json.dump(out, f)
        tmp = f.name
    Path(tmp).replace(OUT)
    n = sum(len(p["series"]) for p in pages.values())
    print(f"OK: {len(pages)} pages, {n} series -> {OUT.name}"
          + (f" (failures: {failures})" if failures else ""), file=sys.stderr)

if __name__ == "__main__":
    main()
