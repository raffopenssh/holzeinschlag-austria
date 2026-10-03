#!/usr/bin/env python3
"""Build data/kg_gemeinde.json: Katastralgemeinde (kg_code) -> gemeinde_code.

Source of truth: umfeld-at.exe.xyz (formerly cadastre-process-api) EDM register lookup (/api/v1/lookup?q=<gemeinde_code>&type=kg),
per the sibling integration spec — never maintain our own code tables. One request per Gemeinde in gemeinde_lookup.json.
Idempotent: existing entries kept; only missing Gemeinden are (re)fetched.
"""
import json, sys, time, urllib.request, urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "kg_gemeinde.json"
BASE = "https://umfeld-at.exe.xyz/api/v1/lookup"

lookup = json.load(open(ROOT / "data" / "gemeinde_lookup.json"))
gemeinden = sorted(lookup["names"])
prev = json.load(open(OUT)) if OUT.exists() else {"kg": {}, "done_gemeinden": []}
kg, done = prev["kg"], set(prev.get("done_gemeinden", []))

def fetch(code):
    url = f"{BASE}?q={code}&type=kg&limit=200"
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                return json.load(r).get("data", [])
        except Exception as e:
            time.sleep(2 * (attempt + 1))
            err = e
    print("FAIL", code, err, file=sys.stderr)
    return None

for i, g in enumerate(gemeinden):
    if g in done:
        continue
    rows = fetch(g)
    if rows is None:
        continue
    for r in rows:
        kg[str(r["kg_code"]).zfill(5)] = {"g": str(r["gemeinde_code"]).zfill(5), "n": r.get("name")}
    done.add(g)
    if i % 100 == 0:
        print(f"{i}/{len(gemeinden)} kgs={len(kg)}", file=sys.stderr)
        tmp = OUT.with_suffix(".tmp")
        json.dump({"kg": kg, "done_gemeinden": sorted(done)}, open(tmp, "w"), separators=(",", ":"))
        tmp.replace(OUT)
    time.sleep(0.05)

out = {"source": "umfeld-at.exe.xyz /api/v1/lookup?type=kg (BEV EDM register)",
       "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
       "kg_count": len(kg), "kg": kg, "done_gemeinden": sorted(done)}
tmp = OUT.with_suffix(".tmp")
json.dump(out, open(tmp, "w"), separators=(",", ":"), ensure_ascii=False)
tmp.replace(OUT)
print(f"done: {len(kg)} KGs, {len(done)}/{len(gemeinden)} Gemeinden", file=sys.stderr)
