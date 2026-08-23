#!/usr/bin/env python3
"""
Refresh data/timber_prices.json from preise.agrarforschung.at.

Source: GET https://preise.agrarforschung.at/api/getcontent?contentId=saegeholz_monatl
(Statistik Austria sawlog prices, monthly since 2010, EUR/m3 = EUR/Efm).
Writes atomically; keeps existing file on any error. Logs to stderr.
"""
import json, sys, tempfile, urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / "data"
OUT = DATA / "timber_prices.json"
URL = "https://preise.agrarforschung.at/api/getcontent?contentId=saegeholz_monatl"

# attr_code -> our key + display names
MAP = {
    "at_blochholz_bu_klb3_monat":      ("beech",         "Buche Kl. B3",        "Blochholz Buche Kl. B 3"),
    "at_blochholz_ki_klb2aplus_monat": ("pine",          "Kiefer Kl. B 2a+",    "Blochholz Kiefer Kl. B 2a+"),
    "at_blochholz_fita_klbmed2b_monat":("spruce_fir_2b", "Fichte/Tanne B Media 2b", "Blochholz Fichte/Tanne Kl. B, Media 2b"),
    "at_blochholz_fita_klb3a_monat":   ("spruce_fir_3a", "Fichte/Tanne B 3a",   "Blochholz Fichte/Tanne Kl. B 3a"),
}

def main():
    req = urllib.request.Request(URL, headers={"User-Agent": "holzeinschlag-at forestry context (contact via exe.dev)"})
    with urllib.request.urlopen(req, timeout=60) as r:
        payload = json.load(r)
    recs = payload["data"]["records"]

    series = defaultdict(dict)  # key -> {ts: value}
    for rec in recs:
        m = MAP.get(rec["attr_code"])
        if m and rec["value"] is not None:
            series[m[0]][rec["ts_start"][:7]] = float(rec["value"])

    if len(series) < len(MAP):
        print(f"ERROR: expected {len(MAP)} series, got {list(series)}", file=sys.stderr)
        sys.exit(1)

    prices = {}
    for code, (key, name, name_de) in MAP.items():
        s = series[key]
        months = sorted(s)
        if len(months) < 100:  # sanity: ~16 years of monthly data expected
            print(f"ERROR: series {key} too short ({len(months)})", file=sys.stderr)
            sys.exit(1)
        yearly = defaultdict(list)
        for mth in months:
            yearly[mth[:4]].append(s[mth])
        prices[key] = {
            "name": name, "name_de": name_de,
            "latest_price": s[months[-1]], "latest_date": months[-1],
            "yearly_avg": {y: round(sum(v) / len(v), 2) for y, v in sorted(yearly.items())},
            "monthly": {m: s[m] for m in months},
        }

    out = {
        "source": "preise.agrarforschung.at",
        "source_dataset": "saegeholz_monatl (Statistik Austria)",
        "unit": "EUR/Efm",
        "note": "Blochholz (sawlog) prices. Schadholz/Industrieholz fetches ~30-50% of sawlog prices.",
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "prices": prices,
    }
    with tempfile.NamedTemporaryFile("w", dir=DATA, suffix=".tmp", delete=False) as f:
        json.dump(out, f)
        tmp = f.name
    Path(tmp).replace(OUT)
    latest = {k: (v["latest_date"], v["latest_price"]) for k, v in prices.items()}
    print(f"OK: updated {OUT.name}: {latest}", file=sys.stderr)

if __name__ == "__main__":
    main()
