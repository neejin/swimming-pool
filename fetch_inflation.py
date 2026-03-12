"""
fetch_inflation.py — BLS CPI + BEA PCE + WTI oil → data/inflation.json
Runs via GitHub Actions daily. Requires FRED_API_KEY secret.
"""
import json, os, requests, math
from datetime import datetime, timezone

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
START        = "2016-01-01"
OUT          = "data/inflation.json"

SERIES = {
    "cpi_raw": "CPIAUCSL",   # BLS CPI All Urban Consumers (index level)
    "pce_raw": "PCEPI",      # BEA PCE Price Index (index level)
    "oil":     "DCOILWTICO", # WTI Crude Oil (monthly average, $/bbl) — for CPI projection
}


def fetch_fred(series_id, frequency="m"):
    if not FRED_API_KEY:
        print(f"[FRED] No API key — skipping {series_id}")
        return []
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={FRED_API_KEY}"
        f"&file_type=json&observation_start={START}&frequency={frequency}&sort_order=asc"
    )
    try:
        r = requests.get(url, timeout=25)
        r.raise_for_status()
        data = r.json()
        if data.get("error_message"):
            print(f"[FRED] {series_id}: {data['error_message']}")
            return []
        result = []
        for o in data["observations"]:
            if o["value"] == ".":
                continue
            try:
                val = float(o["value"])
            except ValueError:
                continue
            if math.isnan(val):
                continue
            date_str = o["date"][:10] if frequency in ("d", "w") else o["date"][:7]
            result.append({"x": date_str, "y": round(val, 4)})
        print(f"[FRED] {series_id}: {len(result)} months, latest={result[-1] if result else 'none'}")
        return result
    except Exception as e:
        print(f"[FRED] {series_id}: {e}")
        return []


def to_yoy(series):
    """Convert index level series to YoY % change series."""
    by_ym = {d["x"]: d["y"] for d in series}
    result = []
    for d in series:
        yr, mo = d["x"][:4], d["x"][5:7]
        prev_ym = f"{int(yr)-1}-{mo}"
        if prev_ym not in by_ym:
            continue
        pct = (d["y"] / by_ym[prev_ym] - 1) * 100
        result.append({"x": d["x"], "y": round(pct, 3)})
    return result


def main():
    os.makedirs("data", exist_ok=True)

    print("=== fetch_inflation.py ===")
    print(f"FRED_API_KEY present: {bool(FRED_API_KEY)}")

    cpi_raw   = fetch_fred("CPIAUCSL")
    pce_raw   = fetch_fred("PCEPI")
    oil       = fetch_fred("DCOILWTICO")           # monthly — for CPI projection
    oil_daily = fetch_fred("DCOILWTICO", frequency="d")  # daily — for chart
    rbob      = fetch_fred("GASREGCOVW", frequency="w")  # weekly

    cpi_yoy = to_yoy(cpi_raw)
    pce_yoy = to_yoy(pce_raw)

    print(f"CPI YoY: {len(cpi_yoy)} months, latest={cpi_yoy[-1] if cpi_yoy else 'none'}")
    print(f"PCE YoY: {len(pce_yoy)} months, latest={pce_yoy[-1] if pce_yoy else 'none'}")
    print(f"WTI monthly: {len(oil)} months, latest={oil[-1] if oil else 'none'}")
    print(f"WTI daily:   {len(oil_daily)} days,   latest={oil_daily[-1] if oil_daily else 'none'}")
    print(f"RBOB weekly: {len(rbob)} weeks,   latest={rbob[-1] if rbob else 'none'}")

    out = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cpi": cpi_yoy,
        "pce": pce_yoy,
        "oil": oil,           # monthly, for CPI projection
        "oil_daily": oil_daily[-756:],  # last ~3 years daily
        "rbob": rbob[-156:],  # last ~3 years weekly
    }

    with open(OUT, "w") as f:
        json.dump(out, f, separators=(",", ":"))
    print(f"\nWrote {OUT}")
    print(f"  CPI: {len(cpi_yoy)} records")
    print(f"  PCE: {len(pce_yoy)} records")
    print(f"  Oil monthly: {len(oil)} records")
    print(f"  Oil daily:   {len(oil_daily[-180:])} records")
    print(f"  RBOB weekly: {len(rbob[-40:])} records")


if __name__ == "__main__":
    main()
