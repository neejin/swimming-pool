"""
fetch_rates.py
BIS WS_CBPOL_D API로 중앙은행 정책금리를 수집해서 data/rates.json에 저장.
FRED로 보완 (US, EU, CA).
"""

import json, os, requests
from datetime import datetime, timezone, timedelta

# ── 설정 ──────────────────────────────────────────────────────────────
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")   # GitHub Secret에 저장
START = "2016-01-01"
OUT   = "data/rates.json"

# BIS country code → dashboard code
BIS_MAP = {
    "US": "US", "XM": "EU", "GB": "UK", "JP": "JP",
    "CA": "CA", "AU": "AU", "CH": "CH", "SE": "SE",
    "NO": "NO", "NZ": "NZ", "KR": "KR", "CN": "CN",
    "IN": "IN", "MX": "MX", "BR": "BR", "ZA": "ZA",
    "ID": "ID",
}

# ── BIS API ───────────────────────────────────────────────────────────
def fetch_bis():
    """BIS WS_CBPOL_D: daily central bank policy rates"""
    countries = "+".join(BIS_MAP.keys())
    url = (
        f"https://stats.bis.org/api/v1/data/WS_CBPOL_D/"
        f"D.{countries}.//?startPeriod={START}&format=jsondata"
    )
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return parse_bis(r.json())
    except Exception as e:
        print(f"[BIS] Error: {e}")
        return {}

def parse_bis(j):
    """SDMX-JSON → {code: [{x, y}, ...]}"""
    result = {}
    try:
        structure = j["structure"]
        # dimension position of REF_AREA (country)
        dims = structure["dimensions"]["series"]
        area_pos = next(i for i,d in enumerate(dims) if d["id"] == "REF_AREA")
        area_map = {str(v["id"]): v["name"] for v in dims[area_pos]["values"]}

        series = j["dataSets"][0]["series"]
        for key, sdata in series.items():
            parts    = key.split(":")
            bis_code = list(area_map.keys())[int(parts[area_pos])]
            dash_code = BIS_MAP.get(bis_code)
            if not dash_code:
                continue

            obs = sdata.get("observations", {})
            time_vals = j["structure"]["dimensions"]["observation"][0]["values"]
            points = []
            for idx_str, vals in obs.items():
                idx = int(idx_str)
                if vals[0] is None:
                    continue
                date_str = time_vals[idx]["id"]   # e.g. "2024-03-15"
                ym = date_str[:7]                  # "2024-03"
                points.append({"x": ym, "y": float(vals[0])})

            # Keep only last point per month (deduplicate)
            by_month = {}
            for p in sorted(points, key=lambda p: p["x"]):
                by_month[p["x"]] = p["y"]
            monthly = [{"x": k, "y": v} for k,v in sorted(by_month.items())]

            if monthly:
                result[dash_code] = monthly
                print(f"[BIS] {dash_code}: {len(monthly)} months, latest={monthly[-1]}")
    except Exception as e:
        print(f"[BIS] Parse error: {e}")
    return result

# ── FRED API (fallback / supplement) ─────────────────────────────────
FRED_SERIES = {
    "US": "FEDFUNDS",
    "EU": "ECBDFR",
    "CA": "IRSTCB01CAM156N",
    "UK": "IUDSOIA",
}

def fetch_fred(series_id):
    if not FRED_API_KEY:
        return []
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={FRED_API_KEY}"
        f"&file_type=json&observation_start={START}&frequency=m&sort_order=asc"
    )
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        if data.get("error_message"):
            print(f"[FRED] {series_id}: {data['error_message']}")
            return []
        return [
            {"x": o["date"][:7], "y": float(o["value"])}
            for o in data["observations"]
            if o["value"] != "."
        ]
    except Exception as e:
        print(f"[FRED] {series_id}: {e}")
        return []

# ── Stats helpers ─────────────────────────────────────────────────────
def extract_stats(series):
    if not series:
        return None
    current = series[-1]["y"]

    # 3개월 전
    d3m = datetime.now(timezone.utc) - timedelta(days=91)
    d3m_str = d3m.strftime("%Y-%m")
    prev_candidates = [p for p in series if p["x"] <= d3m_str]
    prev3m = prev_candidates[-1]["y"] if prev_candidates else series[0]["y"]

    # 마지막 변화
    last_change_date, last_change_bps = None, 0
    for i in range(len(series)-1, 0, -1):
        diff = round((series[i]["y"] - series[i-1]["y"]) * 100)
        if abs(diff) >= 1:
            last_change_date = series[i]["x"] + "-01"
            last_change_bps  = diff
            break

    return {
        "current": current,
        "prev3m":  prev3m,
        "lastChangeDate": last_change_date,
        "lastChangeBps":  last_change_bps,
    }

# ── Main ──────────────────────────────────────────────────────────────
def main():
    os.makedirs("data", exist_ok=True)

    print("Fetching BIS data...")
    data = fetch_bis()

    # FRED supplement for US/EU/CA/UK (more reliable for these)
    if FRED_API_KEY:
        print("Fetching FRED supplements...")
        for code, series_id in FRED_SERIES.items():
            series = fetch_fred(series_id)
            if series:
                data[code] = series
                print(f"[FRED] {code}: {len(series)} months, latest={series[-1]}")

    # Build output
    out = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rates": {}
    }

    for code, series in data.items():
        stats = extract_stats(series)
        if not stats:
            continue
        out["rates"][code] = {
            "series": series,
            **stats
        }

    with open(OUT, "w") as f:
        json.dump(out, f, separators=(",", ":"))

    print(f"\nWrote {OUT} with {len(out['rates'])} countries.")
    print("Countries:", sorted(out["rates"].keys()))

if __name__ == "__main__":
    main()
