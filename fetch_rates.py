"""
fetch_rates.py
BIS bulk CSV 다운로드로 중앙은행 정책금리 수집 → data/rates.json 저장
FRED API로 미국·EU·캐나다 보완
"""

import json, os, io, zipfile, csv, requests
from datetime import datetime, timezone, timedelta

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
START        = "2016-01"
OUT          = "data/rates.json"

# BIS REF_AREA 코드 → 대시보드 코드
BIS_TO_DASH = {
    "US":"US", "XM":"EU", "GB":"UK", "JP":"JP",
    "CA":"CA", "AU":"AU", "CH":"CH", "SE":"SE",
    "NO":"NO", "NZ":"NZ", "KR":"KR", "CN":"CN",
    "IN":"IN", "MX":"MX", "BR":"BR", "ZA":"ZA",
    "ID":"ID",
}

# ── BIS bulk CSV ───────────────────────────────────────────────────────
def fetch_bis_csv():
    url = "https://data.bis.org/bulkdownload/WS_CBPOL_csv_flat.zip"
    print(f"[BIS] Downloading {url} ...")
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
    except Exception as e:
        print(f"[BIS] Download failed: {e}")
        return {}

    try:
        z        = zipfile.ZipFile(io.BytesIO(r.content))
        csv_name = next(n for n in z.namelist() if n.endswith(".csv"))
        raw      = z.read(csv_name).decode("utf-8")
    except Exception as e:
        print(f"[BIS] ZIP parse failed: {e}")
        return {}

    reader  = csv.DictReader(io.StringIO(raw))
    by_code = {}

    for row in reader:
        freq     = row.get("FREQ", "")
        ref_area = row.get("REF_AREA", "")
        time_str = row.get("TIME_PERIOD", "")
        val_str  = row.get("OBS_VALUE", "")

        if freq != "M" or not val_str or not time_str:
            continue
        if time_str < START:
            continue

        dash_code = BIS_TO_DASH.get(ref_area)
        if not dash_code:
            continue

        try:
            val = float(val_str)
        except ValueError:
            continue

        ym = time_str[:7]
        if dash_code not in by_code:
            by_code[dash_code] = {}
        by_code[dash_code][ym] = val

    result = {}
    for code, month_map in by_code.items():
        series = [{"x": k, "y": v} for k, v in sorted(month_map.items())]
        if series:
            result[code] = series
            print(f"[BIS] {code}: {len(series)} months, latest={series[-1]}")

    return result


# ── FRED supplement ────────────────────────────────────────────────────
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
        f"&file_type=json&observation_start={START}-01"
        f"&frequency=m&sort_order=asc"
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


# ── Stats helpers ──────────────────────────────────────────────────────
def extract_stats(series):
    if not series:
        return None
    current = series[-1]["y"]

    d3m     = datetime.now(timezone.utc) - timedelta(days=91)
    d3m_str = d3m.strftime("%Y-%m")
    cands   = [p for p in series if p["x"] <= d3m_str]
    prev3m  = cands[-1]["y"] if cands else series[0]["y"]

    last_change_date, last_change_bps = None, 0
    for i in range(len(series)-1, 0, -1):
        diff = round((series[i]["y"] - series[i-1]["y"]) * 100)
        if abs(diff) >= 1:
            last_change_date = series[i]["x"] + "-01"
            last_change_bps  = diff
            break

    return {
        "current":        current,
        "prev3m":         prev3m,
        "lastChangeDate": last_change_date,
        "lastChangeBps":  last_change_bps,
    }


# ── Main ───────────────────────────────────────────────────────────────
def main():
    os.makedirs("data", exist_ok=True)
    data = fetch_bis_csv()

    if FRED_API_KEY:
        print("\n[FRED] Supplementing US, EU, CA, UK ...")
        for code, sid in FRED_SERIES.items():
            series = fetch_fred(sid)
            if series:
                data[code] = series
                print(f"[FRED] {code}: {len(series)} months, latest={series[-1]}")
    else:
        print("[FRED] No API key — skipping")

    out = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rates":   {}
    }
    for code, series in data.items():
        stats = extract_stats(series)
        if not stats:
            continue
        out["rates"][code] = {"series": series, **stats}

    with open(OUT, "w") as f:
        json.dump(out, f, separators=(",", ":"))

    print(f"\nWrote {OUT} — {len(out['rates'])} countries: {sorted(out['rates'].keys())}")


if __name__ == "__main__":
    main()
