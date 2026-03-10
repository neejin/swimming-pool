"""
fetch_rates.py — BIS bulk CSV + FRED → data/rates.json
"""
import json, os, io, zipfile, csv, requests
from datetime import datetime, timezone, timedelta

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
START        = "2016-01"
OUT          = "data/rates.json"

BIS_TO_DASH = {
    "US":"US","XM":"EU","GB":"UK","JP":"JP","CA":"CA","AU":"AU",
    "CH":"CH","SE":"SE","NO":"NO","NZ":"NZ","KR":"KR","CN":"CN",
    "IN":"IN","MX":"MX","BR":"BR","ZA":"ZA","ID":"ID",
}

def fetch_bis_csv():
    url = "https://data.bis.org/bulkdownload/WS_CBPOL_csv_flat.zip"
    print(f"[BIS] GET {url}")
    try:
        r = requests.get(url, timeout=90)
        print(f"[BIS] HTTP {r.status_code}, size={len(r.content)} bytes")
        r.raise_for_status()
    except Exception as e:
        print(f"[BIS] Download error: {e}")
        return {}

    try:
        z        = zipfile.ZipFile(io.BytesIO(r.content))
        names    = z.namelist()
        print(f"[BIS] ZIP contents: {names}")
        csv_name = next((n for n in names if n.endswith(".csv")), None)
        if not csv_name:
            print("[BIS] No CSV found in ZIP")
            return {}
        raw = z.read(csv_name).decode("utf-8")
    except Exception as e:
        print(f"[BIS] ZIP error: {e}")
        return {}

    reader = csv.DictReader(io.StringIO(raw))
    # Print actual column names for debugging
    print(f"[BIS] CSV columns: {reader.fieldnames}")

    # Print first 3 rows to understand format
    rows_seen = 0
    by_code   = {}

    for row in reader:
        if rows_seen < 3:
            print(f"[BIS] Sample row {rows_seen}: {dict(row)}")
            rows_seen += 1

        # Handle both upper and lowercase column names
        freq     = (row.get("FREQ") or row.get("freq") or "").strip()
        ref_area = (row.get("REF_AREA") or row.get("ref_area") or "").strip()
        time_str = (row.get("TIME_PERIOD") or row.get("time_period") or row.get("date") or "").strip()
        val_str  = (row.get("OBS_VALUE") or row.get("obs_value") or row.get("value") or "").strip()

        if rows_seen <= 3:
            rows_seen += 1

        # Accept monthly (M) rows; BIS daily series summarised to monthly by last value
        if freq not in ("M", "m", ""):
            continue
        if not val_str or not time_str:
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

    print(f"[BIS] Total countries parsed: {sorted(result.keys())}")
    return result


FRED_SERIES = {"US":"FEDFUNDS","EU":"ECBDFR","CA":"IRSTCB01CAM156N","UK":"IUDSOIA"}

def fetch_fred(series_id):
    if not FRED_API_KEY:
        return []
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={FRED_API_KEY}"
        f"&file_type=json&observation_start={START}-01&frequency=m&sort_order=asc"
    )
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        if data.get("error_message"):
            print(f"[FRED] {series_id}: {data['error_message']}")
            return []
        return [{"x":o["date"][:7],"y":float(o["value"])} for o in data["observations"] if o["value"]!="."]
    except Exception as e:
        print(f"[FRED] {series_id}: {e}")
        return []


def extract_stats(series):
    if not series: return None
    current = series[-1]["y"]
    d3m     = (datetime.now(timezone.utc) - timedelta(days=91)).strftime("%Y-%m")
    cands   = [p for p in series if p["x"] <= d3m]
    prev3m  = cands[-1]["y"] if cands else series[0]["y"]
    lcd, lc = None, 0
    for i in range(len(series)-1, 0, -1):
        diff = round((series[i]["y"] - series[i-1]["y"]) * 100)
        if abs(diff) >= 1:
            lcd, lc = series[i]["x"]+"-01", diff
            break
    return {"current":current,"prev3m":prev3m,"lastChangeDate":lcd,"lastChangeBps":lc}


def main():
    os.makedirs("data", exist_ok=True)
    data = fetch_bis_csv()

    if FRED_API_KEY:
        print("\n[FRED] Supplementing US, EU, CA, UK...")
        for code, sid in FRED_SERIES.items():
            s = fetch_fred(sid)
            if s:
                data[code] = s
                print(f"[FRED] {code}: {len(s)} months, latest={s[-1]}")
    else:
        print("[FRED] No API key set")

    out = {"updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "rates":{}}
    for code, series in data.items():
        stats = extract_stats(series)
        if stats:
            out["rates"][code] = {"series": series, **stats}

    with open(OUT, "w") as f:
        json.dump(out, f, separators=(",",":"))
    print(f"\nWrote {OUT} — {len(out['rates'])} countries: {sorted(out['rates'].keys())}")

if __name__ == "__main__":
    main()
