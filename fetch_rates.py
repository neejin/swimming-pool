"""
fetch_rates.py — BIS bulk CSV + FRED → data/rates.json
BIS WS_CBPOL은 일별(D) 데이터로 저장됨 → 월별로 집계
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
    url = "https://data.bis.org/static/bulk/WS_CBPOL_csv_flat.zip"
    print(f"[BIS] GET {url}")
    try:
        r = requests.get(url, timeout=90)
        print(f"[BIS] HTTP {r.status_code}, size={len(r.content):,} bytes")
        r.raise_for_status()
    except Exception as e:
        print(f"[BIS] Download error: {e}")
        return {}

    try:
        z        = zipfile.ZipFile(io.BytesIO(r.content))
        csv_name = next((n for n in z.namelist() if n.endswith(".csv")), None)
        print(f"[BIS] ZIP contents: {z.namelist()}")
        if not csv_name:
            print("[BIS] No CSV in ZIP")
            return {}
        raw = z.read(csv_name).decode("utf-8")
    except Exception as e:
        print(f"[BIS] ZIP error: {e}")
        return {}

    reader = csv.DictReader(io.StringIO(raw))
    print(f"[BIS] Columns: {reader.fieldnames}")

    # key: dash_code → { "YYYY-MM": last_value_in_month }
    by_code = {}
    rows_printed = 0

    for row in reader:
        # 컬럼명 대소문자 무관하게 처리
        row_lower = {k.strip().upper(): v.strip() for k, v in row.items()}

        ref_area = row_lower.get("REF_AREA", "")
        time_str = row_lower.get("TIME_PERIOD", "")
        val_str  = row_lower.get("OBS_VALUE", "")
        freq     = row_lower.get("FREQ", "")

        if rows_printed < 3:
            print(f"[BIS] Sample: FREQ={freq} REF_AREA={ref_area} TIME={time_str} VAL={val_str}")
            rows_printed += 1

        # 값 없으면 스킵
        if not val_str or not time_str:
            continue

        # 날짜가 START 이전이면 스킵
        ym = time_str[:7]   # "2024-03" 또는 "2024-03-15" → "2024-03"
        if ym < START:
            continue

        dash_code = BIS_TO_DASH.get(ref_area)
        if not dash_code:
            continue

        try:
            val = float(val_str)
        except ValueError:
            continue

        # 월별로 마지막 값 유지 (일별 데이터면 월말 값이 남음)
        if dash_code not in by_code:
            by_code[dash_code] = {}
        by_code[dash_code][ym] = val

    result = {}
    for code, month_map in by_code.items():
        series = [{"x": k, "y": v} for k, v in sorted(month_map.items())]
        if series:
            result[code] = series
            print(f"[BIS] {code}: {len(series)} months, latest={series[-1]}")

    print(f"[BIS] Countries parsed: {sorted(result.keys())}")
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
        diff = (series[i]["y"] - series[i-1]["y"]) * 100
        # 중앙은행은 보통 25bp 단위로 금리 변경 → 10bp 이상 변화만 인식, 25bp 단위로 반올림
        if abs(diff) >= 10:
            rounded = round(diff / 25) * 25
            if rounded == 0:
                rounded = round(diff / 5) * 5  # 작은 경우 5bp 단위
            lcd, lc = series[i]["x"]+"-01", int(rounded)
            break
    return {"current":current,"prev3m":prev3m,"lastChangeDate":lcd,"lastChangeBps":lc}


def main():
    os.makedirs("data", exist_ok=True)

    # BIS로 전체 수집
    data = fetch_bis_csv()

    # FRED로 US/EU/CA 덮어쓰기 (더 정확한 월평균값)
    if FRED_API_KEY:
        print("\n[FRED] Supplementing US, EU, CA...")
        for code, sid in FRED_SERIES.items():
            s = fetch_fred(sid)
            if s:
                data[code] = s
                print(f"[FRED] {code}: {len(s)} months, latest={s[-1]}")
    else:
        print("[FRED] No API key")

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
