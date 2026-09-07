"""
fetch_fred_excel.py — FRED API에서 경제지표를 받아 엑셀(xlsx)로 저장

사용법:
    pip install requests pandas openpyxl
    export FRED_API_KEY="발급받은 키"   # https://fred.stlouisfed.org/docs/api/api_key.html
    python fetch_fred_excel.py

SERIES 딕셔너리에 "FRED series ID": "엑셀 시트/컬럼에 쓸 이름"을 추가하면
그 지표도 함께 받아옵니다. (series ID는 https://fred.stlouisfed.org 에서 검색)
"""
import os
import sys
from datetime import datetime, timezone

import pandas as pd
import requests

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
START = "2016-01-01"
OUT = "fred_data.xlsx"

# 예시 지표 4개 — 필요에 맞게 자유롭게 추가/삭제하세요.
SERIES = {
    "FEDFUNDS": "기준금리(effective federal funds rate)",
    "CPIAUCSL": "소비자물가지수 CPI (index)",
    "UNRATE": "실업률(%)",
    "DCOILWTICO": "WTI 유가($/bbl)",
}


def fetch_fred(series_id: str) -> pd.DataFrame:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": START,
        "sort_order": "asc",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if data.get("error_message"):
        raise RuntimeError(f"{series_id}: {data['error_message']}")

    rows = [
        {"date": o["date"], "value": float(o["value"])}
        for o in data["observations"]
        if o["value"] != "."
    ]
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df


def main():
    if not FRED_API_KEY:
        sys.exit(
            "FRED_API_KEY 환경변수가 없습니다. "
            "https://fred.stlouisfed.org/docs/api/api_key.html 에서 발급받아 설정하세요."
        )

    sheets = {}
    summary_rows = []

    for series_id, label in SERIES.items():
        print(f"[FRED] {series_id} ({label}) 요청 중...")
        try:
            df = fetch_fred(series_id)
        except Exception as e:
            print(f"[FRED] {series_id} 실패: {e}")
            continue

        if df.empty:
            print(f"[FRED] {series_id}: 데이터 없음")
            continue

        sheets[series_id] = df
        summary_rows.append(
            {
                "series_id": series_id,
                "지표명": label,
                "최신 날짜": df["date"].iloc[-1].strftime("%Y-%m-%d"),
                "최신 값": df["value"].iloc[-1],
                "관측치 수": len(df),
            }
        )
        print(f"[FRED] {series_id}: {len(df)}건, 최신값={df['value'].iloc[-1]}")

    if not sheets:
        sys.exit("받아온 데이터가 없습니다. API 키와 series ID를 확인하세요.")

    summary_df = pd.DataFrame(summary_rows)
    summary_df.insert(0, "업데이트 시각(UTC)", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

    with pd.ExcelWriter(OUT, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        for series_id, df in sheets.items():
            label = SERIES[series_id]
            df.to_excel(writer, sheet_name=series_id[:31], index=False, header=["날짜", label])

    print(f"\n완료: {OUT} 에 {len(sheets)}개 시트 저장됨 (Summary 포함 {len(sheets) + 1}개)")


if __name__ == "__main__":
    main()
