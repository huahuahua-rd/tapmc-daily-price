#!/usr/bin/env python3
import io
import json
import os
import re
from datetime import UTC, date, datetime, timedelta
from typing import Dict, Iterable, List, Tuple

import gspread
import pandas as pd
import requests
import socket
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

VEG_URL = os.getenv("SOURCE_VEG_URL", "https://amis.afa.gov.tw/veg/VegProdDayTransInfo.aspx")
FRUIT_URL = os.getenv("SOURCE_FRUIT_URL", "https://amis.afa.gov.tw/fruit/FruitProdDayTransInfo.aspx")
VEG_SELECTOR_URL = os.getenv(
    "SOURCE_VEG_SELECTOR_URL",
    "https://amis.afa.gov.tw/Selector/VegProductSelector.aspx?textField=ctl00_contentPlaceHolder_txtProduct&valueField=ctl00_contentPlaceHolder_hfldProductNo&productTypeField=ctl00_contentPlaceHolder_hfldProductType",
)
FRUIT_SELECTOR_URL = os.getenv(
    "SOURCE_FRUIT_SELECTOR_URL",
    "https://amis.afa.gov.tw/Selector/FruitProductSelector.aspx?textField=ctl00_contentPlaceHolder_txtProduct&valueField=ctl00_contentPlaceHolder_hfldProductNo&productTypeField=ctl00_contentPlaceHolder_hfldProductType",
)

MARKET_NO = os.getenv("MARKET_NO", "109")
MARKET_NAME = os.getenv("MARKET_NAME", "北市一")
TIMEZONE = os.getenv("TIMEZONE", "Asia/Taipei")
SSL_VERIFY = os.getenv("SSL_VERIFY", "true").lower() != "false"
FORCE_IPV4 = os.getenv("FORCE_IPV4", "true").lower() == "true"
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

DATE_INPUT_SOURCE = os.getenv("DATE_INPUT_SOURCE", "sheet").strip().lower()
DATE_START = os.getenv("DATE_START", "").strip()
DATE_END = os.getenv("DATE_END", "").strip()
DATE_RANGE1_START_CELL = os.getenv("DATE_RANGE1_START_CELL", "B1")
DATE_RANGE1_END_CELL = os.getenv("DATE_RANGE1_END_CELL", "B2")
DATE_RANGE2_START_CELL = os.getenv("DATE_RANGE2_START_CELL", "C1")
DATE_RANGE2_END_CELL = os.getenv("DATE_RANGE2_END_CELL", "C2")

WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "").strip()
OUTPUT_START_CELL = os.getenv("OUTPUT_START_CELL", "A1")
CLEAR_BEFORE_WRITE = os.getenv("CLEAR_BEFORE_WRITE", "true").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "120"))
MAX_PRODUCTS = int(os.getenv("MAX_PRODUCTS", "0"))
ALL_PRODUCTS = os.getenv("ALL_PRODUCTS", "true").lower() == "true"


REQUIRED_COLUMNS = {
    "date": ["日期"],
    "market": ["市場"],
    "product": ["產品"],
    "high": ["上價"],
}


def normalize_text(v) -> str:
    if v is None:
        return ""
    s = str(v)
    s = s.replace("\u3000", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_key(v) -> str:
    return re.sub(r"\s+", "", normalize_text(v)).upper()


def to_roc_date(d: date) -> str:
    return f"{d.year - 1911:03d}/{d.month:02d}/{d.day:02d}"


def get_taipei_today() -> date:
    utc_now = datetime.now(UTC)
    return (utc_now + timedelta(hours=8)).date()


def parse_date_loose(s: str, today: date) -> date:
    t = normalize_text(s)
    if not t:
        raise ValueError("date is empty")

    # ROC format YYY/MM/DD
    m = re.fullmatch(r"(\d{2,3})[/-](\d{1,2})[/-](\d{1,2})", t)
    if m:
        y, mth, d = map(int, m.groups())
        if y <= 300:
            return date(y + 1911, mth, d)
        return date(y, mth, d)

    # Gregorian YYYY/MM/DD or YYYY-MM-DD
    m = re.fullmatch(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", t)
    if m:
        y, mth, d = map(int, m.groups())
        return date(y, mth, d)

    # M/D or M-D, assume current year
    m = re.fullmatch(r"(\d{1,2})[/-](\d{1,2})", t)
    if m:
        mth, d = map(int, m.groups())
        return date(today.year, mth, d)

    raise ValueError(f"unsupported date format: {s}")


def build_session() -> requests.Session:
    if FORCE_IPV4:
        # Force IPv4 to avoid occasional IPv6 routing failures on GitHub runners.
        original_getaddrinfo = socket.getaddrinfo

        def _getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
            if host and host.endswith("amis.afa.gov.tw"):
                family = socket.AF_INET
            return original_getaddrinfo(host, port, family, type, proto, flags)

        socket.getaddrinfo = _getaddrinfo

    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    return s


def hidden_value(html: str, name: str) -> str:
    m = re.search(rf'name="{re.escape(name)}"[^>]*value="([^"]*)"', html)
    return m.group(1) if m else ""


def fetch_selector_products(selector_url: str) -> Dict[str, str]:
    session = build_session()
    resp = session.get(selector_url, timeout=REQUEST_TIMEOUT, verify=SSL_VERIFY)
    resp.raise_for_status()
    html = resp.text
    out = {}
    for code, label in re.findall(r'<option value="([^"]+)">([^<]+)</option>', html):
        code = normalize_text(code)
        label = normalize_text(label)
        if not code or code == "ALL":
            continue
        out[code] = label
    return out


def pick_column(df: pd.DataFrame, target: str) -> str | None:
    colmap = {c: normalize_key(c) for c in df.columns}
    for c, key in colmap.items():
        if key == target:
            return c
    return None


def parse_number(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = normalize_text(v)
    if not s:
        return None
    m = re.search(r"-?\d+(?:,\d{3})*(?:\.\d+)?", s)
    if not m:
        return None
    return float(m.group(0).replace(",", ""))


def fetch_excel_rows(
    url: str,
    start_roc: str,
    end_roc: str,
    product_codes: List[str],
    product_labels: List[str],
) -> List[Tuple[str, str, float]]:
    session = build_session()
    resp = session.get(url, timeout=REQUEST_TIMEOUT, verify=SSL_VERIFY)
    resp.raise_for_status()
    html = resp.text

    payload = {
        "__VIEWSTATE": hidden_value(html, "__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": hidden_value(html, "__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION": hidden_value(html, "__EVENTVALIDATION"),
        "ctl00$contentPlaceHolder$ucDateScope$rblDateScope": "P",
        "ctl00$contentPlaceHolder$ucSolarLunar$radlSolarLunar": "S",
        "ctl00$contentPlaceHolder$txtSTransDate": start_roc,
        "ctl00$contentPlaceHolder$txtETransDate": end_roc,
        "ctl00$contentPlaceHolder$txtMarket": MARKET_NAME,
        "ctl00$contentPlaceHolder$hfldMarketNo": MARKET_NO,
        "ctl00$contentPlaceHolder$txtProduct": ", ".join(product_labels or []),
        "ctl00$contentPlaceHolder$hfldProductNo": ",".join(product_codes or []),
        "ctl00$contentPlaceHolder$hfldProductType": "",
        "ctl00$contentPlaceHolder$btnXls": "下載Excel",
    }
    xls = session.post(url, data=payload, timeout=REQUEST_TIMEOUT, verify=SSL_VERIFY)
    xls.raise_for_status()

    df = pd.read_excel(io.BytesIO(xls.content), header=4)
    c_date = pick_column(df, "日期")
    c_market = pick_column(df, "市場")
    c_product = pick_column(df, "產品")
    c_high = pick_column(df, "上價")
    required = [c_date, c_market, c_product, c_high]
    if any(c is None for c in required):
        raise RuntimeError(f"unexpected columns from {url}: {list(df.columns)}")

    rows: List[Tuple[str, str, float]] = []
    for _, r in df.iterrows():
        d = normalize_text(r.get(c_date))
        p = normalize_text(r.get(c_product))
        mkt = normalize_text(r.get(c_market))
        if not d or not p:
            continue
        if not mkt.startswith(MARKET_NO):
            continue
        high = parse_number(r.get(c_high))
        if high is None:
            continue
        rows.append((d, p, high))
    return rows


def chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def get_client(service_account_json_path: str, service_account_json_content: str):
    if service_account_json_content:
        info = json.loads(service_account_json_content)
        return gspread.service_account_from_dict(info)
    return gspread.service_account(filename=service_account_json_path)


def read_date_range_from_sheet(sheet, start_cell: str, end_cell: str) -> Tuple[date, date]:
    ws = sheet.worksheet(WORKSHEET_NAME) if WORKSHEET_NAME else sheet.get_worksheet(0)
    start_raw = normalize_text(ws.acell(start_cell).value)
    end_raw = normalize_text(ws.acell(end_cell).value)
    today = get_taipei_today()
    if not start_raw or not end_raw:
        raise ValueError("sheet date cells are empty")
    start_dt = parse_date_loose(start_raw, today)
    end_dt = parse_date_loose(end_raw, today)
    return start_dt, end_dt


def compute_avg_by_product(rows: List[Tuple[str, str, float]]):
    sums: Dict[str, float] = {}
    counts: Dict[str, int] = {}
    for _, product, high in rows:
        key = product
        sums[key] = sums.get(key, 0.0) + high
        counts[key] = counts.get(key, 0) + 1
    out = []
    for product, total in sums.items():
        avg = total / counts[product]
        out.append((product, avg, counts[product]))
    return out


def compute_avg_by_product_for_range(
    url: str,
    start_dt: date,
    end_dt: date,
    product_codes: List[str],
    product_labels: List[str],
):
    start_roc = to_roc_date(start_dt)
    end_roc = to_roc_date(end_dt)
    rows = fetch_excel_rows(url, start_roc, end_roc, product_codes, product_labels)
    avg_rows = compute_avg_by_product(rows)
    return rows, avg_rows


def sort_products(items: List[Tuple[str, float, int]]):
    def sort_key(item):
        product = item[0]
        parts = product.split(" ", 1)
        code = parts[0]
        name = parts[1] if len(parts) > 1 else ""
        return (code, name)

    return sorted(items, key=sort_key)


def main():
    load_dotenv()

    today = get_taipei_today()
    if DATE_INPUT_SOURCE == "sheet" and not DRY_RUN:
        service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
        service_account_json_content = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT") or "").strip()
        sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
        if not sheet_id:
            raise ValueError("missing GOOGLE_SHEET_ID")
        if not service_account_json and not service_account_json_content:
            raise ValueError("missing GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT")
        client = get_client(service_account_json, service_account_json_content)
        sheet = client.open_by_key(sheet_id)
        range1_start_dt, range1_end_dt = read_date_range_from_sheet(sheet, DATE_RANGE1_START_CELL, DATE_RANGE1_END_CELL)
        range2_start_dt, range2_end_dt = read_date_range_from_sheet(sheet, DATE_RANGE2_START_CELL, DATE_RANGE2_END_CELL)
    else:
        if not DATE_START or not DATE_END:
            raise ValueError("DATE_START/DATE_END required when DATE_INPUT_SOURCE!=sheet or DRY_RUN=true")
        range1_start_dt = parse_date_loose(DATE_START, today)
        range1_end_dt = parse_date_loose(DATE_END, today)
        range2_start_dt = range1_start_dt
        range2_end_dt = range1_end_dt
        sheet = None

    if range1_start_dt > range1_end_dt:
        range1_start_dt, range1_end_dt = range1_end_dt, range1_start_dt
    if range2_start_dt > range2_end_dt:
        range2_start_dt, range2_end_dt = range2_end_dt, range2_start_dt

    veg_options = {}
    fruit_options = {}
    veg_codes: List[str] = []
    fruit_codes: List[str] = []
    if not ALL_PRODUCTS:
        veg_options = fetch_selector_products(VEG_SELECTOR_URL)
        fruit_options = fetch_selector_products(FRUIT_SELECTOR_URL)
        veg_codes = sorted(veg_options.keys())
        fruit_codes = sorted(fruit_options.keys())
        if MAX_PRODUCTS > 0:
            veg_codes = veg_codes[:MAX_PRODUCTS]
            fruit_codes = fruit_codes[:MAX_PRODUCTS]

    def collect_avg_for_range(start_dt: date, end_dt: date):
        all_rows: List[Tuple[str, str, float]] = []
        if ALL_PRODUCTS:
            rows, _avg = compute_avg_by_product_for_range(VEG_URL, start_dt, end_dt, [], [])
            all_rows.extend(rows)
            rows, _avg = compute_avg_by_product_for_range(FRUIT_URL, start_dt, end_dt, [], [])
            all_rows.extend(rows)
        else:
            for chunk in chunked(veg_codes, CHUNK_SIZE):
                labels = [veg_options[c] for c in chunk]
                rows, _avg = compute_avg_by_product_for_range(VEG_URL, start_dt, end_dt, chunk, labels)
                all_rows.extend(rows)
            for chunk in chunked(fruit_codes, CHUNK_SIZE):
                labels = [fruit_options[c] for c in chunk]
                rows, _avg = compute_avg_by_product_for_range(FRUIT_URL, start_dt, end_dt, chunk, labels)
                all_rows.extend(rows)
        avg_rows = compute_avg_by_product(all_rows)
        avg_rows = sort_products(avg_rows)
        return all_rows, avg_rows

    range1_rows, range1_avg = collect_avg_for_range(range1_start_dt, range1_end_dt)
    range2_rows, range2_avg = collect_avg_for_range(range2_start_dt, range2_end_dt)

    range1_map = {prod: round(avg, 2) for prod, avg, _ in range1_avg}
    range2_map = {prod: round(avg, 2) for prod, avg, _ in range2_avg}
    all_products = sorted(set(range1_map.keys()) | set(range2_map.keys()), key=lambda s: (s.split(" ", 1)[0], s.split(" ", 1)[1] if " " in s else ""))

    output_rows = [[prod, range1_map.get(prod, ""), range2_map.get(prod, "")] for prod in all_products]

    if DRY_RUN:
        print(
            json.dumps(
                {
                    "ok": True,
                    "mode": "dry-run",
                    "range1": [to_roc_date(range1_start_dt), to_roc_date(range1_end_dt)],
                    "range2": [to_roc_date(range2_start_dt), to_roc_date(range2_end_dt)],
                    "rows": {"range1": len(range1_rows), "range2": len(range2_rows)},
                    "product_count": len(all_products),
                    "sample": output_rows[:10],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    ws = sheet.worksheet(WORKSHEET_NAME) if WORKSHEET_NAME else sheet.get_worksheet(0)
    if CLEAR_BEFORE_WRITE:
        ws.clear()

    values = [
        ["區間", range1_start_dt.strftime("%Y/%m/%d"), range2_start_dt.strftime("%Y/%m/%d")],
        ["區間", range1_end_dt.strftime("%Y/%m/%d"), range2_end_dt.strftime("%Y/%m/%d")],
        ["品項", "上價平均", "上價平均"],
    ] + output_rows

    ws.update(OUTPUT_START_CELL, values, value_input_option="USER_ENTERED")

    print(
        json.dumps(
            {
                "ok": True,
                "range1": [to_roc_date(range1_start_dt), to_roc_date(range1_end_dt)],
                "range2": [to_roc_date(range2_start_dt), to_roc_date(range2_end_dt)],
                "product_count": len(all_products),
                "rows": {"range1": len(range1_rows), "range2": len(range2_rows)},
                "worksheet": ws.title,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
