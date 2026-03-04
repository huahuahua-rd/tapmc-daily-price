#!/usr/bin/env python3
import io
import json
import os
import re
import time
from datetime import UTC, date, datetime, timedelta
from typing import Dict, Iterable, List, Tuple

import gspread
import pandas as pd
import requests
import socket
from dotenv import load_dotenv
from gspread.exceptions import APIError, WorksheetNotFound
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

ITEM_WORKSHEET_NAME = os.getenv("ITEM_WORKSHEET_NAME", "item")
ITEM_TAB_COLUMN = int(os.getenv("ITEM_TAB_COLUMN", "3"))
ITEM_CODE_COLUMN = int(os.getenv("ITEM_CODE_COLUMN", "4"))
ITEM_ENABLE_COLUMN = int(os.getenv("ITEM_ENABLE_COLUMN", "1"))

DATE_FORMAT = (os.getenv("DATE_FORMAT", "ROC") or "ROC").strip().upper()
MAX_BACKTRACK_DAYS = int(os.getenv("MAX_BACKTRACK_DAYS", "60"))
MAX_DAYS_PER_RUN = int(os.getenv("MAX_DAYS_PER_RUN", "0"))

ALLOW_CREATE_TABS = os.getenv("ALLOW_CREATE_TABS", "true").strip().lower() in {"1", "true", "yes"}
ALLOW_NO_DATA = os.getenv("ALLOW_NO_DATA", "true").strip().lower() in {"1", "true", "yes"}

HEADER = [
    "日期",
    "市場",
    "產品",
    "上價",
    "中價",
    "下價",
    "平均價",
    "增減%",
    "交易量(公斤)",
    "增減%",
]


def normalize_text(v) -> str:
    if v is None:
        return ""
    s = str(v)
    s = s.replace("\u3000", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_key(v) -> str:
    s = normalize_text(v).upper()
    s = re.sub(r"\.\d+$", "", s)
    s = s.replace("%", "").replace("％", "")
    s = re.sub(r"[()（）]", "", s)
    s = re.sub(r"\s+", "", s)
    return s


def to_roc_date(d: date) -> str:
    return f"{d.year - 1911:03d}/{d.month:02d}/{d.day:02d}"


def parse_roc_date(s: str) -> date:
    m = re.fullmatch(r"(\d{2,3})[/-](\d{1,2})[/-](\d{1,2})", normalize_text(s))
    if not m:
        raise ValueError(f"unsupported date format: {s}")
    y, mth, d = map(int, m.groups())
    if y <= 300:
        y += 1911
    return date(y, mth, d)


def get_taipei_today() -> date:
    utc_now = datetime.now(UTC)
    return (utc_now + timedelta(hours=8)).date()


def build_session() -> requests.Session:
    if FORCE_IPV4:
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
        code = normalize_text(code).upper()
        label = normalize_text(label)
        if not code or code == "ALL":
            continue
        out[code] = label
    return out


def pick_column(df: pd.DataFrame, target: str) -> str | None:
    colmap = {c: normalize_key(c) for c in df.columns}
    target_key = normalize_key(target)
    for c, key in colmap.items():
        if key == target_key:
            return c
    return None


def pick_columns(df: pd.DataFrame, target: str) -> List[str]:
    target_key = normalize_key(target)
    return [c for c in df.columns if normalize_key(c) == target_key]


def pick_any_column(df: pd.DataFrame, targets: List[str]) -> str | None:
    for t in targets:
        c = pick_column(df, t)
        if c is not None:
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


def extract_code_from_product(product: str) -> str:
    text = normalize_text(product)
    if not text:
        return ""
    m = re.match(r"^([A-Za-z0-9]+)", text)
    return m.group(1).upper() if m else ""


def load_item_meta_from_ws(ws, tab_col: int, code_col: int, enable_col: int):
    max_col = max(tab_col, code_col, enable_col)
    max_letter = col_index_to_letter(max_col)
    rows = ws.get(f"A:{max_letter}")
    meta = []
    for i, row in enumerate(rows):
        if not row:
            continue
        enable = row[enable_col - 1] if len(row) >= enable_col else ""
        tab_name = row[tab_col - 1] if len(row) >= tab_col else ""
        code = row[code_col - 1] if len(row) >= code_col else ""
        if i == 0 and normalize_text(code).upper() in {"品名代號", "ITEM", "CODE", "品項", "ITEMCODE", "品號"}:
            continue
        if normalize_text(enable).upper() not in {"Y", "YES", "TRUE", "1"}:
            continue
        code = normalize_text(code).upper()
        tab_name = str(tab_name).strip()
        if not code or not tab_name:
            continue
        meta.append({"code": code, "tab": tab_name})
    return meta


def col_index_to_letter(index: int) -> str:
    if index < 1:
        raise ValueError("column index must be >= 1")
    letters = []
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def call_with_retry(fn, *, label: str, max_retries: int = 5, base_sleep: float = 10.0):
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except APIError as exc:
            message = str(exc)
            if "Quota exceeded" not in message and "429" not in message:
                raise
            if attempt >= max_retries:
                raise
            sleep_s = base_sleep * (2 ** attempt)
            print(f"{label} quota exceeded, retrying in {sleep_s:.0f}s (attempt {attempt + 1}/{max_retries})")
            time.sleep(sleep_s)


def get_or_create_item_worksheet(sheet, title: str):
    try:
        return sheet.worksheet(title)
    except WorksheetNotFound:
        if not ALLOW_CREATE_TABS:
            raise
        ws = call_with_retry(
            lambda: sheet.add_worksheet(title=title, rows=2000, cols=len(HEADER)),
            label=f"add_worksheet {title}",
        )
        call_with_retry(
            lambda: ws.append_row(HEADER, value_input_option="USER_ENTERED"),
            label=f"append_header {title}",
        )
        return ws


def get_last_date_from_ws(ws) -> date | None:
    values = call_with_retry(lambda: ws.col_values(1), label=f"get_col_a {ws.title}")
    for raw in reversed(values[1:]):
        if normalize_text(raw):
            try:
                return parse_roc_date(raw)
            except ValueError:
                continue
    return None


def append_rows(ws, rows):
    if not rows:
        return 0
    call_with_retry(
        lambda: ws.append_rows(rows, value_input_option="USER_ENTERED"),
        label=f"append_rows {ws.title}",
    )
    return len(rows)


def read_existing_keys(ws):
    values = call_with_retry(lambda: ws.get("A:C"), label=f"get_abc {ws.title}")
    keys = set()
    for r in values[1:]:
        d = normalize_text(r[0]) if len(r) > 0 else ""
        p = normalize_text(r[2]) if len(r) > 2 else ""
        if d and p:
            keys.add((d, p))
    return keys


def fetch_excel_rows_for_date(url: str, target_date: date, product_codes: List[str]) -> List[List[object]]:
    session = build_session()
    resp = session.get(url, timeout=REQUEST_TIMEOUT, verify=SSL_VERIFY)
    resp.raise_for_status()
    html = resp.text

    payload = {
        "__VIEWSTATE": hidden_value(html, "__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": hidden_value(html, "__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION": hidden_value(html, "__EVENTVALIDATION"),
        "ctl00$contentPlaceHolder$ucDateScope$rblDateScope": "D",
        "ctl00$contentPlaceHolder$ucSolarLunar$radlSolarLunar": "S",
        "ctl00$contentPlaceHolder$txtSTransDate": to_roc_date(target_date),
        "ctl00$contentPlaceHolder$txtETransDate": to_roc_date(target_date),
        "ctl00$contentPlaceHolder$txtMarket": MARKET_NAME,
        "ctl00$contentPlaceHolder$hfldMarketNo": MARKET_NO,
        "ctl00$contentPlaceHolder$txtProduct": "",
        "ctl00$contentPlaceHolder$hfldProductNo": ",".join(product_codes),
        "ctl00$contentPlaceHolder$hfldProductType": "",
        "ctl00$contentPlaceHolder$btnXls": "下載Excel",
    }
    xls = session.post(url, data=payload, timeout=REQUEST_TIMEOUT, verify=SSL_VERIFY)
    xls.raise_for_status()

    df = pd.read_excel(io.BytesIO(xls.content), header=4)
    c_date = pick_any_column(df, ["日期"])
    c_market = pick_any_column(df, ["市場"])
    c_product = pick_any_column(df, ["產品", "品名"])
    c_high = pick_any_column(df, ["上價"])
    c_mid = pick_any_column(df, ["中價"])
    c_low = pick_any_column(df, ["下價"])
    c_avg = pick_any_column(df, ["平均(元/公斤)", "平均價(元/公斤)", "平均價", "平均"])
    change_cols = pick_columns(df, "增減%") or pick_columns(df, "增減率")
    c_change = change_cols[0] if change_cols else None
    c_vol_change = change_cols[-1] if len(change_cols) > 1 else None
    c_volume = pick_any_column(df, ["交易量(公斤)", "交易量"])

    required = [c_date, c_market, c_product, c_high, c_mid, c_low, c_avg, c_change, c_volume]
    if any(c is None for c in required):
        raise RuntimeError(f"unexpected columns from {url}: {list(df.columns)}")

    rows: List[List[object]] = []
    for _, r in df.iterrows():
        d = normalize_text(r.get(c_date))
        p = normalize_text(r.get(c_product))
        mkt = normalize_text(r.get(c_market))
        if not d or not p:
            continue
        if not mkt.startswith(MARKET_NO):
            continue
        rows.append(
            [
                d,
                mkt,
                p,
                parse_number(r.get(c_high)),
                parse_number(r.get(c_mid)),
                parse_number(r.get(c_low)),
                parse_number(r.get(c_avg)),
                parse_number(r.get(c_change)),
                parse_number(r.get(c_volume)),
                parse_number(r.get(c_vol_change)) if c_vol_change else None,
            ]
        )
    return rows


def build_rows_by_tab(rows: List[List[object]], code_to_tab: Dict[str, str], allowed_codes: set) -> Dict[str, List[List[object]]]:
    out: Dict[str, List[List[object]]] = {}
    for row in rows:
        product = row[2]
        code = extract_code_from_product(product)
        if not code or code not in allowed_codes:
            continue
        tab = code_to_tab.get(code)
        if not tab:
            continue
        out.setdefault(tab, []).append(row)
    return out


def chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def main():
    load_dotenv()

    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    service_account_json_content = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT") or "").strip()
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    if not sheet_id:
        raise ValueError("missing GOOGLE_SHEET_ID")
    if not service_account_json and not service_account_json_content:
        raise ValueError("missing GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT")

    client = (
        gspread.service_account_from_dict(json.loads(service_account_json_content))
        if service_account_json_content
        else gspread.service_account(filename=service_account_json)
    )
    sheet = client.open_by_key(sheet_id)

    ws_item = sheet.worksheet(ITEM_WORKSHEET_NAME)
    item_meta = load_item_meta_from_ws(ws_item, ITEM_TAB_COLUMN, ITEM_CODE_COLUMN, ITEM_ENABLE_COLUMN)
    code_to_tab = {m["code"]: m["tab"] for m in item_meta}
    target_codes = sorted(code_to_tab.keys())
    if not target_codes:
        raise ValueError("no enabled item codes found")

    veg_codes = fetch_selector_products(VEG_SELECTOR_URL)
    fruit_codes = fetch_selector_products(FRUIT_SELECTOR_URL)

    veg_set = set(veg_codes.keys())
    fruit_set = set(fruit_codes.keys())

    veg_targets = [c for c in target_codes if c in veg_set]
    fruit_targets = [c for c in target_codes if c in fruit_set]
    missing_codes = [c for c in target_codes if c not in veg_set and c not in fruit_set]

    if missing_codes:
        print(json.dumps({"ok": False, "missing_codes": missing_codes}, ensure_ascii=False))

    today = get_taipei_today()

    # Determine latest available date by backtracking from today.
    latest_date = None
    daily_cache = {}
    for days_back in range(MAX_BACKTRACK_DAYS + 1):
        d = today - timedelta(days=days_back)
        rows = []
        if veg_targets:
            rows.extend(fetch_excel_rows_for_date(VEG_URL, d, veg_targets))
        if fruit_targets:
            rows.extend(fetch_excel_rows_for_date(FRUIT_URL, d, fruit_targets))
        if rows:
            daily_cache[d] = rows
            latest_date = d
            break
    if latest_date is None:
        msg = f"no data found within last {MAX_BACKTRACK_DAYS} days"
        if ALLOW_NO_DATA:
            print(json.dumps({"ok": False, "skipped": True, "error": msg}, ensure_ascii=False))
            return
        raise ValueError(msg)

    # Prepare worksheets and last dates
    ws_map = {}
    last_dates = {}
    for code, tab in code_to_tab.items():
        ws = get_or_create_item_worksheet(sheet, tab)
        ws_map[tab] = ws
    for tab, ws in ws_map.items():
        last_dates[tab] = get_last_date_from_ws(ws)

    # Determine start date per tab
    min_start = None
    for tab, last_dt in last_dates.items():
        if last_dt is None:
            start_dt = latest_date
        else:
            start_dt = last_dt + timedelta(days=1)
        if min_start is None or start_dt < min_start:
            min_start = start_dt

    if min_start is None or min_start > latest_date:
        print(
            json.dumps(
                {
                    "ok": True,
                    "date": to_roc_date(latest_date),
                    "appended": 0,
                    "updated_worksheets": {},
                    "missing_codes": missing_codes,
                },
                ensure_ascii=False,
            )
        )
        return

    # Backfill day by day
    updated = {}
    for tab, ws in ws_map.items():
        existing_keys = read_existing_keys(ws)
        updated[tab] = 0
        last_dt = last_dates[tab]
        start_dt = latest_date if last_dt is None else last_dt + timedelta(days=1)
        if start_dt > latest_date:
            continue

        days_to_fetch = (latest_date - start_dt).days + 1
        if MAX_DAYS_PER_RUN > 0:
            days_to_fetch = min(days_to_fetch, MAX_DAYS_PER_RUN)
        for i in range(days_to_fetch):
            d = start_dt + timedelta(days=i)
            if d in daily_cache:
                rows = daily_cache[d]
            else:
                rows = []
                if veg_targets:
                    rows.extend(fetch_excel_rows_for_date(VEG_URL, d, veg_targets))
                if fruit_targets:
                    rows.extend(fetch_excel_rows_for_date(FRUIT_URL, d, fruit_targets))
                daily_cache[d] = rows

            rows_by_tab = build_rows_by_tab(rows, code_to_tab, set(target_codes))
            rows_for_tab = rows_by_tab.get(tab, [])
            if not rows_for_tab:
                continue

            deduped = []
            for r in rows_for_tab:
                key = (normalize_text(r[0]), normalize_text(r[2]))
                if key in existing_keys:
                    continue
                existing_keys.add(key)
                deduped.append(r)
            if not deduped:
                continue
            updated[tab] += append_rows(ws, deduped)

    appended_rows = sum(updated.values())
    print(
        json.dumps(
            {
                "ok": True,
                "date": to_roc_date(latest_date),
                "appended": appended_rows,
                "updated_worksheets": updated,
                "missing_codes": missing_codes,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
