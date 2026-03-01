#!/usr/bin/env python3
import io
import json
import os
import re
import time
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta

import gspread
from gspread.exceptions import APIError
import openpyxl
import pandas as pd
import requests
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
MARKET_NAME = os.getenv("MARKET_NAME", "台北一")
TIMEZONE = os.getenv("TIMEZONE", "Asia/Taipei")
SKIP_SHEETS = set(x.strip() for x in os.getenv("WORKSHEET_SKIP", "Setting,(空白模板_水果),(空白模板_蔬菜)").split(",") if x.strip())
START_DATE_ROC = os.getenv("START_DATE_ROC", "").strip()
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
TEMPLATE_XLSX = os.getenv("TEMPLATE_XLSX", "").strip()
SSL_VERIFY = os.getenv("SSL_VERIFY", "true").lower() != "false"


def normalize_text(v):
    if v is None:
        return ""
    s = str(v)
    s = s.replace("\u3000", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_key(v):
    return re.sub(r"\s+", "", normalize_text(v)).upper()


def parse_roc_date(s):
    m = re.fullmatch(r"(\d{2,4})/(\d{1,2})/(\d{1,2})", normalize_text(s))
    if not m:
        raise ValueError(f"invalid ROC date: {s}")
    y_raw, mth_raw, d_raw = m.groups()
    y = int(y_raw.lstrip("0") or "0")
    mth = int(mth_raw)
    d = int(d_raw)
    return date(y + 1911, mth, d)


def parse_date_to_roc_key(s):
    t = normalize_text(s)
    if not t:
        return None, None
    try:
        d = parse_roc_date(t)
        return to_roc_date(d), d
    except Exception:
        pass
    m = re.fullmatch(r"(\d{4})/(\d{1,2})/(\d{1,2})", t)
    if m:
        y, mth, d = map(int, m.groups())
        gd = date(y, mth, d)
        return to_roc_date(gd), gd
    return None, None


def to_roc_date(d):
    return f"{d.year - 1911:03d}/{d.month:02d}/{d.day:02d}"


def get_taipei_today():
    utc_now = datetime.now(UTC)
    return (utc_now + timedelta(hours=8)).date()


def build_session():
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


def hidden_value(html, name):
    m = re.search(rf'name="{re.escape(name)}"[^>]*value="([^"]*)"', html)
    return m.group(1) if m else ""


def fetch_selector_products(selector_url):
    session = build_session()
    resp = session.get(selector_url, timeout=30, verify=SSL_VERIFY)
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


def fetch_excel_rows(url, start_roc, end_roc, product_codes=None, product_labels=None):
    session = build_session()
    resp = session.get(url, timeout=30, verify=SSL_VERIFY)
    resp.raise_for_status()
    html = resp.text
    product_codes = [c for c in (product_codes or []) if c]

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
        "ctl00$contentPlaceHolder$hfldProductNo": ",".join(product_codes),
        "ctl00$contentPlaceHolder$hfldProductType": "",
        "ctl00$contentPlaceHolder$btnXls": "下載Excel",
    }
    xls = session.post(url, data=payload, timeout=60, verify=SSL_VERIFY)
    xls.raise_for_status()

    df = pd.read_excel(io.BytesIO(xls.content), header=4)
    # Normalize column names like 日　　期 -> 日期
    colmap = {c: normalize_key(c) for c in df.columns}

    def pick_col(target):
        for c, k in colmap.items():
            if k == target:
                return c
        return None

    c_date = pick_col("日期")
    c_market = pick_col("市場")
    c_prod = pick_col("產品")
    c_high = pick_col("上價")
    c_mid = pick_col("中價")
    c_low = pick_col("下價")
    c_avg = pick_col("平均價(元/公斤)")
    c_chg = pick_col("增減%")
    c_qty = pick_col("交易量(公斤)")
    # The second 增減% column becomes 增減%.1
    c_chg_qty = None
    for c in df.columns:
        if normalize_key(c).startswith("增減%") and c != c_chg:
            c_chg_qty = c
            break

    required = [c_date, c_market, c_prod, c_high, c_mid, c_low, c_avg, c_chg, c_qty]
    if any(c is None for c in required):
        raise RuntimeError(f"unexpected columns from {url}: {list(df.columns)}")

    rows = []
    for _, r in df.iterrows():
        d = normalize_text(r.get(c_date))
        p = normalize_text(r.get(c_prod))
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
                r.get(c_high),
                r.get(c_mid),
                r.get(c_low),
                r.get(c_avg),
                normalize_text(r.get(c_chg)),
                r.get(c_qty),
                normalize_text(r.get(c_chg_qty)) if c_chg_qty else "",
            ]
        )
    return rows


def worksheet_mapping_from_template(path):
    wb = openpyxl.load_workbook(path, data_only=True)
    mapping = {}
    by_title = {}
    product_codes = set()
    for ws in wb.worksheets:
        if ws.title in SKIP_SHEETS:
            continue
        a1 = normalize_text(ws["A1"].value)
        if a1:
            mapping[normalize_key(a1)] = ws.title
            code = a1.split(" ")[0]
            if code:
                product_codes.add(code)
        by_title[normalize_key(ws.title)] = ws.title
    return mapping, by_title, product_codes


def worksheet_mapping_from_gsheet(sheet):
    mapping = {}
    by_title = {}
    existing_dates = {}
    max_dt = None
    product_codes = set()
    for ws in sheet.worksheets():
        if ws.title in SKIP_SHEETS:
            continue
        a1 = normalize_text(ws.acell("A1").value)
        if a1:
            mapping[normalize_key(a1)] = ws
            code = a1.split(" ")[0]
            if code:
                product_codes.add(code)
        by_title[normalize_key(ws.title)] = ws

        dates = set()
        col_a = ws.col_values(1)
        for s in col_a[5:]:
            s = normalize_text(s)
            if not s:
                continue
            try:
                d = parse_roc_date(s)
            except Exception:
                continue
            dates.add(s)
            if max_dt is None or d > max_dt:
                max_dt = d
        existing_dates[ws.title] = dates
    return mapping, by_title, existing_dates, max_dt, product_codes


def split_product_code(product_text):
    s = normalize_text(product_text)
    if not s:
        return "", ""
    parts = s.split(" ", 1)
    if len(parts) == 1:
        return parts[0], parts[0]
    return parts[0], s


def read_product_config(sheet):
    ws = sheet.worksheet("品項")
    rows = read_with_retry(lambda: ws.get_all_values())
    if not rows:
        return []

    header = [normalize_text(h) for h in rows[0]]
    idx = {name: i for i, name in enumerate(header)}
    required = ["啟用", "分類", "分頁名稱", "品項代號", "品項完整名稱"]
    for k in required:
        if k not in idx:
            raise RuntimeError("品項 分頁缺少欄位: " + ", ".join(required))

    out = []
    for r in rows[1:]:
        enabled = normalize_text(r[idx["啟用"]] if idx["啟用"] < len(r) else "").upper()
        if enabled not in {"Y", "YES", "TRUE", "1"}:
            continue
        cat = normalize_text(r[idx["分類"]] if idx["分類"] < len(r) else "").lower()
        sheet_name = normalize_text(r[idx["分頁名稱"]] if idx["分頁名稱"] < len(r) else "")
        code = normalize_text(r[idx["品項代號"]] if idx["品項代號"] < len(r) else "")
        full_name = normalize_text(r[idx["品項完整名稱"]] if idx["品項完整名稱"] < len(r) else "")
        if not sheet_name or not code:
            continue
        out.append({
            "category": cat,
            "sheet_name": sheet_name,
            "code": code,
            "full_name": full_name or code,
        })
    return out


def get_existing_dates_and_max(sheet, sheet_names):
    existing_dates = {}
    max_dt = None
    ws_map = read_with_retry(lambda: {w.title: w for w in sheet.worksheets()})
    for name in sheet_names:
        ws = ws_map.get(name)
        if ws is None:
            existing_dates[name] = set()
            continue
        col_a = read_with_retry(lambda: ws.col_values(1))
        dates = set()
        for s in col_a[5:]:
            roc_key, d = parse_date_to_roc_key(s)
            if not roc_key or d is None:
                continue
            dates.add(roc_key)
            if max_dt is None or d > max_dt:
                max_dt = d
        existing_dates[name] = dates
        time.sleep(0.25)
    return existing_dates, max_dt


def ensure_product_sheet(sheet, title, a1_text):
    try:
        ws = sheet.worksheet(title)
        return ws
    except Exception:
        ws = sheet.add_worksheet(title=title, rows=2000, cols=20)
        write_with_retry(
            lambda: ws.update(
                "A1:J5",
                [
                    [a1_text, "", "", "", "", "", "", "", "", ""],
                    ["交易日期：", "", "", "", "", "", "", "", "", ""],
                    ["市 場：", "台北一", "", "", "", "", "", "", "", ""],
                    ["產 品：", "全部產品", "", "", "", "", "", "", "", ""],
                    ["日 期", "市 場", "產 品", "上價", "中價", "下價", "平均價(元/公斤)", "增減%", "交易量(公斤)", "增減%"],
                ],
            )
        )
        return ws


def write_with_retry(fn, tries=8, base_sleep=1.5):
    last_err = None
    for i in range(tries):
        try:
            return fn()
        except APIError as e:
            last_err = e
            msg = str(e)
            if "429" not in msg and "Quota" not in msg and "quota" not in msg:
                raise
            time.sleep(base_sleep * (2 ** min(i, 4)))
    if last_err:
        raise last_err


def read_with_retry(fn, tries=8, base_sleep=1.5):
    last_err = None
    for i in range(tries):
        try:
            return fn()
        except APIError as e:
            last_err = e
            msg = str(e)
            if "429" not in msg and "Quota" not in msg and "quota" not in msg:
                raise
            time.sleep(base_sleep * (2 ** min(i, 4)))
    if last_err:
        raise last_err


def route_sheet(product, map_by_a1, map_by_title):
    k = normalize_key(product)
    if k in map_by_a1:
        return map_by_a1[k]

    # fallback: match by sheet name included in product text
    for title_k, ws in map_by_title.items():
        if title_k and title_k in k:
            return ws
    return None


def main():
    today = get_taipei_today()

    if DRY_RUN:
        if not TEMPLATE_XLSX:
            raise RuntimeError("DRY_RUN=true requires TEMPLATE_XLSX path")
        map_by_a1, map_by_title, needed_product_codes = worksheet_mapping_from_template(TEMPLATE_XLSX)
        if START_DATE_ROC:
            start_dt = parse_roc_date(START_DATE_ROC)
        else:
            start_dt = date(today.year, 1, 1)
    else:
        sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
        sa_json = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT") or "").strip()
        if not sheet_id or not sa_json:
            raise RuntimeError("missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT")

        client = gspread.service_account_from_dict(json.loads(sa_json))
        sheet = client.open_by_key(sheet_id)
        product_cfg = read_product_config(sheet)
        code_to_sheet = {x["code"]: x for x in product_cfg}
        needed_product_codes = set(code_to_sheet.keys())
        existing_dates, max_dt = get_existing_dates_and_max(sheet, sorted(set(x["sheet_name"] for x in product_cfg)))
        map_by_a1 = {}
        map_by_title = {}

        if START_DATE_ROC:
            start_dt = parse_roc_date(START_DATE_ROC)
        elif max_dt is None:
            start_dt = date(today.year, 1, 1)
        else:
            start_dt = max_dt + timedelta(days=1)

    end_dt = today
    if start_dt > end_dt:
        print(json.dumps({"ok": True, "message": "no new date range", "start": str(start_dt), "end": str(end_dt)}, ensure_ascii=False))
        return

    start_roc = to_roc_date(start_dt)
    end_roc = to_roc_date(end_dt)

    veg_options = fetch_selector_products(VEG_SELECTOR_URL)
    fruit_options = fetch_selector_products(FRUIT_SELECTOR_URL)
    if DRY_RUN:
        veg_codes = sorted(c for c in needed_product_codes if c in veg_options)
        fruit_codes = sorted(c for c in needed_product_codes if c in fruit_options)
    else:
        veg_codes = sorted(c for c in needed_product_codes if c in veg_options and code_to_sheet[c]["category"] in {"", "veg"})
        fruit_codes = sorted(c for c in needed_product_codes if c in fruit_options and code_to_sheet[c]["category"] in {"", "fruit"})

    veg_rows = fetch_excel_rows(VEG_URL, start_roc, end_roc, veg_codes, [veg_options[c] for c in veg_codes])
    fruit_rows = fetch_excel_rows(FRUIT_URL, start_roc, end_roc, fruit_codes, [fruit_options[c] for c in fruit_codes])
    all_rows = veg_rows + fruit_rows

    grouped = defaultdict(list)
    unknown_products = set()

    for row in all_rows:
        if DRY_RUN:
            ws = route_sheet(row[2], map_by_a1, map_by_title)
        else:
            code, full_product = split_product_code(row[2])
            cfg = code_to_sheet.get(code)
            ws = cfg["sheet_name"] if cfg else None
        if ws is None:
            unknown_products.add(row[2])
            continue

        ws_title = ws if isinstance(ws, str) else ws.title

        if DRY_RUN:
            grouped[ws_title].append(row)
            continue

        row_key, _ = parse_date_to_roc_key(row[0])
        if row_key and row_key in existing_dates.get(ws_title, set()):
            continue
        grouped[ws_title].append(row)

    if DRY_RUN:
        print(json.dumps({
            "ok": True,
            "mode": "dry-run",
            "range": [start_roc, end_roc],
            "fetched_rows": len(all_rows),
            "requested_product_count": {"veg": len(veg_codes), "fruit": len(fruit_codes)},
            "routed_worksheets": len(grouped),
            "unknown_products": sorted(unknown_products)[:20],
            "worksheet_counts": {k: len(v) for k, v in grouped.items()},
        }, ensure_ascii=False, indent=2))
        return

    updated = {}
    for title, rows in grouped.items():
        if not rows:
            continue
        rows.sort(key=lambda x: x[0])
        cfg = None
        for v in code_to_sheet.values():
            if v["sheet_name"] == title:
                cfg = v
                break
        a1_text = cfg["full_name"] if cfg else title
        ws = ensure_product_sheet(sheet, title, a1_text)
        write_with_retry(lambda: ws.append_rows(rows, value_input_option="RAW"))
        time.sleep(1.2)
        updated[title] = len(rows)

    print(json.dumps({
        "ok": True,
        "range": [start_roc, end_roc],
        "fetched_rows": len(all_rows),
        "requested_product_count": {"veg": len(veg_codes), "fruit": len(fruit_codes)},
        "updated_worksheets": updated,
        "unknown_products": sorted(unknown_products),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
