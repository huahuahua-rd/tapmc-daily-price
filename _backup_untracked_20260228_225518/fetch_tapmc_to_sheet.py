#!/usr/bin/env python3
import json
import os
import re
import sys
from datetime import datetime, timedelta
from io import StringIO

import gspread
import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from gspread.exceptions import WorksheetNotFound

REQUIRED_FIELDS = {
    "code": ["品名代號", "代號"],
    "name": ["品名"],
    "variety": ["品種"],
    "high": ["上價"],
    "mid": ["中價"],
    "low": ["下價"],
}


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\u3000", " ")
    text = re.sub(r"\s+", "", text)
    return text


def parse_number(value: object):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        n = float(value)
        return int(n) if n.is_integer() else n

    text = str(value).strip()
    if not text:
        return None

    match = re.search(r"-?\d+(?:,\d{3})*(?:\.\d+)?", text)
    if not match:
        return None

    n = float(match.group(0).replace(",", ""))
    return int(n) if n.is_integer() else n


def fetch_query_result_html(url: str, date_roc: str, category: str, fv_code: str, market: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    with requests.Session() as session:
        resp = session.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        if not resp.encoding:
            resp.encoding = resp.apparent_encoding or "utf-8"

        soup = BeautifulSoup(resp.text, "html.parser")
        form = soup.find("form", id="form1") or soup.find("form")
        if form is None:
            raise ValueError("找不到查詢表單")

        payload = {}
        for inp in form.find_all("input"):
            name = inp.get("name")
            if not name:
                continue
            payload[name] = inp.get("value", "")

        payload["__EVENTTARGET"] = "ctl00$ContentPlaceHolder1$btnQuery"
        payload["__EVENTARGUMENT"] = ""
        payload["ctl00$ContentPlaceHolder1$txtDate"] = date_roc
        payload["ctl00$ContentPlaceHolder1$DDL_Category"] = category
        payload["ctl00$ContentPlaceHolder1$DDL_FV_Code"] = fv_code
        payload["ctl00$ContentPlaceHolder1$DDL_Market"] = market

        post_resp = session.post(url, data=payload, headers=headers, timeout=30)
        post_resp.raise_for_status()
        if not post_resp.encoding:
            post_resp.encoding = post_resp.apparent_encoding or "utf-8"
        return post_resp.text


def parse_tables(html: str):
    try:
        return pd.read_html(StringIO(html), flavor=["lxml", "bs4", "html5lib"])
    except ValueError:
        return []


def flatten_columns(table: pd.DataFrame) -> pd.DataFrame:
    if isinstance(table.columns, pd.MultiIndex):
        table.columns = [
            "_".join([str(x).strip() for x in col if str(x).strip() and str(x).strip().lower() != "nan"])
            for col in table.columns
        ]
    else:
        table.columns = [str(c).strip() for c in table.columns]
    return table


def detect_field_map(columns):
    normalized_cols = {c: normalize_text(c) for c in columns}
    field_map = {}
    used_cols = set()

    for field, candidates in REQUIRED_FIELDS.items():
        candidate_norms = [normalize_text(c) for c in candidates]

        # 1) 優先精準匹配（避免「品名」誤匹配到「品名代號」）
        exact_matches = [
            col
            for col, norm_col in normalized_cols.items()
            if col not in used_cols and any(norm_col == cand for cand in candidate_norms)
        ]
        if exact_matches:
            field_map[field] = exact_matches[0]
            used_cols.add(exact_matches[0])
            continue

        # 2) 退而求其次用包含匹配，並加上防呆條件
        contains_matches = []
        for col, norm_col in normalized_cols.items():
            if col in used_cols:
                continue
            if not any(cand in norm_col for cand in candidate_norms):
                continue

            # 品名欄不得抓到品名代號欄
            if field == "name" and "代號" in norm_col:
                continue

            contains_matches.append(col)

        if not contains_matches:
            return None

        field_map[field] = contains_matches[0]
        used_cols.add(contains_matches[0])

    return field_map


def extract_records_from_html(html: str):
    records = {}
    tables = parse_tables(html)

    for table in tables:
        table = flatten_columns(table)
        table = table.dropna(how="all")
        if table.empty:
            continue

        field_map = detect_field_map(table.columns.tolist())
        if not field_map:
            continue

        for _, row in table.iterrows():
            code = normalize_text(row[field_map["code"]]).upper()
            if not code:
                continue

            record = {
                "code": code,
                "name": str(row[field_map["name"]]).strip() if pd.notna(row[field_map["name"]]) else "",
                "variety": str(row[field_map["variety"]]).strip() if pd.notna(row[field_map["variety"]]) else "",
                "high": parse_number(row[field_map["high"]]),
                "mid": parse_number(row[field_map["mid"]]),
                "low": parse_number(row[field_map["low"]]),
            }
            if code not in records:
                records[code] = record

    return records


def parse_query_combos(raw: str):
    combos = []
    for part in raw.split(","):
        seg = part.strip()
        if not seg:
            continue
        pieces = [p.strip() for p in seg.split(":")]
        if len(pieces) != 3:
            raise ValueError(f"QUERY_COMBOS 格式錯誤: {seg}，正確格式為 category:fv_code:market")
        combos.append((pieces[0], pieces[1], pieces[2]))
    if not combos:
        raise ValueError("QUERY_COMBOS 不能為空")
    return combos


def roc_date_from_gregorian(dt) -> str:
    roc_year = dt.year - 1911
    return f"{roc_year:03d}/{dt.month:02d}/{dt.day:02d}"


def parse_roc_date(text: str):
    m = re.fullmatch(r"(\d{2,3})/(\d{1,2})/(\d{1,2})", text.strip())
    if not m:
        raise ValueError("QUERY_DATE_ROC 格式錯誤，請用 YYY/MM/DD，例如 115/02/24")
    roc_year, month, day = map(int, m.groups())
    return datetime(roc_year + 1911, month, day).date()


def get_client(service_account_json_path: str, service_account_json_content: str):
    if service_account_json_content:
        # 雲端執行時可直接用 secret 內容建立憑證
        info = json.loads(service_account_json_content)
        return gspread.service_account_from_dict(info)
    return gspread.service_account(filename=service_account_json_path)


def load_item_codes(sheet, worksheet_name: str, item_column: int):
    ws = sheet.worksheet(worksheet_name)
    values = ws.col_values(item_column)

    codes = []
    for i, v in enumerate(values):
        code = normalize_text(v).upper()
        if not code:
            continue
        if i == 0 and code in {"品名代號", "ITEM", "CODE", "品項", "ITEMCODE"}:
            continue
        codes.append(code)
    return codes


def sanitize_worksheet_title(text: str) -> str:
    cleaned = re.sub(r'[\[\]:*?/\\]', " ", text or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        cleaned = "未命名"
    return cleaned[:100]


def worksheet_title_for_record(record) -> str:
    return sanitize_worksheet_title(f'{record["code"]} {record["name"]}')


def get_or_create_item_worksheet(sheet, title: str):
    try:
        return sheet.worksheet(title)
    except WorksheetNotFound:
        ws = sheet.add_worksheet(title=title, rows=2000, cols=7)
        ws.append_row(
            ["日期", "品名代號", "品名", "品種", "上價", "中價", "下價"],
            value_input_option="USER_ENTERED",
        )
        return ws


def append_rows_by_worksheet(sheet, rows_by_worksheet):
    updated = {}
    for title, rows in rows_by_worksheet.items():
        if not rows:
            continue
        ws = get_or_create_item_worksheet(sheet, title)
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        updated[title] = len(rows)
    return updated


def main():
    load_dotenv()

    source_url = os.getenv("SOURCE_URL", "https://www.tapmc.com.tw/Pages/Trans/Price1")
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    service_account_json_content = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT") or "").strip()
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    timezone_name = os.getenv("TIMEZONE", "Asia/Taipei")

    item_worksheet_name = os.getenv("ITEM_WORKSHEET_NAME", "item")
    item_column = int(os.getenv("ITEM_COLUMN", "1"))

    query_combos_raw = os.getenv(
        "QUERY_COMBOS",
        "1:V:1,1:F:1,2:V:1,2:V:2,2:F:1,2:F:2",
    )
    query_date_roc_override = (os.getenv("QUERY_DATE_ROC") or "").strip()
    max_backtrack_days = int(os.getenv("MAX_BACKTRACK_DAYS", "10"))

    missing = []
    if not service_account_json and not service_account_json_content:
        missing.append("GOOGLE_SERVICE_ACCOUNT_JSON 或 GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT")
    if not sheet_id:
        missing.append("GOOGLE_SHEET_ID")
    if missing:
        raise ValueError(f"缺少必要環境變數: {', '.join(missing)}")

    tz = pytz.timezone(timezone_name)
    now = datetime.now(tz)

    if query_date_roc_override:
        start_date = parse_roc_date(query_date_roc_override)
        target_date_str = start_date.strftime("%Y-%m-%d")
    else:
        start_date = now.date()
        target_date_str = now.strftime("%Y-%m-%d")

    gc = get_client(service_account_json, service_account_json_content)
    sh = gc.open_by_key(sheet_id)

    target_codes = load_item_codes(sh, item_worksheet_name, item_column)
    if not target_codes:
        raise ValueError(f"{item_worksheet_name} 分頁第 {item_column} 欄沒有可用代號")

    combos = parse_query_combos(query_combos_raw)
    last_html = ""
    rows_to_append_by_sheet = {}
    missing_codes = []
    used_query_date_roc = None
    backtracked_days = 0

    for days_back in range(max_backtrack_days + 1):
        query_date = start_date - timedelta(days=days_back)
        query_date_roc = roc_date_from_gregorian(query_date)
        all_records = {}

        for category, fv_code, market in combos:
            html = fetch_query_result_html(
                source_url,
                date_roc=query_date_roc,
                category=category,
                fv_code=fv_code,
                market=market,
            )
            last_html = html
            records = extract_records_from_html(html)
            for code, rec in records.items():
                if code not in all_records:
                    all_records[code] = rec

        if not all_records:
            continue

        candidate_rows_by_sheet = {}
        candidate_missing_codes = []
        for code in target_codes:
            rec = all_records.get(code)
            if not rec:
                candidate_missing_codes.append(code)
                continue

            sheet_title = worksheet_title_for_record(rec)
            row = [
                target_date_str,
                rec["code"],
                rec["name"],
                rec["variety"],
                rec["high"],
                rec["mid"],
                rec["low"],
            ]
            candidate_rows_by_sheet.setdefault(sheet_title, []).append(row)

        if candidate_rows_by_sheet:
            rows_to_append_by_sheet = candidate_rows_by_sheet
            missing_codes = candidate_missing_codes
            used_query_date_roc = query_date_roc
            backtracked_days = days_back
            break

    if not rows_to_append_by_sheet:
        debug_path = os.path.abspath("debug_tapmc_response.html")
        with open(debug_path, "w", encoding="utf-8") as f:
            f.write(last_html)
        raise ValueError(
            f"在最近 {max_backtrack_days} 天內都找不到可用資料 (已儲存 {debug_path})"
        )

    updated_worksheets = append_rows_by_worksheet(sh, rows_to_append_by_sheet)
    appended_rows = sum(updated_worksheets.values())

    print(
        json.dumps(
            {
                "ok": True,
                "date": target_date_str,
                "used_query_date_roc": used_query_date_roc,
                "backtracked_days": backtracked_days,
                "item_codes": len(target_codes),
                "appended": appended_rows,
                "missing_codes": missing_codes,
                "updated_worksheets": updated_worksheets,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)
