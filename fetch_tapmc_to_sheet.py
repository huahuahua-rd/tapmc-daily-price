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
        pieces = 