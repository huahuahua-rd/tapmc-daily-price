# 北農每日行情寫入 Google Sheet

這個腳本會在執行時：
1. 到北農單日交易行情頁面送出「當日查詢」
2. 擷取表格欄位：`品名代號 / 品名 / 品種 / 上價 / 中價 / 下價`
3. 讀取 Google Sheet 的 `item` 分頁（預設 A 欄）代號清單
4. 依 `品名代號 + 品名` 自動建立分頁，並把命中的代號資料 append 到對應分頁，格式為 `日期 / 品名代號 / 品名 / 品種 / 上價 / 中價 / 下價`
5. 若當天休市無資料，會自動往前找最近有資料的日期（可設定回溯天數）

## 1) 安裝

```bash
cd /Users/hushiyu/Documents/北農
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) Google 權限

1. 啟用 Google Sheets API
2. 建立 Service Account，下載 JSON 金鑰
3. 把目標 Sheet 分享給該 `client_email`（編輯者）

## 3) 設定

```bash
cp .env.example .env
```

`.env` 主要欄位：

- `GOOGLE_SERVICE_ACCOUNT_JSON`: JSON 金鑰絕對路徑
- `GOOGLE_SHEET_ID`: 目標試算表 ID
- `ITEM_WORKSHEET_NAME`: 代號清單分頁（預設 `item`）
- `ITEM_COLUMN`: `item` 分頁哪一欄放代號（預設 `1` = A 欄）
- `QUERY_COMBOS`: 查詢組合，格式 `category:fv_code:market`
- `QUERY_DATE_ROC`: 指定查詢日期（民國格式 `YYY/MM/DD`，例如 `115/02/24`），留空表示查今天
- `MAX_BACKTRACK_DAYS`: 當天無資料時，最多往前回溯幾天（預設 `10`）

預設 `QUERY_COMBOS` 已同時嘗試蔬菜/水果與不同市場。

## 4) 手動測試

```bash
source .venv/bin/activate
python fetch_tapmc_to_sheet.py
```

成功會輸出：
- `appended`: 本次新增筆數
- `missing_codes`: 在 item 裡有，但本次採用日期找不到的代號
- `used_query_date_roc`: 實際抓資料的日期（休市時可能早於今天）

## 5) 每天早上 9:00（cron）

```bash
crontab -e
```

加入：

```cron
0 9 * * * cd /Use
