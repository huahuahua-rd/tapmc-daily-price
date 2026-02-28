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
0 9 * * * cd /Users/hushiyu/Documents/北農 && /Users/hushiyu/Documents/北農/.venv/bin/python /Users/hushiyu/Documents/北農/fetch_tapmc_to_sheet.py >> /Users/hushiyu/Documents/北農/cron.log 2>&1
```

## 6) 雲端版（GitHub Actions，不用開著 Mac）

專案已包含 workflow：
- [.github/workflows/tapmc-daily.yml](/Users/hushiyu/Documents/北農/.github/workflows/tapmc-daily.yml)

執行時間是台灣時間每天 09:00（UTC `01:00`）。

在 GitHub Repo 設定這兩個 Secrets：
- `GOOGLE_SHEET_ID`: 你的試算表 ID
- `GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT`: Service Account 整份 JSON 內容（整段貼上）

設定路徑：`Repo -> Settings -> Secrets and variables -> Actions -> New repository secret`

完成後：
1. 把目前資料夾 push 到 GitHub repository
2. 到 `Actions` 頁籤手動執行一次 `TAPMC Daily Price`（`Run workflow`）
3. 確認各品名分頁（`品名代號 品名`）有新增資料

## 7) AMIS 區間品項上價平均（蔬菜/水果）

腳本：`amis_range_avg_to_sheet.py`

### 需求
- Google Sheet 內 `B1:B2` 放區間一（開始/結束），`C1:C2` 放區間二（開始/結束）
- 市場：北市一
- 品項：全品項（蔬菜 + 水果）

### 執行

```bash
source .venv/bin/activate
python amis_range_avg_to_sheet.py
```

### 輸出格式（預設寫入第一個分頁）
- `A1:C1`：`區間 / 區間一開始 / 區間二開始`
- `A2:C2`：`區間 / 區間一結束 / 區間二結束`
- `A3:C3`：`品項 / 上價平均 / 上價平均`
- `A4:C...`：各品項的區間上價平均（對應 B、C 欄）

### 注意
- 若要指定分頁，請在 `.env` 設定 `WORKSHEET_NAME`
- 若要用環境變數日期而非 Sheet 內日期，請設定 `DATE_INPUT_SOURCE=env` 並填 `DATE_START`/`DATE_END`

### GitHub 手動執行
Workflow 名稱：`上價平均（區間）`

執行方式：
1. GitHub → `Actions`
2. 選擇 `上價平均（區間）`
3. 點 `Run workflow`

需要的 Secrets：
- `GOOGLE_SHEET_ID_RANGE_AVG`：區間試算表 ID
- `GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT`：Service Account JSON 內容（整段貼上）
