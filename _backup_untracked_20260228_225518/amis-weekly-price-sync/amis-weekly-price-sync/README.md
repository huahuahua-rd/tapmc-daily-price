# AMIS 每週產品交易行情同步

這是獨立專案，和原本北農每日查價專案分開。

## 功能
- 每週二、五早上 09:00（台灣時間）執行。
- 資料來源：
  - 蔬菜：`https://amis.afa.gov.tw/veg/VegProdDayTransInfo.aspx`
  - 水果：`https://amis.afa.gov.tw/fruit/FruitProdDayTransInfo.aspx`
- 市場固定抓 `台北一（109）`。
- 每次以「一次下載蔬菜總表 + 一次下載水果總表」再分流到各分頁（最低請求數）。
- 只追加新日期資料，不重複寫入已存在日期。
- 實際抓取品項由 Google Sheet 的 `品項` 分頁控制（可自行增減）。
- 若 `品項` 分頁指定的分頁名稱不存在，會自動建立分頁後再寫入。

## 先決條件
- 目標 Google Sheet 已建立分頁（可由範例檔複製而來）。
- 每個產品分頁 `A1` 應為產品標題（例如 `FI2 茄子 麻荸茄`），用來做分流。

## GitHub Secrets
- `AMIS_WEEKLY_SHEET_ID`: 目標 Google Sheet ID
- `GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT`: Google Service Account JSON 全文

## 品項分頁格式
`品項` 分頁需包含以下欄位（第一列標題）：
- `啟用`：`Y` 才會抓
- `分類`：`veg` 或 `fruit`
- `分頁名稱`：要寫入的分頁名
- `品項代號`：例如 `FI2`
- `品項完整名稱`：例如 `FI2 茄子 麻荸茄`（新建分頁時會寫入 A1）

## 本地 Dry Run
可用範例檔測試分流，不寫入 Google Sheet：

```bash
cd /Users/hushiyu/Documents/北農/amis-weekly-price-sync
DRY_RUN=true TEMPLATE_XLSX='/Users/hushiyu/Downloads/蔬菜產品日交易行情2026.xlsx' python3 sync_amis_weekly.py
```

## 手動觸發
GitHub Actions -> `AMIS Weekly Product Sync` -> `Run workflow`。
