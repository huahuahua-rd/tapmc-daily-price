# TAPMC Daily on GAS

## 1) 建立 Apps Script
- 開啟目標 Google Sheet -> `擴充功能` -> `Apps Script`
- 建立專案後，把 [Code.gs](/Users/hushiyu/Documents/北農/gas-tapmc-daily/Code.gs) 內容貼上

## 2) 設定 Script Properties
必要：
- `GOOGLE_SHEET_ID`

建議：
- `ITEM_WORKSHEET_NAME=item`
- `ITEM_COLUMN=1`
- `TIMEZONE=Asia/Taipei`
- `QUERY_COMBOS=1:V:1,1:F:1,2:V:1,2:V:2,2:F:1,2:F:2`
- `MAX_BACKTRACK_DAYS=10`
- `FETCH_RETRY_COUNT=3`
- `FETCH_RETRY_SLEEP_MS=1200`

## 3) 執行順序（先 5 分鐘測，再 9 點正式）
1. 手動執行 `runTapmcDailySync`（首次授權）
2. 執行 `setupFiveMinuteTrigger`
3. 觀察 30 分鐘以上
4. 執行 `auditTapmcDuplicates`（確認無重複）
5. 通過後執行 `setupDailyNineTrigger`

完整流程見 [TEST_PLAN.md](/Users/hushiyu/Documents/北農/gas-tapmc-daily/TEST_PLAN.md)

## 資料寫入規則
- 來源：TAPMC 單日交易頁
- 目標：依 `品名代號 + 品名` 自動建立分頁
- 欄位：`日期 / 品名代號 / 品名 / 品種 / 上價 / 中價 / 下價`
- 內建去重：同分頁內同一天同代號不重複 append
