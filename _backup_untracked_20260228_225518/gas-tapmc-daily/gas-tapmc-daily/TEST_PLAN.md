# GAS 測試計畫（先 5 分鐘，後 9 點）

## 目標
- 驗證 `runTapmcDailySync` 在高頻（每 5 分鐘）下可穩定執行。
- 驗證不會因重跑產生同日同代號重複資料。
- 驗證自動建立「一品名一分頁」寫入邏輯。
- 通過後切換為每日 09:00（台灣時間）。

## 前置設定
- 已在 Apps Script 專案貼上 `Code.gs`。
- Script Properties 至少有：
  - `GOOGLE_SHEET_ID`
- 建議加：
  - `ITEM_WORKSHEET_NAME=item`
  - `ITEM_COLUMN=1`
  - `TIMEZONE=Asia/Taipei`
  - `MAX_BACKTRACK_DAYS=10`

## Phase A：單次煙霧測試
1. 手動執行 `runTapmcDailySync`。
2. 確認 Logs 內回傳：
   - `ok=true`
   - `updated_worksheets` 有內容
3. 確認試算表是否新增分頁（命名格式：`品名代號 品名`）。

## Phase B：5 分鐘 burn-in（重點）
1. 執行 `setupFiveMinuteTrigger()` 建立 5 分鐘觸發器（會先清掉舊觸發器）。
2. 執行 `getTapmcTriggerStatus()`，確認只有 `runTapmcDailySync` 一條 trigger。
3. 連續觀察至少 30 分鐘（>= 6 次執行）。
4. 每次檢查：
   - 執行記錄無 exception
   - `appended` 與 `skipped_existing` 合理
5. 執行 `auditTapmcDuplicates()`，結果應為 `{}`（無重複）。

## Phase C：修復標準
- 若出現 HTTP 429/5xx：
  - 調高 `FETCH_RETRY_COUNT`（例如 4）
  - 調高 `FETCH_RETRY_SLEEP_MS`（例如 2000）
- 若發生併發重入：
  - 確認 `Another run is in progress` 僅偶發，且資料無重複
- 若分頁名失敗：
  - 檢查來源品名是否含特殊字元（程式已做 sanitize）

## Phase D：切換正式排程（每日 09:00）
1. burn-in 通過後執行 `setupDailyNineTrigger()`。
2. 再執行 `getTapmcTriggerStatus()` 確認已切為每日 trigger。
3. 隔天 09:00 驗收一次。

## 通過條件
- Burn-in 期間成功率 >= 95%。
- 無重複資料（`auditTapmcDuplicates()` 回 `{}`）。
- 每次執行完成時間小於 5 分鐘間隔。
