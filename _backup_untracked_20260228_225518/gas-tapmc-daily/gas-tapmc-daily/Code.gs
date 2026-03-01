/**
 * TAPMC sync for Google Apps Script.
 * - Reads target item codes from ITEM_WORKSHEET_NAME
 * - Creates one worksheet per item ("<code> <name>")
 * - Appends [date, code, name, variety, high, mid, low]
 * - Supports 5-min burn-in trigger and daily 9AM trigger
 */

function runTapmcDailySync() {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(30 * 1000)) {
    throw new Error("Another run is in progress; skip this execution.");
  }

  try {
    var cfg = loadConfig_();
    var ss = SpreadsheetApp.openById(cfg.sheetId);
    var itemSheet = ss.getSheetByName(cfg.itemWorksheetName);
    if (!itemSheet) {
      throw new Error("Worksheet not found: " + cfg.itemWorksheetName);
    }

    var targetCodes = loadTargetCodes_(itemSheet, cfg.itemColumn);
    if (targetCodes.length === 0) {
      throw new Error("No item codes found in worksheet " + cfg.itemWorksheetName);
    }

    var now = new Date();
    var targetDate = formatDateYmd_(now, cfg.timezone);
    var startDate = cfg.queryDateRoc ? parseRocDateToDate_(cfg.queryDateRoc) : dateOnlyInTimezone_(now, cfg.timezone);

    var usedQueryDateRoc = null;
    var backtrackedDays = 0;
    var rowsByWorksheet = {};
    var missingCodes = [];

    for (var daysBack = 0; daysBack <= cfg.maxBacktrackDays; daysBack++) {
      var queryDate = addDays_(startDate, -daysBack);
      var queryDateRoc = toRocDate_(queryDate);
      var allRecords = {};

      for (var i = 0; i < cfg.queryCombos.length; i++) {
        var combo = cfg.queryCombos[i];
        var html = fetchQueryHtml_(cfg.sourceUrl, queryDateRoc, combo.category, combo.fvCode, combo.market);
        var records = extractRecordsFromHtml_(html);
        mergeRecords_(allRecords, records);
      }

      if (Object.keys(allRecords).length === 0) {
        continue;
      }

      var candidateRowsByWs = {};
      var candidateMissing = [];
      for (var j = 0; j < targetCodes.length; j++) {
        var code = targetCodes[j];
        var rec = allRecords[code];
        if (!rec) {
          candidateMissing.push(code);
          continue;
        }

        var wsTitle = worksheetTitleForRecord_(rec);
        if (!candidateRowsByWs[wsTitle]) {
          candidateRowsByWs[wsTitle] = [];
        }
        candidateRowsByWs[wsTitle].push([
          targetDate,
          rec.code,
          rec.name,
          rec.variety,
          rec.high,
          rec.mid,
          rec.low
        ]);
      }

      if (Object.keys(candidateRowsByWs).length > 0) {
        rowsByWorksheet = candidateRowsByWs;
        missingCodes = candidateMissing;
        usedQueryDateRoc = queryDateRoc;
        backtrackedDays = daysBack;
        break;
      }
    }

    if (Object.keys(rowsByWorksheet).length === 0) {
      throw new Error("No usable data found within MAX_BACKTRACK_DAYS.");
    }

    var updateStats = appendRowsByWorksheet_(ss, rowsByWorksheet, targetDate);
    var payload = {
      ok: true,
      date: targetDate,
      used_query_date_roc: usedQueryDateRoc,
      backtracked_days: backtrackedDays,
      item_codes: targetCodes.length,
      appended: updateStats.appended,
      skipped_existing: updateStats.skipped,
      updated_worksheets: updateStats.updatedWorksheets,
      missing_codes: missingCodes
    };

    Logger.log(JSON.stringify(payload, null, 2));
    return payload;
  } finally {
    lock.releaseLock();
  }
}

function loadConfig_() {
  var p = PropertiesService.getScriptProperties();
  var cfg = {
    sourceUrl: p.getProperty("SOURCE_URL") || "https://www.tapmc.com.tw/Pages/Trans/Price1",
    sheetId: must_(p.getProperty("GOOGLE_SHEET_ID"), "GOOGLE_SHEET_ID"),
    timezone: p.getProperty("TIMEZONE") || "Asia/Taipei",
    itemWorksheetName: p.getProperty("ITEM_WORKSHEET_NAME") || "item",
    itemColumn: parseInt(p.getProperty("ITEM_COLUMN") || "1", 10),
    queryDateRoc: (p.getProperty("QUERY_DATE_ROC") || "").trim(),
    maxBacktrackDays: parseInt(p.getProperty("MAX_BACKTRACK_DAYS") || "10", 10),
    queryCombos: parseQueryCombos_(p.getProperty("QUERY_COMBOS") || "1:V:1,1:F:1,2:V:1,2:V:2,2:F:1,2:F:2"),
    fetchRetryCount: parseInt(p.getProperty("FETCH_RETRY_COUNT") || "3", 10),
    fetchRetrySleepMs: parseInt(p.getProperty("FETCH_RETRY_SLEEP_MS") || "1200", 10)
  };

  if (!cfg.itemColumn || cfg.itemColumn < 1) {
    throw new Error("ITEM_COLUMN must be >= 1");
  }
  return cfg;
}

function must_(value, name) {
  if (!value) throw new Error("Missing required property: " + name);
  return value;
}

function parseQueryCombos_(raw) {
  var parts = raw.split(",");
  var out = [];
  for (var i = 0; i < parts.length; i++) {
    var seg = parts[i].trim();
    if (!seg) continue;
    var x = seg.split(":");
    if (x.length !== 3) throw new Error("Bad QUERY_COMBOS segment: " + seg);
    out.push({ category: x[0].trim(), fvCode: x[1].trim(), market: x[2].trim() });
  }
  if (out.length === 0) throw new Error("QUERY_COMBOS cannot be empty");
  return out;
}

function loadTargetCodes_(sheet, itemColumn) {
  var values = sheet.getRange(1, itemColumn, sheet.getLastRow(), 1).getValues();
  var codes = [];
  for (var i = 0; i < values.length; i++) {
    var code = normalizeText_(values[i][0]).toUpperCase();
    if (!code) continue;
    if (i === 0 && ["品名代號", "ITEM", "CODE", "品項", "ITEMCODE"].indexOf(code) >= 0) continue;
    codes.push(code);
  }
  return codes;
}

function fetchQueryHtml_(url, dateRoc, category, fvCode, market) {
  var cfg = loadConfig_();
  var getResp = fetchWithRetry_(url, {
    method: "get",
    muteHttpExceptions: true,
    headers: { "User-Agent": "Mozilla/5.0" }
  }, cfg.fetchRetryCount, cfg.fetchRetrySleepMs);

  if (getResp.getResponseCode() >= 400) {
    throw new Error("GET failed: " + getResp.getResponseCode());
  }

  var html = getResp.getContentText("UTF-8");
  var payload = extractFormPayload_(html);
  payload.__EVENTTARGET = "ctl00$ContentPlaceHolder1$btnQuery";
  payload.__EVENTARGUMENT = "";
  payload["ctl00$ContentPlaceHolder1$txtDate"] = dateRoc;
  payload["ctl00$ContentPlaceHolder1$DDL_Category"] = category;
  payload["ctl00$ContentPlaceHolder1$DDL_FV_Code"] = fvCode;
  payload["ctl00$ContentPlaceHolder1$DDL_Market"] = market;

  var postResp = fetchWithRetry_(url, {
    method: "post",
    payload: payload,
    muteHttpExceptions: true,
    headers: { "User-Agent": "Mozilla/5.0" }
  }, cfg.fetchRetryCount, cfg.fetchRetrySleepMs);

  if (postResp.getResponseCode() >= 400) {
    throw new Error("POST failed: " + postResp.getResponseCode());
  }
  return postResp.getContentText("UTF-8");
}

function fetchWithRetry_(url, options, retryCount, baseSleepMs) {
  var lastErr = null;
  for (var i = 0; i <= retryCount; i++) {
    try {
      var resp = UrlFetchApp.fetch(url, options);
      var code = resp.getResponseCode();
      if (code === 429 || code >= 500) {
        if (i < retryCount) {
          Utilities.sleep(baseSleepMs * Math.pow(2, i));
          continue;
        }
      }
      return resp;
    } catch (e) {
      lastErr = e;
      if (i >= retryCount) throw e;
      Utilities.sleep(baseSleepMs * Math.pow(2, i));
    }
  }
  if (lastErr) throw lastErr;
  throw new Error("fetchWithRetry_ failed unexpectedly");
}

function extractFormPayload_(html) {
  var form = html.match(/<form[\s\S]*?<\/form>/i);
  if (!form) throw new Error("Form not found in source page.");
  var payload = {};
  var inputRe = /<input\b[^>]*>/gi;
  var m;
  while ((m = inputRe.exec(form[0])) !== null) {
    var tag = m[0];
    var name = attr_(tag, "name");
    if (!name) continue;
    payload[name] = attr_(tag, "value") || "";
  }
  return payload;
}

function attr_(tag, name) {
  var re = new RegExp(name + '\\s*=\\s*["\\\']([^"\\\']*)["\\\']', "i");
  var m = tag.match(re);
  return m ? m[1] : "";
}

function extractRecordsFromHtml_(html) {
  var records = {};
  var tables = html.match(/<table[\s\S]*?<\/table>/gi) || [];
  for (var i = 0; i < tables.length; i++) {
    var rows = parseTableRows_(tables[i]);
    if (rows.length < 2) continue;

    var fieldMap = detectFieldMap_(rows[0]);
    if (!fieldMap) continue;

    for (var r = 1; r < rows.length; r++) {
      var row = rows[r];
      var code = normalizeText_(row[fieldMap.code]).toUpperCase();
      if (!code) continue;
      if (!records[code]) {
        records[code] = {
          code: code,
          name: cleanCell_(row[fieldMap.name]),
          variety: cleanCell_(row[fieldMap.variety]),
          high: parseNumber_(row[fieldMap.high]),
          mid: parseNumber_(row[fieldMap.mid]),
          low: parseNumber_(row[fieldMap.low])
        };
      }
    }
  }
  return records;
}

function parseTableRows_(tableHtml) {
  var out = [];
  var rowRe = /<tr[\s\S]*?<\/tr>/gi;
  var m;
  while ((m = rowRe.exec(tableHtml)) !== null) {
    var tr = m[0];
    var cells = [];
    var cellRe = /<t[hd]\b[\s\S]*?<\/t[hd]>/gi;
    var c;
    while ((c = cellRe.exec(tr)) !== null) {
      var txt = stripTags_(c[0]);
      cells.push(cleanCell_(txt));
    }
    if (cells.length > 0) out.push(cells);
  }
  return out;
}

function detectFieldMap_(headerCells) {
  var required = {
    code: ["品名代號", "代號"],
    name: ["品名"],
    variety: ["品種"],
    high: ["上價"],
    mid: ["中價"],
    low: ["下價"]
  };

  var norm = [];
  for (var i = 0; i < headerCells.length; i++) norm.push(normalizeText_(headerCells[i]));

  var used = {};
  var map = {};
  var keys = Object.keys(required);

  for (var k = 0; k < keys.length; k++) {
    var field = keys[k];
    var cands = required[field];
    var idx = -1;

    for (var i1 = 0; i1 < norm.length; i1++) {
      if (used[i1]) continue;
      for (var c1 = 0; c1 < cands.length; c1++) {
        if (norm[i1] === normalizeText_(cands[c1])) {
          idx = i1;
          break;
        }
      }
      if (idx >= 0) break;
    }

    if (idx < 0) {
      for (var i2 = 0; i2 < norm.length; i2++) {
        if (used[i2]) continue;
        var ok = false;
        for (var c2 = 0; c2 < cands.length; c2++) {
          if (norm[i2].indexOf(normalizeText_(cands[c2])) >= 0) {
            ok = true;
            break;
          }
        }
        if (!ok) continue;
        if (field === "name" && norm[i2].indexOf("代號") >= 0) continue;
        idx = i2;
        break;
      }
    }

    if (idx < 0) return null;
    map[field] = idx;
    used[idx] = true;
  }

  return map;
}

function mergeRecords_(target, src) {
  var keys = Object.keys(src);
  for (var i = 0; i < keys.length; i++) {
    if (!target[keys[i]]) target[keys[i]] = src[keys[i]];
  }
}

function sanitizeWorksheetTitle_(text) {
  var cleaned = String(text || "")
    .replace(/[\[\]:*?/\\]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!cleaned) cleaned = "未命名";
  return cleaned.slice(0, 100);
}

function worksheetTitleForRecord_(record) {
  return sanitizeWorksheetTitle_(record.code + " " + record.name);
}

function ensureItemWorksheet_(ss, title) {
  var ws = ss.getSheetByName(title);
  if (!ws) {
    ws = ss.insertSheet(title);
  }

  if (ws.getLastRow() === 0) {
    ws.getRange(1, 1, 1, 7).setValues([["日期", "品名代號", "品名", "品種", "上價", "中價", "下價"]]);
  }
  return ws;
}

function appendRowsByWorksheet_(ss, rowsByWorksheet, targetDate) {
  var updated = {};
  var appendedTotal = 0;
  var skippedTotal = 0;
  var wsTitles = Object.keys(rowsByWorksheet);

  for (var i = 0; i < wsTitles.length; i++) {
    var title = wsTitles[i];
    var rows = rowsByWorksheet[title] || [];
    if (rows.length === 0) continue;

    var ws = ensureItemWorksheet_(ss, title);
    var deduped = filterAlreadyAppendedForSheet_(ws, rows, targetDate);
    skippedTotal += rows.length - deduped.length;

    if (deduped.length > 0) {
      ws.getRange(ws.getLastRow() + 1, 1, deduped.length, 7).setValues(deduped);
      updated[title] = deduped.length;
      appendedTotal += deduped.length;
    }
  }

  return {
    appended: appendedTotal,
    skipped: skippedTotal,
    updatedWorksheets: updated
  };
}

function filterAlreadyAppendedForSheet_(sheet, rows, targetDate) {
  var lastRow = sheet.getLastRow();
  if (lastRow < 2) return rows;

  var dateCodeSet = {};
  var values = sheet.getRange(2, 1, lastRow - 1, 2).getValues();
  for (var i = 0; i < values.length; i++) {
    var d = normalizeText_(values[i][0]);
    var c = normalizeText_(values[i][1]).toUpperCase();
    if (!d || !c) continue;
    if (d === targetDate) dateCodeSet[d + "|" + c] = true;
  }

  var out = [];
  for (var j = 0; j < rows.length; j++) {
    var key = rows[j][0] + "|" + normalizeText_(rows[j][1]).toUpperCase();
    if (!dateCodeSet[key]) out.push(rows[j]);
  }
  return out;
}

function normalizeText_(value) {
  if (value === null || value === undefined) return "";
  return String(value).replace(/\u3000/g, " ").replace(/\s+/g, "");
}

function cleanCell_(value) {
  if (value === null || value === undefined) return "";
  return String(value).replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();
}

function stripTags_(html) {
  return String(html)
    .replace(/<br\s*\/?>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/\s+/g, " ")
    .trim();
}

function parseNumber_(value) {
  if (value === null || value === undefined || value === "") return "";
  var m = String(value).match(/-?\d+(?:,\d{3})*(?:\.\d+)?/);
  if (!m) return "";
  var n = Number(m[0].replace(/,/g, ""));
  return isNaN(n) ? "" : n;
}

function formatDateYmd_(dateObj, timezone) {
  return Utilities.formatDate(dateObj, timezone, "yyyy-MM-dd");
}

function dateOnlyInTimezone_(dateObj, timezone) {
  var ymd = formatDateYmd_(dateObj, timezone);
  return new Date(ymd + "T00:00:00");
}

function addDays_(dateObj, days) {
  var d = new Date(dateObj.getTime());
  d.setDate(d.getDate() + days);
  return d;
}

function toRocDate_(dateObj) {
  var y = dateObj.getFullYear() - 1911;
  var m = dateObj.getMonth() + 1;
  var d = dateObj.getDate();
  return pad_(y, 3) + "/" + pad_(m, 2) + "/" + pad_(d, 2);
}

function parseRocDateToDate_(text) {
  var m = String(text).trim().match(/^(\d{2,3})\/(\d{1,2})\/(\d{1,2})$/);
  if (!m) throw new Error("QUERY_DATE_ROC format should be YYY/MM/DD, e.g. 115/02/24");
  return new Date(Number(m[1]) + 1911, Number(m[2]) - 1, Number(m[3]));
}

function pad_(n, width) {
  var s = String(n);
  while (s.length < width) s = "0" + s;
  return s;
}

/** Trigger helpers for burn-in and production */

function clearTapmcTriggers() {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === "runTapmcDailySync") {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }
}

function setupFiveMinuteTrigger() {
  clearTapmcTriggers();
  ScriptApp.newTrigger("runTapmcDailySync").timeBased().everyMinutes(5).create();
  return getTapmcTriggerStatus();
}

function setupDailyNineTrigger() {
  clearTapmcTriggers();
  ScriptApp.newTrigger("runTapmcDailySync").timeBased().atHour(9).everyDays(1).create();
  return getTapmcTriggerStatus();
}

function getTapmcTriggerStatus() {
  var out = [];
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    var t = triggers[i];
    if (t.getHandlerFunction() !== "runTapmcDailySync") continue;
    out.push({
      handler: t.getHandlerFunction(),
      eventType: String(t.getEventType()),
      triggerSource: String(t.getTriggerSource()),
      uniqueId: t.getUniqueId()
    });
  }
  Logger.log(JSON.stringify(out, null, 2));
  return out;
}

function auditTapmcDuplicates() {
  var cfg = loadConfig_();
  var ss = SpreadsheetApp.openById(cfg.sheetId);
  var sheets = ss.getSheets();
  var dupSummary = {};

  for (var i = 0; i < sheets.length; i++) {
    var ws = sheets[i];
    if (ws.getName() === cfg.itemWorksheetName) continue;
    if (ws.getLastRow() < 2) continue;

    var values = ws.getRange(2, 1, ws.getLastRow() - 1, 2).getValues();
    var seen = {};
    var dup = 0;

    for (var r = 0; r < values.length; r++) {
      var d = normalizeText_(values[r][0]);
      var c = normalizeText_(values[r][1]).toUpperCase();
      if (!d || !c) continue;
      var k = d + "|" + c;
      if (seen[k]) dup++;
      seen[k] = true;
    }

    if (dup > 0) {
      dupSummary[ws.getName()] = dup;
    }
  }

  Logger.log(JSON.stringify(dupSummary, null, 2));
  return dupSummary;
}
