import os
import json
import sys
import subprocess
from datetime import date, timedelta

START = date(2026, 1, 1)
END = date(2026, 2, 27)
SUMMARY_PATH = '/Users/hushiyu/Documents/北農/backfill_20260101_20260227_summary.json'
LOG_PATH = '/Users/hushiyu/Documents/北農/backfill_20260101_20260227.log'


def to_roc(d):
    return f"{d.year-1911:03d}/{d.month:02d}/{d.day:02d}"


def main():
    ok_days = 0
    fail_days = 0
    details = []

    cur = START
    while cur <= END:
        env = os.environ.copy()
        env['QUERY_DATE_ROC'] = to_roc(cur)
        env['MAX_BACKTRACK_DAYS'] = '0'

        item = {'date': str(cur), 'ok': False}
        try:
            p = subprocess.run(
                [sys.executable, 'fetch_tapmc_to_sheet.py'],
                cwd='/Users/hushiyu/Documents/北農',
                env=env,
                capture_output=True,
                text=True,
                timeout=900,
            )
            if p.returncode == 0:
                ok_days += 1
                item['ok'] = True
                try:
                    payload = json.loads((p.stdout or '').strip().splitlines()[-1])
                except Exception:
                    payload = {}
                item['appended'] = payload.get('appended')
                print(f"OK {cur} appended={item.get('appended')}", flush=True)
            else:
                fail_days += 1
                lines = (p.stderr or p.stdout or '').strip().splitlines()
                item['error'] = lines[-1] if lines else 'unknown-error'
                print(f"FAIL {cur} {item['error']}", flush=True)
        except subprocess.TimeoutExpired:
            fail_days += 1
            item['error'] = 'timeout>900s'
            print(f"FAIL {cur} timeout>900s", flush=True)

        details.append(item)
        cur += timedelta(days=1)

    summary = {
        'start': str(START),
        'end': str(END),
        'ok_days': ok_days,
        'fail_days': fail_days,
        'details': details,
    }
    with open(SUMMARY_PATH, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print('SUMMARY ' + json.dumps({'start': str(START), 'end': str(END), 'ok_days': ok_days, 'fail_days': fail_days}, ensure_ascii=False), flush=True)
    print('SUMMARY_FILE ' + SUMMARY_PATH, flush=True)


if __name__ == '__main__':
    main()
