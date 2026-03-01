import os
import json
import sys
import subprocess
from datetime import date, timedelta

START = date(2026, 1, 1)
END = date(2026, 2, 27)
LOG_SUMMARY = '/Users/hushiyu/Documents/北農/backfill_20260101_20260227_summary.json'


def to_roc(d):
    return f"{d.year-1911:03d}/{d.month:02d}/{d.day:02d}"


def run_one(d):
    env = os.environ.copy()
    env['QUERY_DATE_ROC'] = to_roc(d)
    env['MAX_BACKTRACK_DAYS'] = '0'

    p = subprocess.run(
        [sys.executable, 'fetch_tapmc_to_sheet.py'],
        cwd='/Users/hushiyu/Documents/北農',
        env=env,
        capture_output=True,
        text=True,
        timeout=420,
    )
    if p.returncode == 0:
        data = {}
        try:
            data = json.loads((p.stdout or '').strip().splitlines()[-1])
        except Exception:
            data = {}
        return {
            'ok': True,
            'date': str(d),
            'appended': data.get('appended'),
            'updated_worksheets': data.get('updated_worksheets', {}),
        }

    err = (p.stderr or p.stdout or '').strip().splitlines()
    return {
        'ok': False,
        'date': str(d),
        'error': err[-1] if err else 'unknown',
    }


def main():
    ok_days = 0
    fail_days = 0
    details = []
    d = START
    while d <= END:
        try:
            result = run_one(d)
        except subprocess.TimeoutExpired:
            result = {'ok': False, 'date': str(d), 'error': 'timeout>420s'}

        if result['ok']:
            ok_days += 1
            print(f"OK {result['date']} appended={result.get('appended')}", flush=True)
        else:
            fail_days += 1
            print(f"FAIL {result['date']} {result.get('error')}", flush=True)

        details.append(result)
        d += timedelta(days=1)

    summary = {
        'start': str(START),
        'end': str(END),
        'ok_days': ok_days,
        'fail_days': fail_days,
        'details': details,
    }
    with open(LOG_SUMMARY, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print('SUMMARY', json.dumps({'start': str(START), 'end': str(END), 'ok_days': ok_days, 'fail_days': fail_days}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
