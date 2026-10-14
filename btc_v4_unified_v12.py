#!/usr/bin/env python3
"""BTC V12: read-only orchestrator for the existing V8 -> V10 -> V11 audits.

This intentionally reuses the original feature/risk implementations rather than
reimplementing their rules. Source scripts and DB remain unchanged.
"""
import argparse
import csv
import datetime as dt
import pathlib
import re
import subprocess
import sys
import tempfile

BASE = pathlib.Path('/tmp')
DEFAULT_DB = '/opt/tmco_webhook_v4/data/v4_paper_live.db'
V8_SOURCE = BASE / 'btc_v4_structure_context_v8_200_244.py'
V10_SOURCE = BASE / 'btc_v4_structure_context_v10_200_244.py'
V11_SOURCE = BASE / 'btc_v4_risk_audit_v11_200_244.py'


def change_once(text, old, new, name):
    n = text.count(old)
    if n != 1:
        raise RuntimeError(f'{name}: expected 1 occurrence of {old!r}, found {n}. No audit started.')
    return text.replace(old, new)


def prepare_v8(source, dest, start, end, count, db):
    s = source.read_text()
    s = change_once(s, 'WHERE signal_id BETWEEN 200 AND 244 ORDER BY signal_id',
                    f'WHERE signal_id BETWEEN {start} AND {end} ORDER BY signal_id', 'V8 range')
    s = change_once(s, 'if len(trades)!=45:', f'if len(trades)!={count}:', 'V8 count')
    s = change_once(s, 'expected 45 records', f'expected {count} records', 'V8 warning')
    # This is the read-only outcomes lookup injected in the verified V8_200_244 source.
    s = change_once(s,
        'WHERE signal_id BETWEEN 200 AND 244\'\n',
        f'WHERE signal_id BETWEEN {start} AND {end}\'\n', 'V8 outcome range')
    s = change_once(s,
        "'file:/opt/tmco_webhook_v4/data/v4_paper_live.db?mode=ro'",
        repr(pathlib.Path(db).resolve().as_uri() + '?mode=ro'), 'V8 outcome DB')
    dest.write_text(s)
    compile(s, str(dest), 'exec')


def prepare_v10(source, dest, start, end, v8_script, v8_csv):
    s = source.read_text()
    s = change_once(s, 'WHERE signal_id BETWEEN 200 AND 244 ORDER BY signal_id',
                    f'WHERE signal_id BETWEEN {start} AND {end} ORDER BY signal_id', 'V10 range')
    s = change_once(s, '/tmp/btc_v4_structure_context_v8_200_244.py', str(v8_script), 'V10 V8 script')
    s = change_once(s,
        '/tmp/btc_v4_structure_context_v8_200_244/structure_context_report.csv',
        str(v8_csv), 'V10 V8 report')
    dest.write_text(s)
    compile(s, str(dest), 'exec')


def run(cmd, log):
    print('\nRUN:', ' '.join(map(str, cmd)), flush=True)
    with log.open('w') as f:
        proc = subprocess.Popen(list(map(str, cmd)), stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, bufsize=1)
        for line in proc.stdout:
            print(line, end='', flush=True)
            f.write(line)
        code = proc.wait()
    if code:
        raise RuntimeError(f'Command exited with status {code}; see {log}')


def load(path):
    if not path.is_file():
        raise RuntimeError(f'Missing output: {path}')
    with path.open(newline='') as f:
        return list(csv.DictReader(f))


def main():
    ap = argparse.ArgumentParser(description='Read-only V8 + V10 + V11 unified BTC audit')
    ap.add_argument('--start', type=int, default=200)
    ap.add_argument('--end', type=int, default=244)
    ap.add_argument('--db', default=DEFAULT_DB)
    ap.add_argument('--out', default=None)
    ap.add_argument('--v8-source', type=pathlib.Path, default=V8_SOURCE)
    ap.add_argument('--v10-source', type=pathlib.Path, default=V10_SOURCE)
    ap.add_argument('--v11-source', type=pathlib.Path, default=V11_SOURCE)
    args = ap.parse_args()
    if args.start < 1 or args.end < args.start or args.end - args.start > 1000:
        ap.error('Invalid ID range (max 1001 IDs per run)')
    for p in (args.v8_source, args.v10_source, args.v11_source, pathlib.Path(args.db)):
        if not p.is_file():
            ap.error(f'Missing required file: {p}')
    out = pathlib.Path(args.out or f'/tmp/btc_v4_unified_v12_{args.start}_{args.end}').resolve()
    out.mkdir(parents=True, exist_ok=True)
    v8out, v10out, v11out = (out / n for n in ('v8', 'v10', 'v11'))
    for p in (v8out, v10out, v11out):
        p.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='v12_readonly_') as tmp:
        v8script = pathlib.Path(tmp) / 'v8.py'
        v10script = pathlib.Path(tmp) / 'v10.py'
        prepare_v8(args.v8_source, v8script, args.start, args.end,
                   args.end - args.start + 1, args.db)
        prepare_v10(args.v10_source, v10script, args.start, args.end,
                    v8script, v8out / 'structure_context_report.csv')
        print('Prepared isolated V8/V10 scripts. Original scripts unchanged.', flush=True)
        run([sys.executable, v8script, '--db', args.db, '--out', v8out], out / 'v8.log')
        run([sys.executable, v10script, '--db', args.db, '--v8', v8script,
             '--v8-csv', v8out / 'structure_context_report.csv', '--out', v10out], out / 'v10.log')
        run([sys.executable, args.v11_source, '--input', v10out / 'all_signals.csv',
             '--out', v11out], out / 'v11.log')
    a, b, c = [load(p) for p in (v8out / 'structure_context_report.csv',
                                v10out / 'all_signals.csv', v11out / 'all_signals.csv')]
    def index(rows):
        if not rows or 'ID' not in rows[0]:
            raise RuntimeError('Report has no rows or ID column')
        ids = [int(r['ID']) for r in rows]
        if len(ids) != len(set(ids)):
            raise RuntimeError('Duplicate IDs in report')
        return {int(r['ID']): r for r in rows}
    ia, ib, ic = index(a), index(b), index(c)
    expected = set(range(args.start, args.end + 1))
    if set(ia) != expected or set(ib) != expected or set(ic) != expected:
        raise RuntimeError('Incomplete or inconsistent IDs in V8/V10/V11 reports')
    result = []
    for sid in sorted(expected):
        row = {'ID': sid}
        for prefix, data in [('V8', ia[sid]), ('V10', ib[sid]), ('V11', ic[sid])]:
            row.update({f'{prefix}_{k}': v for k, v in data.items() if k != 'ID'})
        result.append(row)
    output = out / 'unified_all_signals.csv'
    with output.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(result[0]), extrasaction='ignore')
        writer.writeheader()
        writer.writerows(result)
    flagged = [r for r in c if r.get('RISK_STATUS', '').upper() == 'REVIEW']
    passed = [r for r in c if r.get('RISK_STATUS', '').upper() == 'NO_RULE_TRIGGER']
    insufficient = [r for r in c if r.get('RISK_STATUS', '').upper() == 'INSUFFICIENT_DATA']
    summary = out / 'summary.txt'
    lines = [f'V12 unified audit: IDs {args.start}-{args.end}',
             f'Processed: {len(c)}', f'V11 flagged: {len(flagged)}',
             f'V11 passed: {len(passed)}', f'Insufficient data: {len(insufficient)}',
             'Passed IDs: ' + ', '.join(r['ID'] for r in passed),
             'Flagged IDs: ' + ', '.join(r['ID'] for r in flagged),
             'Note: all signals are provisional PARENT, not overlap-deduplicated.',
             'Note: outcome snapshot can change for ACTIVE trades.',
             'Note: this is descriptive; no trading decisions are made.']
    summary.write_text('\n'.join(lines) + '\n')
    print('\n' + '\n'.join(lines))
    print('UNIFIED CSV:', output)
    print('SUMMARY:', summary)
    print('FINISHED | no orders, no DB writes, no service changes')


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, OSError, ValueError) as e:
        print('V12 ERROR:', e, file=sys.stderr)
        sys.exit(1)
