#!/usr/bin/env python3
"""Read-only BTC V4 24h market-structure audit. No trading, DB writes or service changes.

Uses only completed Bybit 5m candles known by each paper trade's decision time.
Swing pivots are confirmed causally after price reverses by k * ATR(14).
Produces CSV and optional annotated PNGs for selected signals.
"""
import argparse
import csv
import json
import math
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE = Path('/opt/tmco_webhook_v4')
DB = BASE / 'data/v4_paper_live.db'
FIVE = 300
DAY = 24 * 3600


def get_json(url, attempts=3):
    for attempt in range(attempts):
        try:
            with urlopen(Request(url, headers={'User-Agent': 'BTC-V4-Structure-Audit/1.0'}), timeout=20) as resp:
                return json.load(resp)
        except Exception:
            if attempt == attempts - 1:
                raise
            time.sleep(1 + attempt)


def fetch_bybit(start, end):
    """Get completed historical candles by explicit bounded UTC interval."""
    out = {}
    cursor = start - start % FIVE
    while cursor < end:
        batch_end = min(end, cursor + 900 * FIVE)
        params = urlencode({'category': 'linear', 'symbol': 'BTCUSDT', 'interval': '5',
                            'start': cursor * 1000, 'end': (batch_end - 1) * 1000, 'limit': 1000})
        payload = get_json('https://api.bybit.com/v5/market/kline?' + params)
        if payload.get('retCode') != 0:
            raise RuntimeError('Bybit error: ' + str(payload.get('retMsg')))
        for row in payload.get('result', {}).get('list', []):
            t = int(row[0]) // 1000
            if start <= t < end:
                out[t] = {'timestamp': t, 'open': float(row[1]), 'high': float(row[2]),
                          'low': float(row[3]), 'close': float(row[4]), 'volume': float(row[5])}
        cursor = batch_end
    return [out[t] for t in sorted(out)]


def atr_series(bars, period=14):
    """Causal ATR using trailing period true ranges (not future candles)."""
    tr = []
    values = []
    for i, b in enumerate(bars):
        prev = bars[i-1]['close'] if i else b['close']
        tr.append(max(b['high']-b['low'], abs(b['high']-prev), abs(b['low']-prev)))
        values.append(sum(tr[-period:]) / min(period, len(tr)))
    return values


def swings(bars, atrs, multiplier):
    """Online directional-change swing detector. Confirmation occurs on current bar.

    A pivot's price/time is the preceding extreme, but it is never usable before
    confirm_index. This is causal and avoids ZigZag repainting in backtests.
    """
    if not bars:
        return []
    mode = None
    high = (bars[0]['high'], 0)
    low = (bars[0]['low'], 0)
    found = []
    for i, b in enumerate(bars):
        threshold = max(1e-8, multiplier * atrs[i])
        if mode is None:
            if b['high'] > high[0]: high = (b['high'], i)
            if b['low'] < low[0]: low = (b['low'], i)
            if b['close'] >= low[0] + threshold and low[1] < i:
                found.append({'type': 'L', 'price': low[0], 'pivot_i': low[1], 'confirm_i': i})
                mode = 'UP'; high = (b['high'], i)
            elif b['close'] <= high[0] - threshold and high[1] < i:
                found.append({'type': 'H', 'price': high[0], 'pivot_i': high[1], 'confirm_i': i})
                mode = 'DOWN'; low = (b['low'], i)
        elif mode == 'UP':
            if b['high'] > high[0]: high = (b['high'], i)
            if b['close'] <= high[0] - threshold and high[1] < i:
                found.append({'type': 'H', 'price': high[0], 'pivot_i': high[1], 'confirm_i': i})
                mode = 'DOWN'; low = (b['low'], i)
        else:
            if b['low'] < low[0]: low = (b['low'], i)
            if b['close'] >= low[0] + threshold and low[1] < i:
                found.append({'type': 'L', 'price': low[0], 'pivot_i': low[1], 'confirm_i': i})
                mode = 'UP'; high = (b['high'], i)
    return found


def classify(pivots, atr_now, tolerance=0.35):
    hs = [p for p in pivots if p['type'] == 'H']
    ls = [p for p in pivots if p['type'] == 'L']
    if len(hs) < 2 or len(ls) < 2:
        return 'INSUFFICIENT', hs[-2:], ls[-2:]
    h1, h2 = hs[-2:]
    l1, l2 = ls[-2:]
    dh = (h2['price'] - h1['price']) / atr_now
    dl = (l2['price'] - l1['price']) / atr_now
    if dh > tolerance and dl > tolerance:
        trend = 'UP'
    elif dh < -tolerance and dl < -tolerance:
        trend = 'DOWN'
    elif abs(dh) <= tolerance and abs(dl) <= tolerance:
        trend = 'RANGE'
    else:
        trend = 'TRANSITION'
    return trend, hs[-2:], ls[-2:]


def bos(bars, pivots):
    if len(bars) < 2:
        return '-'
    # Only pivots confirmed BEFORE the most recent closed bar can be used.
    prior = [p for p in pivots if p['confirm_i'] < len(bars) - 1]
    highs = [p['price'] for p in prior if p['type'] == 'H']
    lows = [p['price'] for p in prior if p['type'] == 'L']
    prev, curr = bars[-2]['close'], bars[-1]['close']
    if highs and prev <= highs[-1] < curr:
        return 'UP'
    if lows and prev >= lows[-1] > curr:
        return 'DOWN'
    return '-'


def pivot_text(items, bars):
    return ' | '.join(f"{p['price']:.2f}@{datetime.fromtimestamp(bars[p['pivot_i']]['timestamp'], timezone.utc).strftime('%m-%d %H:%M')} (known {datetime.fromtimestamp(bars[p['confirm_i']]['timestamp'], timezone.utc).strftime('%H:%M')})" for p in items) or '-'


def plot_chart(bars, major, minor, row, result, dest):
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        from matplotlib.dates import DateFormatter
    except ImportError:
        return 'matplotlib unavailable: chart skipped'
    dates = [datetime.fromtimestamp(b['timestamp'], timezone.utc) for b in bars]
    fig, ax = plt.subplots(figsize=(15, 6))
    ax.plot(dates, [b['close'] for b in bars], linewidth=1.1, label='5m close')
    for kind, pts, color, size in [('Major', major, 'darkorange', 65), ('Internal', minor, 'royalblue', 20)]:
        for p in pts:
            ax.scatter(dates[p['pivot_i']], p['price'], s=size, c=color, marker='^' if p['type']=='H' else 'v', zorder=4)
        # Connect pivots in their causal, confirmed order.
        if pts:
            ax.plot([dates[p['pivot_i']] for p in pts], [p['price'] for p in pts], color=color, alpha=.55, linestyle='--', linewidth=1, label=kind+' swings')
    ax.axhline(row['entry'], color='crimson' if row['direction']=='SHORT' else 'green', alpha=.7, linestyle=':', label='Entry')
    ax.set_title(f"BTCUSDT 24h before #{row['signal_id']} {row['direction']} | Major {result['major']} | Internal {result['internal']}")
    ax.set_xlabel('UTC, 5-minute candles (closed before signal)')
    ax.set_ylabel('USDT')
    ax.xaxis.set_major_formatter(DateFormatter('%m-%d %H:%M', tz=timezone.utc))
    fig.autofmt_xdate()
    ax.grid(alpha=.2)
    ax.legend(loc='best')
    fig.tight_layout()
    fig.savefig(dest, dpi=130)
    plt.close(fig)
    return str(dest)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--db', type=Path, default=DB)
    ap.add_argument('--first', type=int, default=159)
    ap.add_argument('--last', type=int, default=179)
    ap.add_argument('--major-atr', type=float, default=1.5)
    ap.add_argument('--internal-atr', type=float, default=0.7)
    ap.add_argument('--range-tolerance-atr', type=float, default=0.35)
    ap.add_argument('--output', type=Path, default=Path('/tmp/btc_v4_structure_24h'))
    ap.add_argument('--charts', default='159,164,165,166,174,175,176,177,178,179', help='Comma-separated signal IDs, or empty string')
    args = ap.parse_args()
    if args.major_atr <= args.internal_atr or args.internal_atr <= 0 or args.range_tolerance_atr < 0:
        ap.error('require major-atr > internal-atr > 0 and nonnegative range tolerance')
    uri = 'file:' + str(args.db) + '?mode=ro'
    with sqlite3.connect(uri, uri=True) as conn:
        rows = [dict(zip(['signal_id', 'direction', 'entry', 'decision_ts'], r)) for r in conn.execute('''
            SELECT signal_id, direction, entry, decision_ts FROM paper_trades_v4
            WHERE signal_id BETWEEN ? AND ? ORDER BY signal_id
        ''', (args.first, args.last))]
    if not rows:
        raise RuntimeError('No trades found in read-only DB')
    # Each decision is evaluated with precisely the 288 fully CLOSED candles before it.
    starts = [int(r['decision_ts']) // FIVE * FIVE for r in rows]
    global_start = min(starts) - DAY - 20 * FIVE
    global_end = max(starts)
    all_bars = fetch_bybit(global_start, global_end)
    index = {b['timestamp']: b for b in all_bars}
    chart_ids = {int(x) for x in args.charts.split(',') if x.strip()}
    args.output.mkdir(parents=True, exist_ok=True)
    reports = []
    print('READ ONLY | Bybit linear BTCUSDT | 288 completed 5m candles per signal')
    print(f'ATR-confirmed causal swings: major={args.major_atr}, internal={args.internal_atr}, range tolerance={args.range_tolerance_atr}')
    print('ID  SIDE  MAJOR        INTERNAL     BOS_M BOS_I  H1->H2      L1->L2       NOTE')
    for row, decision_open in zip(rows, starts):
        # Bars [decision_open - DAY, decision_open) are all completed by decision time.
        times = range(decision_open - DAY, decision_open, FIVE)
        missing = [t for t in times if t not in index]
        if missing:
            print(f"{row['signal_id']:3} MISSING {len(missing)} historical candles; not classified")
            continue
        bars = [index[t] for t in times]
        atrs = atr_series(bars)
        major = swings(bars, atrs, args.major_atr)
        internal = swings(bars, atrs, args.internal_atr)
        m, mh, ml = classify(major, atrs[-1], args.range_tolerance_atr)
        i, ih, il = classify(internal, atrs[-1], args.range_tolerance_atr)
        def diff(pair):
            return f"{pair[-1]['price'] - pair[-2]['price']:+.1f}" if len(pair)==2 else 'N/A'
        note = 'AGAINST MAJOR' if (m in ('UP','DOWN') and row['direction'] != ('LONG' if m=='UP' else 'SHORT')) else '-'
        result = {'signal_id': row['signal_id'], 'direction': row['direction'], 'entry': row['entry'],
                  'decision_utc': datetime.fromtimestamp(row['decision_ts'], timezone.utc).isoformat(),
                  'major': m, 'internal': i, 'bos_major': bos(bars, major), 'bos_internal': bos(bars, internal),
                  'major_H1_H2': pivot_text(mh, bars), 'major_L1_L2': pivot_text(ml, bars),
                  'internal_H1_H2': pivot_text(ih, bars), 'internal_L1_L2': pivot_text(il, bars),
                  'major_high_diff': diff(mh), 'major_low_diff': diff(ml),
                  'internal_high_diff': diff(ih), 'internal_low_diff': diff(il),
                  'atr5': round(atrs[-1], 3), 'major_pivot_count': len(major),
                  'internal_pivot_count': len(internal), 'note': note}
        reports.append(result)
        print(f"{row['signal_id']:3} {row['direction']:5} {m:12} {i:12} {result['bos_major']:5} {result['bos_internal']:5} {diff(mh):>10} {diff(ml):>11}  {note}")
        if row['signal_id'] in chart_ids:
            chart = args.output / f"btc_24h_{row['signal_id']}.png"
            print('  CHART:', plot_chart(bars, major, internal, row, result, chart))
    if reports:
        csv_path = args.output / 'structure_report.csv'
        with csv_path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(reports[0]))
            writer.writeheader()
            writer.writerows(reports)
        print('CSV:', csv_path)
    print('FINISHED | No database writes, no service changes, no orders')


if __name__ == '__main__':
    main()
