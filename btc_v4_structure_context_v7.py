#!/usr/bin/env python3
"""BTC V7 historical market-structure audit. READ ONLY.

Uses 288 contiguous, CLOSED Bybit linear BTCUSDT 5-minute candles at each
signal's decision_ts. No future candles, order placement, or database writes.

Heuristic structural definitions are exploratory, not trading rules.
"""
import argparse
import csv
import datetime as datetime
import json
import math
import sqlite3
import time
import urllib.parse
import urllib.request
from pathlib import Path

DEFAULT_DB = '/opt/tmco_webhook_v4/data/v4_paper_live.db'
DEFAULT_OUT = '/tmp/btc_v4_structure_context_v7'
URL = 'https://api.bybit.com/v5/market/kline'


def candles(epoch, count=288):
    """Return count contiguous 5m candles that were closed by epoch."""
    boundary = int(epoch // 300) * 300
    earliest = boundary - (count + 45) * 300
    data = {}
    cursor = boundary * 1000 - 1
    for _ in range(5):
        query = urllib.parse.urlencode({
            'category': 'linear', 'symbol': 'BTCUSDT', 'interval': '5',
            'end': cursor, 'limit': 200,
        })
        req = urllib.request.Request(URL + '?' + query, headers={'User-Agent': 'BTC-V7-readonly/1.0'})
        with urllib.request.urlopen(req, timeout=30) as response:
            payload = json.load(response)
        if payload.get('retCode') != 0:
            raise RuntimeError('Bybit: ' + str(payload.get('retMsg', payload)))
        batch = payload.get('result', {}).get('list', [])
        if not batch:
            break
        for item in batch:
            t = int(item[0]) // 1000
            if earliest <= t and t + 300 <= epoch:
                data[t] = (t, float(item[1]), float(item[2]), float(item[3]), float(item[4]))
        oldest = min(int(item[0]) for item in batch)
        if oldest <= earliest * 1000:
            break
        cursor = oldest - 1
        time.sleep(0.08)
    result = sorted(data.values())[-count:]
    if len(result) != count:
        raise RuntimeError('Incomplete closed bars: got %s / %s' % (len(result), count))
    gaps = [(i, result[i][0] - result[i - 1][0]) for i in range(1, count)
            if result[i][0] - result[i - 1][0] != 300]
    if gaps:
        raise RuntimeError('Non-contiguous bars: ' + repr(gaps[:3]))
    return result


def atr(b, index, length=14):
    if index < length:
        return None
    trs = []
    for i in range(index - length + 1, index + 1):
        trs.append(max(b[i][2] - b[i][3], abs(b[i][2] - b[i - 1][4]),
                       abs(b[i][3] - b[i - 1][4])))
    return sum(trs) / len(trs)


def pivots(b, left=2, right=2, min_atr=0.7):
    """Pivot is only known at confirmation index i+right."""
    found = []
    for i in range(max(15, left), len(b) - right):
        confirmed = i + right
        a = atr(b, confirmed)
        if not a or a <= 0:
            continue
        local = b[i - left:i + right + 1]
        h, lo = b[i][2], b[i][3]
        if (h > max(x[2] for x in b[i - left:i])
                and h >= max(x[2] for x in b[i + 1:i + right + 1])
                and h - min(x[3] for x in local) >= min_atr * a):
            found.append({'type': 'H', 'price': h, 'i': i, 'confirmed': confirmed, 'atr': a})
        if (lo < min(x[3] for x in b[i - left:i])
                and lo <= min(x[3] for x in b[i + 1:i + right + 1])
                and max(x[2] for x in local) - lo >= min_atr * a):
            found.append({'type': 'L', 'price': lo, 'i': i, 'confirmed': confirmed, 'atr': a})
    return sorted(found, key=lambda x: (x['confirmed'], x['i'], x['type']))


def major_pivots(internal, threshold=1.8):
    """Alternating significant turns; threshold uses ATR at pivot confirmation.

    Candidate replacement is based only on information available when each
    pivot was confirmed. It does NOT backdate availability of a replacement.
    """
    result = []
    for p in internal:
        if not result:
            result.append(p)
            continue
        last = result[-1]
        if p['type'] == last['type']:
            if ((p['type'] == 'H' and p['price'] > last['price']) or
                    (p['type'] == 'L' and p['price'] < last['price'])):
                result[-1] = p
        elif abs(p['price'] - last['price']) >= threshold * p['atr']:
            result.append(p)
    return result


def trend(major, a):
    highs = [x['price'] for x in major if x['type'] == 'H'][-2:]
    lows = [x['price'] for x in major if x['type'] == 'L'][-2:]
    if len(highs) < 2 or len(lows) < 2:
        return 'INSUFFICIENT'
    dh, dl = highs[-1] - highs[-2], lows[-1] - lows[-2]
    tol = 0.35 * a
    if dh > tol and dl > tol:
        return 'UP'
    if dh < -tol and dl < -tol:
        return 'DOWN'
    if dh > tol and dl < -tol:
        return 'EXPANDING_RANGE'
    if dh < -tol and dl > tol:
        return 'CONTRACTING_RANGE'
    return 'TRANSITION'


def break_events(b, internal, threshold=1.8, major=False):
    """Detect unique level breaks causally by replaying confirmed pivots.

    A broken pivot level is counted once, even if price crosses it repeatedly.
    Major pivot classification is recomputed at each bar, so no final-list
    hindsight is used for historical BOS/CHoCH labels.
    """
    events = []
    broken = set()
    for k in range(16, len(b)):
        available = [p for p in internal if p['confirmed'] < k]
        eligible = major_pivots(available, threshold) if major else available
        for kind, direction in (('H', 'UP'), ('L', 'DOWN')):
            levels = [p for p in eligible if p['type'] == kind]
            if not levels:
                continue
            p = levels[-1]
            key = (p['i'], p['type'])
            if key in broken:
                continue
            v = p['price']
            crossed = ((b[k - 1][4] <= v < b[k][4]) if kind == 'H'
                       else (b[k - 1][4] >= v > b[k][4]))
            if crossed:
                events.append({'k': k, 'direction': direction, 'level': v,
                               'pivot_i': p['i'], 'pivot_confirmed': p['confirmed']})
                broken.add(key)
    return events


def fmt_timestamp(b, i):
    return datetime.datetime.fromtimestamp(b[i][0], datetime.timezone.utc).strftime('%Y-%m-%d %H:%M')


def describe_pivot(b, p):
    if p is None:
        return '', '', ''
    return round(p['price'], 2), fmt_timestamp(b, p['i']), fmt_timestamp(b, p['confirmed'])


def analyze(b, side, entry):
    if side not in ('LONG', 'SHORT'):
        raise ValueError('Unknown side: ' + side)
    a = atr(b, len(b) - 1)
    if not a or a <= 0:
        raise ValueError('Invalid ATR')
    internal = pivots(b)
    major = major_pivots(internal)
    last = major[-1] if major else None
    previous = major[-2] if len(major) >= 2 else None
    # Age starts at CONFIRMATION, never at unknowable pivot occurrence.
    leg_age_confirmed = len(b) - 1 - last['confirmed'] if last else None
    leg_age_since_extreme = len(b) - 1 - last['i'] if last else None
    leg_atr = (entry - last['price']) / a if last else None
    leg_direction = 'UP' if leg_atr is not None and leg_atr > 0 else 'DOWN' if leg_atr is not None and leg_atr < 0 else 'FLAT'
    # Position inside the last *confirmed* opposite-type major pivot range.
    # May be outside 0..100%; that is meaningful and not clamped.
    swing_position = None
    if previous and last and previous['type'] != last['type']:
        low, high = sorted((previous['price'], last['price']))
        if high > low:
            swing_position = 100 * (entry - low) / (high - low)
    # Structural level in the trade direction, from confirmed major pivots.
    opposite = [p for p in major if ((side == 'LONG' and p['type'] == 'H' and p['price'] > entry)
                                        or (side == 'SHORT' and p['type'] == 'L' and p['price'] < entry))]
    obstacle = min(opposite, key=lambda p: abs(p['price'] - entry)) if opposite else None
    distance = abs(obstacle['price'] - entry) / a if obstacle else None
    major_events = break_events(b, internal, major=True)
    internal_events = break_events(b, internal, major=False)
    recent_major = [e for e in major_events if e['k'] >= len(b) - 36]
    recent_internal = [e for e in internal_events if e['k'] >= len(b) - 36]
    # CHoCH proxy: opposite direction to the preceding MAJOR break, not a
    # formal validated CHoCH. This distinction is explicit in the report.
    last_event = major_events[-1] if major_events else None
    prev_event = major_events[-2] if len(major_events) > 1 else None
    choch_proxy = bool(last_event and prev_event and last_event['direction'] != prev_event['direction'])
    # Retest: after a major break, a later CLOSED candle touches the level
    # (within 0.25 ATR at its own time) and closes on the breakout side.
    retest = False
    retest_k = None
    if last_event:
        level = last_event['level']
        for k in range(last_event['k'] + 1, len(b)):
            ak = atr(b, k) or a
            touched = b[k][3] - .25 * ak <= level <= b[k][2] + .25 * ak
            held = b[k][4] > level if last_event['direction'] == 'UP' else b[k][4] < level
            if touched and held:
                retest, retest_k = True, k
                break
    last_price, last_time, last_confirmed = describe_pivot(b, last)
    prev_price, prev_time, prev_confirmed = describe_pivot(b, previous)
    obstacle_price, obstacle_time, obstacle_confirmed = describe_pivot(b, obstacle)
    phase = ('NO_MAJOR_PIVOT' if last is None else
             'NEAR_OPPOSITE_LEVEL' if distance is not None and distance < .75 else
             'APPROACH_OPPOSITE_LEVEL' if distance is not None and distance < 1.5 else
             'NO_OVERHEAD_LEVEL' if obstacle is None else 'OPEN_SPACE')
    return {
        'TREND': trend(major, a), 'PHASE': phase, 'ATR': round(a, 2),
        'INTERNAL_PIVOTS': len(internal), 'MAJOR_PIVOTS': len(major),
        'LEG_DIRECTION': leg_direction,
        'LEG_AGE_CONFIRMED_BARS': leg_age_confirmed,
        'LEG_AGE_EXTREME_BARS_DIAGNOSTIC': leg_age_since_extreme,
        'LEG_ATR_SIGNED': round(leg_atr, 3) if leg_atr is not None else '',
        'SWING_POSITION_PCT': round(swing_position, 1) if swing_position is not None else '',
        'OPPOSITE_LEVEL_ATR': round(distance, 3) if distance is not None else '',
        'OPPOSITE_LEVEL_PRICE': obstacle_price,
        'OPPOSITE_LEVEL_PIVOT_UTC': obstacle_time,
        'OPPOSITE_LEVEL_CONFIRMED_UTC': obstacle_confirmed,
        'LAST_MAJOR_PIVOT_TYPE': last['type'] if last else '',
        'LAST_MAJOR_PIVOT_PRICE': last_price,
        'LAST_MAJOR_PIVOT_UTC': last_time,
        'LAST_MAJOR_PIVOT_CONFIRMED_UTC': last_confirmed,
        'PREV_MAJOR_PIVOT_TYPE': previous['type'] if previous else '',
        'PREV_MAJOR_PIVOT_PRICE': prev_price,
        'PREV_MAJOR_PIVOT_UTC': prev_time,
        'PREV_MAJOR_PIVOT_CONFIRMED_UTC': prev_confirmed,
        'MAJOR_BOS_UP_3H_PROXY': sum(e['direction'] == 'UP' for e in recent_major),
        'MAJOR_BOS_DOWN_3H_PROXY': sum(e['direction'] == 'DOWN' for e in recent_major),
        'INTERNAL_BREAK_UP_3H': sum(e['direction'] == 'UP' for e in recent_internal),
        'INTERNAL_BREAK_DOWN_3H': sum(e['direction'] == 'DOWN' for e in recent_internal),
        'LAST_MAJOR_BREAK_DIRECTION': last_event['direction'] if last_event else '',
        'LAST_MAJOR_BREAK_UTC': fmt_timestamp(b, last_event['k']) if last_event else '',
        'LAST_MAJOR_BREAK_LEVEL': round(last_event['level'], 2) if last_event else '',
        'CHOCH_PROXY': 'YES' if choch_proxy else 'NO' if last_event else 'UNKNOWN',
        'RETEST_AFTER_LAST_MAJOR_BREAK': 'YES' if retest else 'NO' if last_event else 'UNKNOWN',
        'RETEST_UTC': fmt_timestamp(b, retest_k) if retest_k is not None else '',
        'LAST_CLOSED_BAR_UTC': fmt_timestamp(b, len(b) - 1),
    }


def main():
    parser = argparse.ArgumentParser(description='READ-ONLY historical BTC V7 structural audit')
    parser.add_argument('--db', default=DEFAULT_DB)
    parser.add_argument('--out', default=DEFAULT_OUT)
    args = parser.parse_args()
    db_uri = Path(args.db).resolve().as_uri() + '?mode=ro'
    con = sqlite3.connect(db_uri, uri=True)
    try:
        trades = con.execute('''SELECT signal_id, decision_ts, direction, entry
                                FROM paper_trades_v4
                                WHERE signal_id BETWEEN 159 AND 179
                                ORDER BY signal_id''').fetchall()
    finally:
        con.close()
    if not trades:
        raise RuntimeError('No BTC paper trades 159..179')
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    records = []
    print('BTC V7 READ ONLY | 288 completed Bybit BTCUSDT 5m bars per signal')
    print('ID SIDE TREND PHASE AGE LEG_ATR SWING% LEVEL_ATR BOS_UP/DN CHOCH RETEST')
    for sid, ts, side, entry in trades:
        try:
            side = str(side).upper()
            b = candles(float(ts))
            r = analyze(b, side, float(entry))
            record = {'ID': sid, 'SIDE': side, 'ENTRY': entry,
                      'DECISION_UTC': datetime.datetime.fromtimestamp(float(ts), datetime.timezone.utc).isoformat(),
                      **r}
            records.append(record)
            print(sid, side, r['TREND'], r['PHASE'], r['LEG_AGE_CONFIRMED_BARS'],
                  r['LEG_ATR_SIGNED'], r['SWING_POSITION_PCT'], r['OPPOSITE_LEVEL_ATR'],
                  str(r['MAJOR_BOS_UP_3H_PROXY']) + '/' + str(r['MAJOR_BOS_DOWN_3H_PROXY']),
                  r['CHOCH_PROXY'], r['RETEST_AFTER_LAST_MAJOR_BREAK'])
        except Exception as exc:
            records.append({'ID': sid, 'SIDE': side, 'ERROR': str(exc)})
            print('ERROR', sid, str(exc))
    fields = list(dict.fromkeys(key for record in records for key in record))
    report = out / 'structure_context_report.csv'
    with report.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)
    comparisons = out / 'key_comparisons.csv'
    chosen = [r for r in records if r['ID'] in (159, 164, 166, 174)]
    with comparisons.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(chosen)
    print('REPORT:', report)
    print('KEY COMPARISONS:', comparisons)
    print('CAUTION: BOS and CHoCH are heuristic proxies, not validated signals.')
    print('FINISHED | no orders, no database writes, no service changes')


if __name__ == '__main__':
    main()
