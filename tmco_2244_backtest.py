#!/usr/bin/env python3
"""Read-only TMCO 2,2,4,4 historical multi-timeframe confirmation test.

Place beside bybit_futures_v4.py and run with /usr/bin/python3.
No database writes, messages, orders, or live bot changes.
"""
import argparse
import time
from datetime import datetime, timedelta, timezone
from bybit_futures_v4 import fetch

IRAN = timezone(timedelta(hours=3, minutes=30))
TIMEFRAMES = ('1m', '3m', '5m')
WINDOW = 600


def ema(values, length):
    output = []
    alpha = 2 / (length + 1)
    previous = None
    for value in values:
        if value is None:
            output.append(None)
            continue
        previous = value if previous is None else alpha * value + (1 - alpha) * previous
        output.append(previous)
    return output


def rsi(values, length):
    output = [None] * len(values)
    if len(values) <= length:
        return output
    changes = [values[i] - values[i-1] for i in range(1, length+1)]
    avg_gain = sum(max(c, 0) for c in changes) / length
    avg_loss = sum(max(-c, 0) for c in changes) / length

    def calc(gain, loss):
        if gain == 0 and loss == 0:
            return 50.0
        if loss == 0:
            return 100.0
        if gain == 0:
            return 0.0
        return 100 - 100 / (1 + gain / loss)

    output[length] = calc(avg_gain, avg_loss)
    for i in range(length+1, len(values)):
        change = values[i] - values[i-1]
        avg_gain = (avg_gain * (length-1) + max(change, 0)) / length
        avg_loss = (avg_loss * (length-1) + max(-change, 0)) / length
        output[i] = calc(avg_gain, avg_loss)
    return output


def tmco(candles):
    source = []
    prev_open = prev_close = None
    for c in candles:
        o, h, l, close = c['open'], c['high'], c['low'], c['close']
        ha_close = (o + h + l + close) / 4
        ha_open = (o + close) / 2 if prev_open is None else (prev_open + prev_close) / 2
        ha_high = max(h, ha_open, ha_close)
        ha_low = min(l, ha_open, ha_close)
        source.append((ha_open + ha_high + ha_low + ha_close) / 4)
        prev_open, prev_close = ha_open, ha_close
    line = ema(rsi(source, 2), 2)
    wave = ema(rsi(source, 4), 4)
    return line, wave


def crossings(candles, tf):
    line, wave = tmco(candles)
    seconds = int(tf[:-1]) * 60
    events = []
    for i in range(1, len(candles)):
        if None in (line[i-1], wave[i-1], line[i], wave[i]):
            continue
        before, after = wave[i-1] - line[i-1], wave[i] - line[i]
        direction = 'LONG' if before >= 0 and after < 0 else ('SHORT' if before <= 0 and after > 0 else None)
        if direction:
            events.append({'time': candles[i]['timestamp'] + seconds, 'tf': tf,
                           'direction': direction, 'line': line[i], 'wave': wave[i]})
    return events


def stamp(t):
    return datetime.fromtimestamp(t, IRAN).strftime('%Y-%m-%d %H:%M')


def evaluate(events):
    # Group same-close events. They are simultaneous; do not allow artificial
    # order within a timestamp to interrupt a same-time confirmation.
    grouped = {}
    for event in events:
        grouped.setdefault(event['time'], []).append(event)
    chain = []
    active = None
    output = []
    for t in sorted(grouped):
        group = sorted(grouped[t], key=lambda x: TIMEFRAMES.index(x['tf']))
        directions = {e['direction'] for e in group}
        if len(directions) > 1:
            # Simultaneous opposite directions: ambiguous ordering; no confirmation.
            chain = []
            active = None
            output.append({'type': 'AMBIGUOUS', 'time': t, 'group': group})
            continue
        direction = group[0]['direction']
        if chain and chain[-1]['direction'] != direction:
            chain = []
            active = None
        if active and active['direction'] != direction:
            active = None
        # Add all events of this timestamp before comparing pairs.
        old = chain[:]
        chain.extend(group)
        if active:
            if not active['strengthened']:
                new_tfs = {e['tf'] for e in chain if e['time'] >= active['start'] and e['direction'] == direction}
                if len(new_tfs) == 3:
                    active['strengthened'] = True
                    output.append({'type': 'STRENGTHENED', 'time': t, 'direction': direction,
                                   'tfs': '/'.join(sorted(new_tfs, key=TIMEFRAMES.index))})
            continue
        # Consecutive pair test: when a third crossing arrives, pair (2,3)
        # is evaluated even if pair (1,2) was outside the 10-minute window.
        # Events at same time can pair only across distinct timeframes.
        candidate_pairs = []
        for j, right in enumerate(chain):
            if right['time'] != t:
                continue
            for left in chain[:j]:
                if left['tf'] != right['tf'] and 0 <= right['time']-left['time'] <= WINDOW:
                    candidate_pairs.append((left, right))
        if candidate_pairs:
            left, right = max(candidate_pairs, key=lambda pair: (pair[0]['time'], pair[1]['time']))
            active = {'direction': direction, 'start': left['time'], 'strengthened': False}
            output.append({'type': 'CONFIRMED', 'time': t, 'direction': direction,
                           'first': left, 'second': right,
                           'gap': right['time']-left['time']})
            if len({e['tf'] for e in chain if e['time'] >= left['time']}) == 3:
                active['strengthened'] = True
                output.append({'type': 'STRENGTHENED', 'time': t, 'direction': direction,
                               'tfs': '1m/3m/5m'})
    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, default=500, help='Closed candles per timeframe (max 990)')
    args = parser.parse_args()
    if not 30 <= args.count <= 990:
        parser.error('--count must be between 30 and 990')
    decision = int(time.time())
    all_events = []
    print('TMCO 2244 | BTCUSDT Bybit linear | read-only')
    for tf in TIMEFRAMES:
        candles = fetch(tf, decision, args.count)
        events = crossings(candles, tf)
        all_events.extend(events)
        print(f'{tf}: {len(candles)} closed candles; {len(events)} crossings; '
              f'{stamp(candles[0]["timestamp"])} to {stamp(candles[-1]["timestamp"] + int(tf[:-1])*60)}')
    # Common overlap is essential: 1m has a shorter historical lookback than 5m.
    starts = [min(e['time'] for e in all_events if e['tf'] == tf) for tf in TIMEFRAMES]
    common_start = max(starts)
    common_end = min(max(e['time'] for e in all_events if e['tf'] == tf) for tf in TIMEFRAMES)
    # Discard early RSI/EMA warmup to reduce startup effects.
    common_start = max(common_start, decision - (args.count - 30) * 60)
    events = sorted((e for e in all_events if common_start <= e['time'] <= common_end),
                    key=lambda e: (e['time'], TIMEFRAMES.index(e['tf'])))
    results = evaluate(events)
    print(f'\nCOMMON CROSSING WINDOW: {stamp(common_start)} -> {stamp(common_end)}')
    print(f'COMMON CROSSES: {len(events)}')
    print(f'CONFIRMED: {sum(x["type"] == "CONFIRMED" for x in results)}')
    print(f'STRENGTHENED: {sum(x["type"] == "STRENGTHENED" for x in results)}')
    print(f'AMBIGUOUS SIMULTANEOUS: {sum(x["type"] == "AMBIGUOUS" for x in results)}')
    print('\nLAST 30 EVENTS (IRAN TIME):')
    for r in results[-30:]:
        if r['type'] == 'CONFIRMED':
            print(f'{stamp(r["time"])} CONFIRMED {r["direction"]} '
                  f'{r["first"]["tf"]}+{r["second"]["tf"]} '
                  f'gap={r["gap"]//60}m first={stamp(r["first"]["time"])}')
        elif r['type'] == 'STRENGTHENED':
            print(f'{stamp(r["time"])} STRENGTHENED {r["direction"]} {r["tfs"]}')
        else:
            print(f'{stamp(r["time"])} AMBIGUOUS simultaneous opposite directions')
    print('\nTEST COMPLETE - NO LIVE CHANGES')

if __name__ == '__main__':
    main()
