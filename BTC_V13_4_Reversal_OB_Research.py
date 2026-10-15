#!/usr/bin/env python3
# coding: utf-8
"""V13.4 independent reversal/OB research. No trades, Telegram or live changes.
This is an early-warning layer, NOT a replacement for the 5m regime monitor.
All timestamps are candle CLOSE times unless named open_ts.
"""
import argparse
import datetime as dt
import json
import math
import urllib.parse
import urllib.request

IRAN = dt.timezone(dt.timedelta(hours=3, minutes=30))
STEP = 180
LOOKBACK = 10
EXPIRY = 6
TRACKING_BARS = 20


def iso(t):
    return dt.datetime.fromtimestamp(t, IRAN).isoformat()


def validate(bars):
    if not bars:
        raise ValueError('Empty candles')
    for i, b in enumerate(bars):
        if len(b) < 5 or not all(isinstance(v, (int, float)) and math.isfinite(v) for v in b[:5]):
            raise ValueError('Invalid OHLC')
        if b[0] != int(b[0]) or int(b[0]) % STEP:
            raise ValueError('Unaligned 3m candle')
        if b[3] <= 0 or b[3] > min(b[1], b[4]) or b[2] < max(b[1], b[4]):
            raise ValueError('Invalid price bounds')
        if i and b[0]-bars[i-1][0] != STEP:
            raise ValueError('Missing/duplicate/unordered candles')


def analyze(bars):
    validate(bars)
    events = []
    pending = {}
    confirmed = []

    def emit(kind, i, candidate, **extra):
        event = dict(event=kind, side=candidate['side'],
                     time_iran=iso(bars[i][0]+STEP), close_ts=int(bars[i][0]+STEP),
                     close=bars[i][4], sweep_open_ts=candidate['sweep_open_ts'],
                     trigger=candidate['trigger'], reclaimed_level=candidate['level'],
                     sweep_extreme=candidate['extreme'], **extra)
        events.append(event)
        return event

    for i in range(LOOKBACK, len(bars)):
        b = bars[i]
        for state in confirmed:
            if not state['active']:
                continue
            bull = state['side'] == 'UP'
            zone = state['zone']
            if i-state['confirmed_index'] > TRACKING_BARS:
                emit('REVERSAL_TRACK_EXPIRED', i, state)
                state['active'] = False
                continue
            invalid_sweep = b[4] < state['extreme'] if bull else b[4] > state['extreme']
            if invalid_sweep:
                emit('REVERSAL_INVALIDATED', i, state, zone=zone)
                if zone is not None:
                    emit('OB_INVALIDATED', i, state, zone=zone)
                state['active'] = False
                continue
            if zone is not None:
                # Invalidation is by CLOSE beyond the opposite edge, not wick touch.
                invalid = b[4] < zone['low'] if bull else b[4] > zone['high']
                if invalid:
                    emit('OB_INVALIDATED', i, state, zone=zone)
                    state['active'] = False
                    continue
                if not state['touched'] and b[3] <= zone['high'] and b[2] >= zone['low']:
                    state['touched'] = True
                    emit('OB_FIRST_RETEST', i, state, zone=zone)
            beyond = b[4] > state['context'] if bull else b[4] < state['context']
            if not state['extended'] and beyond:
                state['extended'] = True
                emit('CONTEXT_BREAK_CONFIRMED', i, state,
                     context_level=state['context'], zone=zone)

        for side in list(pending):
            c = pending[side]
            bull = side == 'UP'
            invalid = b[4] < c['extreme'] if bull else b[4] > c['extreme']
            if invalid:
                emit('REVERSAL_CANCELLED', i, c, reason='CLOSE_BEYOND_SWEEP_EXTREME')
                del pending[side]
                continue
            if i-c['index'] > EXPIRY:
                emit('REVERSAL_EXPIRED', i, c)
                del pending[side]
                continue
            outside = b[4] > c['trigger'] if bull else b[4] < c['trigger']
            if outside:
                if c['count'] == 0:
                    c['first_index'] = i
                    emit('LOCAL_BREAK_PENDING', i, c)
                c['count'] += 1
            else:
                c['count'] = 0
                c['first_index'] = None
            if c['count'] >= 2:
                # Last opposite candle from sweep through BEFORE first breakout close.
                zone = None
                for j in range(c['first_index']-1, c['index']-1, -1):
                    opposite = bars[j][4] < bars[j][1] if bull else bars[j][4] > bars[j][1]
                    if opposite:
                        zone = dict(low=bars[j][3], high=bars[j][2],
                                    candle_open_ts=int(bars[j][0]),
                                    known_at_close_ts=int(b[0]+STEP))
                        break
                emit('REVERSAL_CONFIRMED', i, c, confirmation_closes=2, zone=zone,
                     context_level=c['context'], context_already_broken=(b[4]>c['context'] if bull else b[4]<c['context']))
                confirmed.append(dict(c, confirmed_index=i, zone=zone, active=True, touched=False,
                                      extended=(b[4]>c['context'] if bull else b[4]<c['context'])))
                del pending[side]

        history = bars[i-LOOKBACK:i]
        prior_low = min(x[3] for x in history)
        prior_high = max(x[2] for x in history)
        # Candidate: wick beyond the preceding 30m extreme, reclaimed on the same close,
        # with body pointing in the proposed direction. Strict comparisons throughout.
        signals = []
        if b[3] < prior_low and b[4] > prior_low and b[4] > b[1]:
            signals.append(('UP', prior_low, b[3], b[2], prior_high))
        if b[2] > prior_high and b[4] < prior_high and b[4] < b[1]:
            signals.append(('DOWN', prior_high, b[2], b[3], prior_low))
        for side, level, extreme, trigger, context in signals:
            if side in pending:
                emit('REVERSAL_REPLACED', i, pending[side])
            c = dict(side=side, index=i, sweep_open_ts=int(b[0]),
                     level=level, extreme=extreme, trigger=trigger, context=context,
                     count=0, first_index=None)
            pending[side] = c
            emit('REVERSAL_CANDIDATE', i, c, context_level=context)
    return events


def fetch(day, category):
    midnight = dt.datetime.combine(day, dt.time(), IRAN).timestamp()
    start = int(midnight-6*3600)
    end = int(midnight+86400)
    params = urllib.parse.urlencode(dict(category=category, symbol='BTCUSDT', interval='3',
                                         start=start*1000, end=end*1000-1, limit=1000))
    request = urllib.request.Request('https://api.bybit.com/v5/market/kline?'+params,
                                     headers={'User-Agent':'BTC-V13.4-research'})
    with urllib.request.urlopen(request, timeout=30) as response:
        answer = json.load(response)
    if answer.get('retCode') != 0:
        raise ValueError('Bybit response error: '+str(answer.get('retMsg')))
    bars = sorted([[int(r[0])//1000]+[float(v) for v in r[1:5]]
                   for r in answer['result']['list'] if start*1000 <= int(r[0]) < end*1000])
    validate(bars)
    if len(bars) != 600 or bars[0][0] != start or bars[-1][0]+STEP != end:
        raise ValueError('Incomplete 30h source window')
    return bars


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date', required=True, type=dt.date.fromisoformat, help='Iran calendar date YYYY-MM-DD')
    parser.add_argument('--input', help='Offline JSON object containing candles; otherwise public Bybit GET')
    parser.add_argument('--category', choices=('linear','spot','inverse'), default='linear')
    args = parser.parse_args()
    end = dt.datetime.combine(args.date+dt.timedelta(days=1), dt.time(), IRAN).timestamp()
    start = end-86400
    if end > dt.datetime.now(dt.timezone.utc).timestamp():
        parser.error('Choose a completed day')
    if args.input:
        with open(args.input, encoding='utf-8-sig') as file:
            source = json.load(file)
        if source.get('symbol') != 'BTCUSDT' or source.get('interval') != '3m' or source.get('category') != args.category:
            parser.error('Input must identify BTCUSDT, 3m, and matching category')
        bars = source['candles']
        validate(bars)
        if bars[0][0] > start or bars[-1][0]+STEP < end:
            parser.error('Input must cover the entire requested Iran day')
    else:
        bars = fetch(args.date, args.category)
    events = [x for x in analyze(bars) if start <= x['close_ts'] < end]
    summary = dict(mode='V13_4_REVERSAL_OB_INDEPENDENT_RESEARCH', symbol='BTCUSDT',
                   category=args.category, day_iran=str(args.date),
                   source_start_iran=iso(bars[0][0]), source_end_iran=iso(bars[-1][0]+STEP),
                   warmup_minutes=max(0,int((start-bars[0][0])/60)),
                   first_eligible_close_iran=iso(bars[LOOKBACK][0]+STEP),
                   rules=dict(lookback_bars=LOOKBACK, expiry_bars=EXPIRY,
                              consecutive_closes=2, tracking_bars=TRACKING_BARS, candle_seconds=STEP),
                   counts={kind:sum(e['event']==kind for e in events) for kind in sorted({e['event'] for e in events})})
    print(json.dumps(dict(summary=summary),ensure_ascii=False))
    for event in events:
        print(json.dumps(event,ensure_ascii=False))


if __name__ == '__main__':
    main()
