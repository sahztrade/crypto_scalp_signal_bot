#!/usr/bin/env python3
# coding: utf-8
"""V13.3 read-only research. No trading, SQLite writes or Telegram calls.
Regime state is reconstructed from a finite 900-candle replay.
Thresholds are hypotheses, not validated trading filters.
"""
import argparse
import datetime as dt
import importlib.util
import json
import math
import time

V8_DEFAULT = '/opt/btc_v12_shadow/btc_v4_structure_context_v8_200_244.py'
CORE_DEFAULT = '/opt/btc_v13_research/BTC_V13_Range_Detection.py'
COUNT_5M = 900  # 75h: 900 x 5m; 300 x 15m; 75 x 1h if aligned
PERIODS = {'5m': 300, '15m': 900, '1h': 3600}


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError('Cannot load module: ' + str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def aggregate(bars, seconds):
    """Only completely closed and contiguous aligned higher-TF candles."""
    n = seconds // 300
    if n < 1 or n * 300 != seconds:
        raise ValueError('Bad timeframe')
    out = []
    groups = {}
    for bar in bars:
        start = (int(bar[0]) // seconds) * seconds
        groups.setdefault(start, []).append(bar)
    for start in sorted(groups):
        group = groups[start]
        if len(group) == n and all(int(b[0]) == start + i * 300 for i, b in enumerate(group)):
            out.append((start, group[0][1], max(b[2] for b in group),
                        min(b[3] for b in group), group[-1][4]))
    return out


def signed_ema_slope(bars, core, n=20, distance=5):
    closes = [b[4] for b in bars]
    alpha = 2 / (n + 1)
    ema = closes[0]
    values = []
    for close in closes:
        ema = alpha * close + (1 - alpha) * ema
        values.append(ema)
    atr = sum(core.true_ranges(bars)[-14:]) / 14
    if atr <= 0:
        raise ValueError('Zero ATR')
    return (values[-1] - values[-1 - distance]) / atr, ema, atr


def quantile(values, q):
    values = sorted(values)
    return values[int((len(values)-1)*q)]


def validate(bars, epoch):
    if len(bars) != COUNT_5M:
        raise ValueError('Expected exactly 900 V8 candles')
    for i, b in enumerate(bars):
        if len(b) < 5 or not all(math.isfinite(float(x)) for x in b[:5]):
            raise ValueError('Invalid OHLC row')
        if int(b[0]) != b[0] or int(b[0]) % 300:
            raise ValueError('Unaligned candle timestamp')
        if b[3] > min(b[1], b[4]) or b[2] < max(b[1], b[4]) or b[3] <= 0:
            raise ValueError('Invalid candle prices')
        if i and int(b[0])-int(bars[i-1][0]) != 300:
            raise ValueError('Duplicate, unordered or missing candle')
    expected = int(epoch)//300*300-300
    if int(bars[-1][0]) != expected:
        raise ValueError('Stale/latest candle mismatch')


class Regime:
    def __init__(self):
        self.box = None
        self.pending = None
        self.count = 0

    def step(self, data, tf, core):
        slope, ema, atr = signed_ema_slope(data, core)
        adx, chop, er = core.adx(data), core.chop(data), core.efficiency(data)
        if any(x is None or not math.isfinite(x) for x in (slope, ema, atr, adx, chop, er)):
            raise ValueError('Invalid indicator')
        price = data[-1][4]
        move = (price-data[-4][4])/atr
        direction = 'UP' if move > 0.25 else 'DOWN' if move < -0.25 else 'FLAT'
        reason = 'NO_CONFIRMED_STRUCTURE'
        regime = 'UNCERTAIN'
        if self.box is None:
            # Candidate boundaries use only candles preceding this decision candle.
            n = {'5m':72, '15m':40, '1h':32}[tf]
            history = data[-n-1:-1]
            if len(history) == n:
                support = quantile([b[3] for b in history], 0.10)
                resistance = quantile([b[2] for b in history], 0.90)
                width = resistance-support
                net = abs(history[-1][4]-history[0][4])
                travel = sum(abs(history[i][4]-history[i-1][4]) for i in range(1,n))
                long_er = net/travel if travel else 0.0
                halves = (history[:n//2], history[n//2:])
                touches = all(any(b[3] <= support+0.2*width for b in half) and
                              any(b[2] >= resistance-0.2*width for b in half) for half in halves)
                if (0 < width <= 8*atr and long_er <= 0.30 and touches and
                        support <= price <= resistance):
                    self.box = {'support':support, 'resistance':resistance,
                                'buffer':0.25*atr, 'created_open_ts':int(data[-1][0])}
                    reason = 'RANGE_ESTABLISHED'
        if self.box is not None:
            box = self.box
            side = ('UP' if price > box['resistance']+box['buffer'] else
                    'DOWN' if price < box['support']-box['buffer'] else None)
            self.count = self.count+1 if side and side == self.pending else 1 if side else 0
            self.pending = side
            regime = 'RANGE'
            if reason != 'RANGE_ESTABLISHED':
                reason = 'BREAKOUT_PENDING' if side else 'RANGE_HELD'
            if self.count >= 2:
                regime, reason = side, 'STRUCTURAL_BREAKOUT_CONFIRMED'
                self.box, self.pending, self.count = None, None, 0
            support, resistance = box['support'], box['resistance']
            buffer = box['buffer']
            created = box['created_open_ts']
        else:
            support = resistance = buffer = created = None
            if adx >= 20 and slope >= 0.25 and price > ema:
                regime, reason = 'UP', 'DIRECTIONAL_TREND_WITHOUT_ACTIVE_RANGE'
            elif adx >= 20 and slope <= -0.25 and price < ema:
                regime, reason = 'DOWN', 'DIRECTIONAL_TREND_WITHOUT_ACTIVE_RANGE'
        return {'regime':regime, 'direction':direction, 'reason':reason,
                'last_closed_open_ts':int(data[-1][0]),
                'last_closed_end_utc':iso(int(data[-1][0])+PERIODS[tf]),
                'close':price, 'support':support, 'resistance':resistance,
                'breakout_buffer':buffer, 'range_created_open_ts':created,
                'pending_breakout':self.pending, 'confirmation_closes':self.count,
                'adx14':round(adx,3), 'chop14':round(chop,3), 'er20':round(er,3),
                'ema20_slope5_atr':round(slope,3), 'atr14':round(atr,3)}


def iso(epoch):
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat()


def replay(bars, core):
    machines = {tf:Regime() for tf in PERIODS}
    latest, last_seen = {}, {}
    for i in range(64,len(bars)):
        prefix = bars[:i+1]
        for tf, seconds in PERIODS.items():
            frame = prefix if tf == '5m' else aggregate(prefix,seconds)
            if len(frame) < 65 or last_seen.get(tf) == frame[-1][0]:
                continue
            latest[tf] = machines[tf].step(frame,tf,core)
            last_seen[tf] = frame[-1][0]
        yield {'symbol':'BTCUSDT', 'mode':'V13_3_INDEPENDENT_RESEARCH',
               'closed_5m_open_ts':int(prefix[-1][0]),
               'checked_closed_5m_utc':iso(int(prefix[-1][0])+300),
               'replay_source_start_utc':iso(int(bars[0][0])),
               'timeframes':dict(latest)}


def timestamp(value):
    parsed = dt.datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError('Timestamp must include UTC offset')
    return parsed.timestamp()


def main():
    parser = argparse.ArgumentParser(description='Read-only V13.3 structural regime research')
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--once',action='store_true')
    mode.add_argument('--replay',action='store_true',help='JSON lines; fixed 900-candle history')
    parser.add_argument('--epoch',type=float)
    parser.add_argument('--start',type=timestamp,help='Replay decision close time, inclusive')
    parser.add_argument('--end',type=timestamp,help='Replay decision close time, inclusive')
    parser.add_argument('--v8',default=V8_DEFAULT)
    parser.add_argument('--core',default=CORE_DEFAULT)
    args = parser.parse_args()
    if args.once and (args.start is not None or args.end is not None):
        parser.error('start/end require --replay')
    if args.replay and (args.start is None or args.end is None):
        parser.error('--replay requires --start and --end with timezone offsets')
    if args.replay and args.epoch is not None:
        parser.error('--epoch is only for --once')
    if args.replay and (args.start > args.end or args.end-args.start > 48*3600):
        parser.error('Replay interval must be ordered and at most 48 hours')
    epoch = args.end if args.replay else args.epoch if args.epoch is not None else time.time()-10
    v8 = load_module(args.v8,'v13_3_existing_v8')
    core = load_module(args.core,'v13_3_existing_core')
    bars = v8.candles(float(epoch),COUNT_5M)
    validate(bars,epoch)
    results = replay(bars,core)
    if args.once:
        result = None
        for result in results:
            pass
        if result is None or len(result['timeframes']) != 3:
            raise ValueError('Insufficient timeframe warmup')
        print(json.dumps(result,ensure_ascii=False,indent=2))
    else:
        for result in results:
            close_ts = result['closed_5m_open_ts']+300
            if args.start <= close_ts <= args.end:
                print(json.dumps(result,ensure_ascii=False))


if __name__ == '__main__':
    main()
