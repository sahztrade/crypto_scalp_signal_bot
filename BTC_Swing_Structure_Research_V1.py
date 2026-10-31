#!/usr/bin/env python3
"""BTC Swing V1 research. Public Bybit data only; no orders, services or Telegram.
Python 3.10+, standard library. See accompanying README for execution assumptions.
"""
import argparse
import bisect
import collections
import csv
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING, ROUND_HALF_UP

VERSION = 'BTC-SWING-STRUCTURE-V1'
UTC = dt.timezone.utc
IRAN = dt.timezone(dt.timedelta(hours=3, minutes=30))
POLICY = dict(pivot_left=2, pivot_right=2, breakout_atr=.1,
              retest_atr=.2, retest_bars=6, stop_buffer_atr=.2,
              min_stop_atr=.5, max_stop_atr=2.5, range_bars=20,
              range_width_atr=4., target_R=2., max_chase_R=.1)


def stamp(t):
    return dt.datetime.fromtimestamp(t, IRAN).isoformat()


def epoch(s):
    value = dt.datetime.fromisoformat(s.replace('Z', '+00:00'))
    if value.tzinfo is None:
        raise ValueError('Dates must include timezone, e.g. +03:30')
    return int(value.timestamp())


def tick_round(value, tick, mode='nearest'):
    rounding = {'down': ROUND_FLOOR, 'up': ROUND_CEILING,
                'nearest': ROUND_HALF_UP}[mode]
    return float((Decimal(str(value)) / Decimal(str(tick))).to_integral_value(
        rounding=rounding) * Decimal(str(tick)))


def validate_bar(b):
    if len(b) != 6 or not all(math.isfinite(float(v)) for v in b):
        raise ValueError('INVALID_CANDLE')
    t, o, h, l, c, v = b
    if int(t) != t or not 0 < l <= min(o, c) <= max(o, c) <= h or v < 0:
        raise ValueError('INVALID_OHLCV')


class PublicData:
    def __init__(self, directory, base):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.base = base.rstrip('/')
        self.db = sqlite3.connect(self.directory / 'public_bybit_cache.sqlite3')
        self.db.execute('CREATE TABLE IF NOT EXISTS bars '
                        '(kind TEXT,minutes INTEGER,t INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,'
                        'PRIMARY KEY(kind,minutes,t))')
        self.db.execute('CREATE TABLE IF NOT EXISTS funding(t INTEGER PRIMARY KEY,rate REAL)')
        self.db.execute('CREATE TABLE IF NOT EXISTS coverage(key TEXT PRIMARY KEY)')

    def api(self, endpoint, **params):
        url = self.base + endpoint + '?' + urllib.parse.urlencode(params)
        for attempt in range(5):
            try:
                request = urllib.request.Request(url, headers={'User-Agent': VERSION})
                with urllib.request.urlopen(request, timeout=30) as response:
                    data = json.load(response)
                if data.get('retCode') != 0:
                    raise RuntimeError('BYBIT: ' + str(data.get('retMsg')))
                return data['result']
            except (OSError, ValueError, RuntimeError) as exc:
                if attempt == 4:
                    raise RuntimeError('PUBLIC_DATA_FAILED: ' + str(exc)) from exc
                time.sleep(min(2 ** attempt, 8))
        raise RuntimeError('UNREACHABLE')

    def server_time(self):
        return int(self.api('/v5/market/time')['timeSecond'])

    def instrument(self):
        rows = self.api('/v5/market/instruments-info', category='linear',
                        symbol='BTCUSDT')['list']
        row = next(r for r in rows if r['symbol'] == 'BTCUSDT')
        return float(row['priceFilter']['tickSize'])

    def bars(self, kind, minutes, start, end):
        seconds = minutes * 60
        start = start // seconds * seconds
        end = end // seconds * seconds
        endpoint = '/v5/market/kline' if kind == 'trade' else '/v5/market/mark-price-kline'
        # Fixed chunks make interrupted downloads resumable. Never cache an open candle.
        for left in range(start, end, 1000 * seconds):
            right = min(end, left + 1000 * seconds)
            expected = (right - left) // seconds
            count = self.db.execute('SELECT COUNT(*) FROM bars WHERE kind=? AND minutes=? '
                                    'AND t>=? AND t<?', (kind, minutes, left, right)).fetchone()[0]
            if count != expected:
                rows = self.api(endpoint, category='linear', symbol='BTCUSDT',
                                interval=str(minutes), start=left * 1000,
                                end=right * 1000 - 1, limit=1000)['list']
                values = {}
                for r in rows:
                    t = int(r[0]) // 1000
                    if left <= t < right and t % seconds == 0:
                        b = [t] + list(map(float, r[1:5])) + [float(r[5]) if kind == 'trade' else 0.]
                        validate_bar(b)
                        values[t] = b
                if sorted(values) != list(range(left, right, seconds)):
                    raise RuntimeError('INCOMPLETE_HISTORY: %s %sm %s' % (kind, minutes, stamp(left)))
                with self.db:
                    self.db.executemany('INSERT OR REPLACE INTO bars VALUES(?,?,?,?,?,?,?,?)',
                                        [(kind, minutes, *b) for b in values.values()])
                print('HISTORY_PROGRESS', kind, minutes, stamp(right), flush=True)
                time.sleep(.12)
        bars = [list(r) for r in self.db.execute(
            'SELECT t,o,h,l,c,v FROM bars WHERE kind=? AND minutes=? AND t>=? AND t<? ORDER BY t',
            (kind, minutes, start, end))]
        if len(bars) != (end - start) // seconds or any(
                b[0] != start + i * seconds for i, b in enumerate(bars)):
            raise RuntimeError('CACHED_HISTORY_GAP')
        return bars

    def funding(self, start, end):
        # Pagination by actual timestamps; no assumed 8-hour settlement interval.
        cursor = end * 1000 - 1
        key = 'funding:%d:%d' % (start, end)
        if not self.db.execute('SELECT 1 FROM coverage WHERE key=?', (key,)).fetchone():
            while cursor >= start * 1000:
                rows = self.api('/v5/market/funding/history', category='linear', symbol='BTCUSDT',
                                startTime=start * 1000, endTime=cursor, limit=200)['list']
                if not rows:
                    break
                earliest = min(int(r['fundingRateTimestamp']) for r in rows)
                if earliest > cursor:
                    raise RuntimeError('FUNDING_PAGINATION_STALLED')
                with self.db:
                    self.db.executemany('INSERT OR REPLACE INTO funding VALUES(?,?)',
                        [(int(r['fundingRateTimestamp']) // 1000, float(r['fundingRate']))
                         for r in rows if start * 1000 <= int(r['fundingRateTimestamp']) < end * 1000])
                cursor = earliest - 1
                time.sleep(.12)
            with self.db:
                self.db.execute('INSERT OR IGNORE INTO coverage VALUES(?)', (key,))
        result = list(self.db.execute('SELECT t,rate FROM funding WHERE t>=? AND t<? ORDER BY t',
                                      (start, end)))
        # Reject suspicious missing periods rather than treat them as zero funding.
        times = [start] + [r[0] for r in result] + [end]
        if not result or max(b-a for a, b in zip(times, times[1:])) > 12 * 3600:
            raise RuntimeError('FUNDING_COVERAGE_NOT_VERIFIED')
        return result


def aggregate(bars, seconds, source_seconds=900):
    groups = collections.defaultdict(list)
    for b in bars:
        groups[int(b[0]) // seconds * seconds].append(b)
    output = []
    for start, group in sorted(groups.items()):
        if [b[0] for b in group] != list(range(start, start + seconds, source_seconds)):
            continue
        output.append([start, group[0][1], max(b[2] for b in group),
                       min(b[3] for b in group), group[-1][4], sum(b[5] for b in group)])
    return output


class Structure:
    def __init__(self, seconds):
        self.seconds = seconds
        self.bars = []
        self.highs = []
        self.lows = []
        self.atr = None
        self.trs = []
        self.direction = None
        self.invalidated = set()

    def step(self, b):
        validate_bar(b)
        if self.bars and b[0] != self.bars[-1][0] + self.seconds:
            raise ValueError('STRUCTURE_HISTORY_GAP')
        prior = self.bars[-1][4] if self.bars else b[1]
        tr = max(b[2]-b[3], abs(b[2]-prior), abs(b[3]-prior))
        self.trs.append(tr)
        if self.atr is None and len(self.trs) >= 14:
            self.atr = sum(self.trs[-14:]) / 14
        elif self.atr is not None:
            self.atr = (self.atr * 13 + tr) / 14
        self.bars.append(b)
        if len(self.bars) >= 5:
            window = self.bars[-5:]
            pivot = window[2]
            known = int(b[0]) + self.seconds
            others = window[:2] + window[3:]
            if all(pivot[2] > x[2] for x in others):
                self.highs.append(dict(price=pivot[2], open=int(pivot[0]), known=known))
            if all(pivot[3] < x[3] for x in others):
                self.lows.append(dict(price=pivot[3], open=int(pivot[0]), known=known))
        direction = None
        if len(self.highs) >= 2 and len(self.lows) >= 2:
            h0, h1 = self.highs[-2:]
            l0, l1 = self.lows[-2:]
            if h1['price'] > h0['price'] and l1['price'] > l0['price']:
                identity = ('LONG', l1['open'])
                if b[4] < l1['price']:
                    self.invalidated.add(identity)
                direction = 'LONG' if identity not in self.invalidated else None
            elif h1['price'] < h0['price'] and l1['price'] < l0['price']:
                identity = ('SHORT', h1['open'])
                if b[4] > h1['price']:
                    self.invalidated.add(identity)
                direction = 'SHORT' if identity not in self.invalidated else None
        self.direction = direction
        # Keep sufficient rolling candles; pivot lists retain historical level candidates.
        self.bars = self.bars[-200:]
        self.trs = self.trs[-14:]
        self.highs = self.highs[-500:]
        self.lows = self.lows[-500:]


class Strategy:
    def __init__(self, high_seconds, low_seconds, tick):
        self.high = Structure(high_seconds)
        self.low = Structure(low_seconds)
        self.tick = tick
        self.setup = None
        self.used = set()
        self.reasons = collections.Counter()

    def on_low(self, b, allowed):
        # Freeze all signal inputs before adding the current candle to pivot/ATR history.
        at = int(b[0]) + self.low.seconds
        direction = self.high.direction
        a = self.low.atr
        previous = self.low.bars[-1] if self.low.bars else None
        intent = None
        if not allowed:
            self.setup = None
        elif self.setup:
            s = self.setup
            sign = 1 if s['side'] == 'LONG' else -1
            age = (at - s['at']) // self.low.seconds
            if direction != s['side'] or age > POLICY['retest_bars']:
                self.reasons['SETUP_EXPIRED_OR_TREND_CHANGED'] += 1
                self.setup = None
            elif sign * (b[4] - s['level']) < -s['band']:
                self.reasons['BREAKOUT_FAILED'] += 1
                self.setup = None
            else:
                touched = b[3] <= s['level'] + s['band'] if sign == 1 else b[2] >= s['level'] - s['band']
                if touched:
                    s['touched'] = True
                if s['touched']:
                    s['extreme'] = min(s['extreme'], b[3]) if sign == 1 else max(s['extreme'], b[2])
                if s['touched'] and sign*(b[4]-s['level']) > 0 and sign*(b[4]-b[1]) > 0:
                    stop = s['extreme'] - sign * POLICY['stop_buffer_atr'] * s['atr']
                    stop = tick_round(stop, self.tick, 'down' if sign == 1 else 'up')
                    levels = [p['price'] for engine in (self.low, self.high)
                              for p in (engine.highs if sign == 1 else engine.lows)
                              if p['known'] <= at and sign*(p['price']-b[4]) > 0]
                    # A pivot confirmed at this very close is also available at decision time.
                    intent = dict(side=s['side'], decision=at, reference=b[4], sl=stop,
                                  atr=s['atr'], breakout_level=s['level'], breakout_at=s['at'],
                                  obstacle=min(levels) if sign == 1 and levels else
                                  max(levels) if levels else None)
                    self.setup = None
        elif allowed and direction and a and previous and len(self.low.bars) >= 20:
            sign = 1 if direction == 'LONG' else -1
            pivots = self.low.highs if sign == 1 else self.low.lows
            if pivots:
                p = pivots[-1]
                identity = (direction, p['open'])
                level = p['price']
                prior20 = self.low.bars[-20:]
                top = max(x[2] for x in prior20)
                bottom = min(x[3] for x in prior20)
                compact = top-bottom <= POLICY['range_width_atr'] * a
                if compact:
                    level = max(level, top) if sign == 1 else min(level, bottom)
                crossed = sign*(previous[4]-level) <= 0 and sign*(b[4]-level) >= POLICY['breakout_atr']*a
                if identity not in self.used and p['known'] <= b[0] and crossed:
                    self.used.add(identity)
                    self.setup = dict(side=direction, at=at, level=level, atr=a,
                                      band=POLICY['retest_atr']*a, touched=False,
                                      extreme=float('inf') if sign == 1 else -float('inf'))
                    self.reasons['BREAKOUT_SETUP'] += 1
        self.low.step(b)
        if intent is not None:
            sign = 1 if intent['side'] == 'LONG' else -1
            levels = [p['price'] for engine in (self.low, self.high)
                      for p in (engine.highs if sign == 1 else engine.lows)
                      if p['known'] <= at and sign*(p['price']-b[4]) > 0]
            intent['obstacle'] = (min(levels) if sign == 1 else max(levels)) if levels else None
        return intent


def entry_plan(intent, raw_open, tick, slippage_bps):
    sign = 1 if intent['side'] == 'LONG' else -1
    reference_risk = sign * (intent['reference'] - intent['sl'])
    if reference_risk <= 0:
        return None, 'INVALID_REFERENCE_RISK'
    entry = tick_round(raw_open*(1+sign*slippage_bps/10000), tick, 'up' if sign == 1 else 'down')
    if abs(entry-intent['reference']) > POLICY['max_chase_R']*reference_risk:
        return None, 'ENTRY_MOVED_OVER_0.1R'
    risk = sign*(entry-intent['sl'])
    if not POLICY['min_stop_atr']*intent['atr'] <= risk <= POLICY['max_stop_atr']*intent['atr']:
        return None, 'STOP_ATR_DISTANCE'
    target = tick_round(entry+sign*POLICY['target_R']*risk, tick, 'up' if sign == 1 else 'down')
    obstacle = intent['obstacle']
    if obstacle is not None and sign*(obstacle-entry) <= sign*(target-entry):
        return None, 'OBSTACLE_BEFORE_TARGET'
    return dict(intent, entry=entry, target=target, risk=risk), None


def exit_touch(p, b):
    sign = 1 if p['side'] == 'LONG' else -1
    if sign*(b[1]-p['sl']) <= 0:
        return 'SL_GAP', b[1]
    if sign*(b[1]-p['target']) >= 0:
        return 'TARGET', p['target']
    sl = b[3] <= p['sl'] if sign == 1 else b[2] >= p['sl']
    tp = b[2] >= p['target'] if sign == 1 else b[3] <= p['target']
    if sl:
        return ('AMBIGUOUS_STOP_FIRST' if tp else 'SL'), p['sl']
    if tp:
        return 'TARGET', p['target']
    return None


def simulate(one, low_bars, high_bars, funding, mark_open, start, end,
             high_seconds, low_seconds, tick, initial, fee, slip, risk_pct, cap):
    engine = Strategy(high_seconds, low_seconds, tick)
    low_map = {int(b[0])+low_seconds: b for b in low_bars}
    high_map = {int(b[0])+high_seconds: b for b in high_bars}
    fund_map = dict(funding)
    balance = initial
    peak = initial
    max_dd = 0.
    pos = None
    pending = None
    trades = []
    rejected = collections.Counter()
    curve = []
    first = int(one[0][0])
    # Warm history chronologically without generating entries or consuming breakouts.
    for at in sorted(set(t for t in high_map if t <= first) | set(t for t in low_map if t <= first)):
        if at in high_map:
            engine.high.step(high_map[at])
        if at in low_map:
            engine.low.step(low_map[at])
    for b in one:
        t = int(b[0])
        close_at = t+60
        if t >= end:
            break
        if t in fund_map and pos is not None and pos['opened'] < t:
            if t not in mark_open:
                raise RuntimeError('MISSING_SETTLEMENT_MARK')
            payment = -(1 if pos['side'] == 'LONG' else -1) * pos['qty'] * mark_open[t] * fund_map[t]
            balance += payment
            pos['funding_pnl'] += payment
        if pending is not None:
            intent = pending
            pending = None
            if start <= t < end and pos is None:
                plan, reason = entry_plan(intent, b[1], tick, slip)
                if reason:
                    rejected[reason] += 1
                elif balance <= 0:
                    rejected['NO_CAPITAL'] += 1
                else:
                    qty = min(balance*risk_pct/plan['risk'], balance*cap/plan['entry'])
                    entry_fee = qty*plan['entry']*fee
                    balance -= entry_fee
                    pos = dict(plan, opened=t, qty=qty, fees=entry_fee,
                               funding_pnl=0., intended_risk_usdt=qty*plan['risk'],
                               capital_capped=qty*plan['risk'] < (balance+entry_fee)*risk_pct-1e-8)
        if pos is not None:
            touched = exit_touch(pos, b)
            if touched:
                status, raw_exit = touched
                sign = 1 if pos['side'] == 'LONG' else -1
                price = tick_round(raw_exit*(1-sign*slip/10000), tick, 'down' if sign == 1 else 'up')
                gross = sign*pos['qty']*(price-pos['entry'])
                exit_fee = pos['qty']*price*fee
                balance += gross-exit_fee
                pos.update(status=status, closed_candle_open=t, closed_by=close_at,
                           exit=price, gross_pnl=gross, fees=pos['fees']+exit_fee)
                pos['net_pnl'] = gross-pos['fees']+pos['funding_pnl']
                pos['net_R'] = pos['net_pnl']/pos['intended_risk_usdt']
                trades.append(pos)
                pos = None
        # At simultaneous closes, high timeframe is available before the low decision.
        if close_at in high_map:
            engine.high.step(high_map[close_at])
        if close_at in low_map:
            allowed = start <= close_at < end and pos is None and pending is None
            pending = engine.on_low(low_map[close_at], allowed)
        if t >= start:
            unrealized = 0. if pos is None else (1 if pos['side']=='LONG' else -1)*pos['qty']*(b[4]-pos['entry'])
            equity = balance+unrealized
            peak = max(peak, equity)
            max_dd = max(max_dd, (peak-equity)/peak if peak > 0 else 0)
            if close_at % 86400 == 0 or close_at == end:
                curve.append(dict(at=close_at, equity=equity))
    gross = sum(p['gross_pnl'] for p in trades)
    fees = sum(p['fees'] for p in trades)
    fund = sum(p['funding_pnl'] for p in trades)
    profits = sum(max(p['net_pnl'], 0.) for p in trades)
    losses = -sum(min(p['net_pnl'], 0.) for p in trades)
    floating = 0. if pos is None else (1 if pos['side']=='LONG' else -1)*pos['qty']*(one[-1][4]-pos['entry'])
    summary = dict(trades=len(trades)+(pos is not None), closed=len(trades), open=int(pos is not None),
                   outcomes=dict(collections.Counter(p['status'] for p in trades)),
                   closed_gross_pnl=gross, closed_fees=fees, closed_funding_pnl=fund,
                   closed_net_pnl=gross-fees+fund,
                   profit_factor=profits/losses if losses else None,
                   net_winners=sum(p['net_pnl'] > 0 for p in trades),
                   ending_cash=balance, ending_equity=balance+floating,
                   total_return_pct=(balance+floating-initial)/initial*100,
                   max_drawdown_minute_close_pct=max_dd*100,
                   open_price_pnl=floating, rejects=dict(rejected), setups=dict(engine.reasons))
    return dict(summary=summary, trades=trades, open_position=pos, equity_daily=curve)


def self_test():
    assert tick_round(.20495, .0001) == .205
    assert tick_round(100.01, .1, 'up') == 100.1
    bars = [[i*60, 10., 12., 8., 10., 1.] for i in range(5)]
    bars[2][2] = 15.
    s = Structure(60)
    for b in bars[:4]:
        s.step(b)
    assert not s.highs
    s.step(bars[4])
    assert s.highs[-1]['known'] == 300
    q = Structure(60)
    for b in bars:
        q.step(b)
    assert q.highs == s.highs
    p = dict(side='LONG', sl=90, target=120)
    assert exit_touch(p, [0,100,121,89,100,0])[0] == 'AMBIGUOUS_STOP_FIRST'
    assert exit_touch(p, [0,85,95,80,90,0]) == ('SL_GAP',85)
    assert exit_touch(dict(side='SHORT',sl=110,target=80), [0,100,111,79,100,0])[0] == 'AMBIGUOUS_STOP_FIRST'
    intent = dict(side='LONG',reference=100,sl=90,atr=10,obstacle=130)
    plan, reason = entry_plan(intent,100,.1,0)
    assert not reason and plan['target'] == 120
    assert entry_plan(intent,102,.1,0)[1] == 'ENTRY_MOVED_OVER_0.1R'
    assert entry_plan(dict(intent,obstacle=119),100,.1,0)[1] == 'OBSTACLE_BEFORE_TARGET'
    assert entry_plan(dict(intent,side='SHORT',sl=110,obstacle=70),100,.1,0)[0]['target'] == 80
    ag = aggregate([[i*900,10,12,8,11,1] for i in range(8)],3600)
    assert len(ag)==2 and ag[0][5]==4
    # Confirm retest only after breakout, frozen ATR, direction change invalidation.
    st = Strategy(240,60,.1)
    st.high.direction='LONG'
    st.low.atr=10.
    st.low.bars=[[i*60,99,101,95,99,1] for i in range(20)]
    st.low.highs=[dict(price=101,open=600,known=900)]
    assert st.on_low([1200,99,104,98,103,1],True) is None
    assert st.setup and st.setup['at']==1260
    out = st.on_low([1260,102,104,101,104,1],True)
    assert out and out['decision']==1320 and out['sl']==99
    assert st.setup is None
    # Prefix invariance: appending a future candle cannot change past decisions.
    def run(seq):
        x=Structure(60); result=[]
        for b in seq:
            x.step(b); result.append((x.direction,list(x.highs),list(x.lows)))
        return result
    assert run(bars+[ [300,10,20,7,12,1] ])[:5] == run(bars)
    print('SELF_TEST_OK: closed pivots, prefix causality, UTC bars, retest timing, '
          'both directions, chase, obstacle, tick rounding, gaps, ambiguous stop-first')


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--self-test',action='store_true')
    ap.add_argument('--start', help='Timezone-aware inclusive date')
    ap.add_argument('--end', help='Timezone-aware exclusive date; defaults to last closed UTC hour')
    ap.add_argument('--days',type=int,default=365)
    ap.add_argument('--holdout-days',type=int,default=90)
    ap.add_argument('--capital',type=float,default=10000.)
    ap.add_argument('--risk-pct',type=float,default=.5)
    ap.add_argument('--max-notional-equity',type=float,default=1.)
    ap.add_argument('--fee-bps',type=float,default=5.5)
    ap.add_argument('--slippage-bps',type=float,default=1.)
    ap.add_argument('--data-dir',default='/opt/btc_swing_v1_research')
    ap.add_argument('--base-url',default='https://api.bybit.com')
    args=ap.parse_args()
    if args.self_test:
        self_test(); return
    if not (args.days>0 and args.holdout_days>0 and args.capital>0 and
            0<args.risk_pct<=2 and 0<args.max_notional_equity<=1 and
            args.fee_bps>=0 and args.slippage_bps>=0):
        ap.error('Invalid risk, cost or window arguments; notional cap is at most 1x equity')
    store=PublicData(args.data_dir,args.base_url)
    server=store.server_time()
    end=epoch(args.end) if args.end else server//3600*3600
    start=epoch(args.start) if args.start else end-args.days*86400
    if end>server//60*60 or end%3600 or start%3600:
        ap.error('Use closed, UTC-hour-aligned timestamps (Iran times end in :30)')
    split=end-args.holdout_days*86400
    if not start<split<end:
        ap.error('Holdout must be shorter than full window')
    tick=store.instrument()
    manifest=dict(version=VERSION,policy=POLICY,start=start,end=end,split=split,
                  start_iran=stamp(start),end_iran=stamp(end),holdout_start_iran=stamp(split),
                  tick=tick,fee_bps_assumed=args.fee_bps,slippage_bps_assumed=args.slippage_bps,
                  script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  risk_pct=args.risk_pct,initial_capital=args.capital,
                  max_notional_equity=args.max_notional_equity,
                  funding='Historical rates times settlement-minute mark OPEN; event applied before gap exits',
                  same_minute_ambiguity='STOP_FIRST_CONSERVATIVE_BOUND',
                  split_books='Independent flat starts; structures warmed; no forced terminal exit',
                  quantity='Continuous BTC approximation; historical lot limits not reconstructed',
                  limitations=['No actual historical quotes or latency',
                               'No funding API missing-data proof beyond gap checks',
                               'Current tick assumed constant historically',
                               'Minute-close drawdown; no exchange liquidation model; <=1x notional cap',
                               'Historical public data may be revised; data hashes recorded'])
    print('RUN_MANIFEST',json.dumps(manifest,ensure_ascii=False),flush=True)
    fifteen=store.bars('trade',15,start-90*86400,end)
    one=store.bars('trade',1,start,end)
    funding=store.funding(start,end)
    marks=store.bars('mark',60,start,end)
    mark_open={int(b[0]):b[1] for b in marks}
    # Do not silently approximate a non-hourly settlement with the wrong hour's mark.
    for t,_ in funding:
        if t not in mark_open:
            mark_open[t]=store.bars('mark',1,t,t+60)[0][1]
    for label,bars in [('trade15',fifteen),('trade1',one),('mark60',marks),('funding',funding)]:
        manifest[label+'_sha256']=hashlib.sha256(json.dumps(bars,separators=(',',':')).encode()).hexdigest()
    run=Path(args.data_dir)/('run_'+dt.datetime.now(UTC).strftime('%Y%m%d_%H%M%S_%f'))
    run.mkdir()
    (run/'manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    results={}
    for name,hs,ls in [('A_4H_1H',14400,3600),('B_1H_15M',3600,900)]:
        lower=aggregate(fifteen,ls)
        higher=aggregate(fifteen,hs)
        results[name]={}
        for segment,left,right in [('DEVELOPMENT',start,split),('HOLDOUT',split,end)]:
            # Same capital for independent windows; no post-hoc tuning in this program.
            lo=bisect.bisect_left([b[0] for b in one],left)
            hi=bisect.bisect_left([b[0] for b in one],right)
            result=simulate(one[lo:hi],lower,higher,funding,mark_open,left,right,
                            hs,ls,tick,args.capital,args.fee_bps/10000,
                            args.slippage_bps,args.risk_pct/100,args.max_notional_equity)
            results[name][segment]=result
            print('RESULT',name,segment,json.dumps(result['summary']),flush=True)
            trades=result['trades']+([dict(result['open_position'],status='OPEN')] if result['open_position'] else [])
            trades=[dict(p, entry_time_iran=stamp(p['opened']),
                         decision_time_iran=stamp(p['decision']),
                         exit_candle_open_iran=stamp(p['closed_candle_open']) if p.get('closed_candle_open') is not None else None)
                    for p in trades]
            if trades:
                keys=sorted(set(k for p in trades for k in p))
                with (run/(name+'_'+segment+'_trades.csv')).open('w',newline='',encoding='utf-8-sig') as f:
                    writer=csv.DictWriter(f,fieldnames=keys);writer.writeheader();writer.writerows(trades)
    (run/'results.json').write_text(json.dumps(results,indent=2,allow_nan=False),encoding='utf-8')
    report=['# مقایسه پژوهشی BTC Swing V1', '',
            'این گزارش شبیه‌سازی است؛ قواعد ربات فعال تغییر نکرده است.', '',
            'ورود پس از تأیید کندل، استاپ ثابت، هدف ۲R و فقط یک موقعیت باز برای هر نسخه.',
            'بازه ارزیابی پایانی جدا و با حساب اولیه مستقل محاسبه شده است.', '',
            '| نسخه | بخش | بسته | باز | هدف | استاپ یا مبهم محافظه‌کارانه | خالص بسته‌شده | بازده کل با شناور | افت سرمایه |',
            '|---|---|---:|---:|---:|---:|---:|---:|---:|']
    for name,segments in results.items():
        for segment,result in segments.items():
            s=result['summary']; outcomes=s['outcomes']
            stopped=sum(n for k,n in outcomes.items() if k!='TARGET')
            report.append('| %s | %s | %d | %d | %d | %d | %.2f USDT | %.2f%% | %.2f%% |' %
                (name,segment,s['closed'],s['open'],outcomes.get('TARGET',0),stopped,
                 s['closed_net_pnl'],s['total_return_pct'],s['max_drawdown_minute_close_pct']))
    report += ['', 'کارمزد و لغزش فرضی‌اند؛ نرخ‌های فاندینگ تاریخی و قیمت آغاز دقیقه تسویه از مارک استفاده شده است.',
               'افت سرمایه بر اساس ارزش حساب در پایان دقیقه است. استاپ و هدف در یک دقیقه: استاپ مقدم و دارای برچسب جدا.',
               'اندازه موقعیت پیوسته و تقریبی است؛ سقف حجم برابر سرمایه حساب است. حداقل مقدار سفارش تاریخی بازسازی نشده است.',
               'معامله باز در انتهای بازه به اجبار بسته نشده؛ جزئیات در results.json و فایل‌های CSV قرار دارد.']
    (run/'report_FA.md').write_text('\n'.join(report)+'\n',encoding='utf-8')
    print('REPORT_DIRECTORY',str(run))
    print('RESEARCH_COMPLETE; NO BOT, SERVICE, TELEGRAM OR ORDER CHANGES')


if __name__=='__main__':
    main()
