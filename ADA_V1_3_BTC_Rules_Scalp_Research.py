#!/usr/bin/env python3
# coding: utf-8
"""ADAUSDT independent paper monitor. Public data; never places exchange orders.

BTC-equivalent accepted-cross confirmation on 1m/3m/5m; 5m scalp context.
BTC V4 entry rules and fixed-stop/full-position exits, with ADA tick precision.
New signals may overlap. Legacy V1.2 positions retain their original management.
TP1/TP2 are milestones; full exit at initial SL or TP3, no timed exit.
Public REST ticker lastPrice is the entry reference; orderflow is informational.
Run --self-test first, --check 120 next, --watch --telegram only after review.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING, ROUND_HALF_EVEN
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import signal
import sqlite3
import statistics
import threading
import time
import urllib.parse
import urllib.request

VERSION = 'ADA-V1.3-BTC-RULES'
SYMBOL = 'ADAUSDT'
API = 'https://api.bybit.com'
WS = 'wss://stream.bybit.com/v5/public/linear'
IRAN = dt.timezone(dt.timedelta(hours=3, minutes=30))
TF = {'1m': 60, '3m': 180, '5m': 300, '15m': 900}
FRAMES = {'3m': 180, '5m': 300, '15m': 900}
# Extra thresholds below apply ONLY to restored pre-V1.3 legacy positions/tests.
# V1.3 entry_plan uses BTC rules explicitly; no orderflow/ATR/spread veto.
POLICY = dict(score_min=4, confirm_seconds=600, decision_age=30,
              chase_pct=0.10, risk_atr_min=0.8, risk_atr_max=3.0,
              spread_bps_max=8, flow_imbalance_min=0.10, flow_trades_min=20,
              fee_bps=6, slippage_bps=2, max_hold_minutes=45)


def stamp(t):
    return dt.datetime.fromtimestamp(t, IRAN).isoformat(timespec='seconds')


def get(path, **params):
    url = API + path + '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={'User-Agent': VERSION})
    with urllib.request.urlopen(req, timeout=8) as response:
        answer = json.load(response)
    if answer.get('retCode') != 0:
        raise RuntimeError('BYBIT_' + str(answer.get('retCode')))
    server = float(answer.get('time', 0)) / 1000
    if server and abs(server - time.time()) > 10:
        raise RuntimeError('CLOCK_DRIFT')
    return answer['result']


def candles(tf, end, count=300):
    seconds = TF[tf]
    result = get('/v5/market/kline', category='linear', symbol=SYMBOL,
                 interval=str(seconds // 60), end=int(end * 1000) - 1,
                 limit=min(1000, count + 3))
    bars = sorted([[int(r[0]) // 1000] + [float(v) for v in r[1:6]]
                   for r in result['list'] if int(r[0]) // 1000 + seconds <= end])[-count:]
    validate(bars, seconds)
    if len(bars) != count or bars[-1][0] + seconds != int(end) // seconds * seconds:
        raise ValueError('INCOMPLETE_' + tf)
    return bars


def validate(bars, seconds):
    for i, b in enumerate(bars):
        if (len(b) != 6 or not all(math.isfinite(v) for v in b)
                or b[0] % seconds or b[3] <= 0 or b[5] < 0
                or b[3] > min(b[1], b[4]) or b[2] < max(b[1], b[4])
                or (i and b[0] - bars[i-1][0] != seconds)):
            raise ValueError('INVALID_CANDLES')


def ema(values, length):
    out, previous = [], None
    for value in values:
        if value is None:
            out.append(None)
            continue
        previous = value if previous is None else 2 / (length + 1) * value + (1 - 2 / (length + 1)) * previous
        out.append(previous)
    return out


def rsi_series(values, length):
    out = [None] * len(values)
    if len(values) <= length:
        return out
    changes = [values[i] - values[i-1] for i in range(1, len(values))]
    gain = sum(max(x, 0) for x in changes[:length]) / length
    loss = sum(max(-x, 0) for x in changes[:length]) / length
    def value():
        return 50.0 if gain == loss == 0 else 100.0 if loss == 0 else 100 - 100 / (1 + gain / loss)
    out[length] = value()
    for i in range(length, len(changes)):
        gain = (gain * (length - 1) + max(changes[i], 0)) / length
        loss = (loss * (length - 1) + max(-changes[i], 0)) / length
        out[i+1] = value()
    return out


def atr(bars, length=14):
    tr = [max(b[2]-b[3], abs(b[2]-bars[i-1][4]), abs(b[3]-bars[i-1][4]))
          for i, b in enumerate(bars) if i]
    value = sum(tr[:length]) / length
    for x in tr[length:]:
        value = (value * (length-1) + x) / length
    return value


def seeded_ema(values, length):
    value = sum(values[:length]) / length
    for x in values[length:]:
        value += (x - value) * 2 / (length + 1)
    return value


def crosses(bars, tf):
    source = [sum(b[1:5]) / 4 for b in bars]
    line, wave = ema(rsi_series(source, 2), 2), ema(rsi_series(source, 4), 4)
    events = []
    for i in range(1, len(bars)):
        if None in (line[i-1], wave[i-1], line[i], wave[i]):
            continue
        before, after = wave[i-1]-line[i-1], wave[i]-line[i]
        side = 'LONG' if before >= 0 and after < 0 else 'SHORT' if before <= 0 and after > 0 else None
        if side:
            events.append(dict(at=bars[i][0]+TF[tf], tf=tf, side=side))
    return events


CROSS_TIMEFRAMES = ('1m', '3m', '5m')
CROSS_MAX_ARRIVAL_DELAY = 15

def closed_cross_events(frames, after, through):
    events = [e for tf in CROSS_TIMEFRAMES for e in crosses(frames[tf], tf)
              if after < e['at'] <= through]
    return sorted(events, key=lambda e: (e['at'], TF[e['tf']]))


class BTCCrossGate:
    """Persistent accepted-cross sequence matching BTC TrendEngine confirmation.

    Ties are processed in deterministic 1m,3m,5m order. Accepted crosses are
    consumed in non-overlapping pairs. Opposite accepted crosses break pairs.
    """
    def __init__(self, saved=None):
        self.ident = 0
        self.previous = None
        self.cutoff = 0
        self.latest = {}
        self.last_close = None
        self.cursor = None
        self.epoch = 0
        if saved:
            self.__dict__.update(saved)

    def dump(self):
        return dict(self.__dict__)

    def process(self, event, received_at):
        at, tf, side = event['at'], event['tf'], event['side']
        if tf not in CROSS_TIMEFRAMES or side not in ('LONG','SHORT'):
            return None, 'INVALID_CROSS'
        if not 0 <= received_at-at <= CROSS_MAX_ARRIVAL_DELAY:
            return None, 'LATE_CROSS'
        if self.last_close is not None and at < self.last_close:
            return None, 'OUT_OF_ORDER'
        if tf in self.latest and at <= self.latest[tf]:
            return None, 'DUPLICATE'
        previous = self.previous
        self.ident += 1
        self.latest[tf] = at
        self.last_close = at
        self.previous = dict(event, id=self.ident)
        if (previous and previous['side']==side and previous['id']>self.cutoff
                and 0 <= at-previous['at'] <= POLICY['confirm_seconds']):
            self.cutoff = self.ident
            self.epoch += 1
            return dict(at=at, side=side, epoch=self.epoch,
                        first=dict(previous), second=dict(self.previous)), 'CONFIRMED'
        return None, 'WAITING_PAIR'

    def advance(self, frames, through, received_at):
        if self.cursor is None:
            # Reconstruct sequence only: never issue a historical entry on bootstrap.
            for event in closed_cross_events(frames, through-3600, through):
                self.process(event, event['at']+1)
            self.cursor = through
            return [], dict(bootstrap=True, accepted_history_only=True)
        if through <= self.cursor:
            return [], dict(bootstrap=False, no_new_minute=True)
        confirmed, counts = [], {}
        for event in closed_cross_events(frames, self.cursor, through):
            result, reason = self.process(event, received_at)
            counts[reason] = counts.get(reason,0)+1
            if result:
                confirmed.append(result)
        self.cursor = through
        return confirmed, dict(bootstrap=False, event_results=counts)


def confirmation(frames, decision):
    """Offline diagnostic only; live entries use the persistent BTCCrossGate."""
    gate = BTCCrossGate()
    side = None
    for event in closed_cross_events(frames, decision-3600, decision):
        result, _ = gate.process(event, event['at']+1)
        if result and result['at']==decision:
            side = result['side']
    return side


def fetch_closed_frames(cache, cutoff):
    needed = [tf for tf in CROSS_TIMEFRAMES if tf not in cache or not cache[tf]
              or cache[tf][-1][0]+TF[tf] != int(cutoff)//TF[tf]*TF[tf]]
    if needed:
        with ThreadPoolExecutor(max_workers=4) as executor:
            jobs = {tf:executor.submit(candles,tf,cutoff,300) for tf in needed}
            updated = {tf:job.result() for tf,job in jobs.items()}
        cache.update(updated)
    return dict(cache)


def evaluate(side, frames):
    one, three, five = frames['1m'][-240:], frames['3m'][-80:], frames['5m'][-100:]
    r1, r3, r5 = [rsi_series([b[4] for b in x], 14)[-1] for x in (one, three, five)]
    closes = [b[4] for b in five]
    e20, e50 = seeded_ema(closes, 20), seeded_ema(closes, 50)
    aligned = e20 > e50 and closes[-1] > e20 if side == 'LONG' else e20 < e50 and closes[-1] < e20
    rsi_ok = 45 <= r3 <= 70 and r5 >= 45 if side == 'LONG' else 30 <= r3 <= 55 and r5 <= 55
    vol = sum(b[5] for b in one[-30:-1]) / 29
    ratio = one[-1][5] / vol if vol > 0 else 0
    b = one[-1]
    body = abs(b[4]-b[1]) / (b[2]-b[3]) if b[2] > b[3] else 0
    candle_ok = (b[4] > b[1] if side == 'LONG' else b[4] < b[1]) and body >= .35
    score = 2*int(aligned) + 2*int(rsi_ok) + int(ratio >= 1) + int(candle_ok)
    # BTC's B3, retained explicitly as an uncalibrated ADA research threshold.
    peak, drop = five[-48][2], 0
    for b in five[-47:]:
        drop = max(drop, (peak-b[3])/peak)
        peak = max(peak, b[2])
    lows = [five[i][3] for i in range(len(five)-46, len(five)-2)
            if all(five[i][3] < five[j][3] for j in (i-2, i-1, i+1, i+2))]
    b3 = side == 'SHORT' and drop >= .005 and len(lows) >= 2 and lows[-1] > lows[-2]
    chase_rsi = r1 > 75 if side == 'LONG' else r1 < 25
    return dict(score=score, ema_aligned=aligned, rsi_aligned=rsi_ok,
                rsi_1m=r1, rsi_3m=r3, rsi_5m=r5, volume_ratio=ratio,
                b3_reject=b3, chase_rsi=chase_rsi, atr=atr(five))


def extrema(values, low):
    result, i = [], 1
    while i < len(values)-1:
        start, value = i, values[i]
        while i+1 < len(values) and values[i+1] == value:
            i += 1
        if i < len(values)-1 and ((value < values[start-1] and value < values[i+1]) if low else
                                  (value > values[start-1] and value > values[i+1])):
            result.append(value)
        i += 1
    return result


def rounded(value, tick, up=False):
    quantum = Decimal(str(tick))
    return float((Decimal(str(value)) / quantum).to_integral_value(
        rounding=ROUND_CEILING if up else ROUND_FLOOR) * quantum)


def structural_stop(side, five, entry, tick):
    low = side == 'LONG'
    values = [b[3] if low else b[2] for b in five[-24:]]
    pivots = extrema(values, low)
    if not pivots:
        raise ValueError('NO_CONFIRMED_STOP_PIVOT')
    first = pivots[-1]
    valid = (lambda x: x < first and x < entry) if low else (lambda x: x > first and x > entry)
    second = next((x for x in reversed(pivots[:-1]) if (x < first if low else x > first)), None)
    extreme = min(values) if low else max(values)
    stop = second if second is not None and valid(second) else extreme if valid(extreme) else first
    stop = rounded(stop, tick, up=not low)
    if not (stop < entry if low else stop > entry):
        raise ValueError('INVALID_STRUCTURAL_STOP')
    return stop


def structure(frames, side, monitor_view=None):
    """Transparent local swing gate, NOT the old V13.5 frozen-range algorithm.

    Requires two most recent 5m closes beyond a confirmed 5m pivot. The pivot
    must already be knowable BEFORE the first confirmation candle opens.
    Live context comes from the embedded, unchanged V13.5.3 engine on ADA bars.
    The EMA/ER fallback is for isolated function tests only.
    """
    five, fifteen = frames['5m'], frames['15m']
    key, sign = (2, 1) if side == 'LONG' else (3, -1)
    candidates = []
    for i in range(max(2, len(five)-26), len(five)-4):
        value = five[i][key]
        if all(sign*value > sign*five[j][key] for j in (i-2, i-1, i+1, i+2)):
            candidates.append((i, value))
    if not candidates:
        return dict(pass_gate=False, context=monitor_view['15m']['regime'] if monitor_view else 'UNCERTAIN', monitor=monitor_view, reason='NO_KNOWN_5M_PIVOT')
    index, pivot = candidates[-1]
    history = five[:-2]
    buffer = .1 * atr(history)
    threshold = pivot + sign * buffer
    broken = all(sign*b[4] > sign*threshold for b in five[-2:])
    prices = [b[4] for b in fifteen]
    e20, e50 = seeded_ema(prices, 20), seeded_ema(prices, 50)
    movement = sum(abs(prices[j]-prices[j-1]) for j in range(len(prices)-19, len(prices)))
    er = abs(prices[-1]-prices[-20]) / movement if movement else 0
    context = 'RANGE' if er < .2 else 'UP' if e20 > e50 and prices[-1] > e20 else 'DOWN' if e20 < e50 and prices[-1] < e20 else 'UNCERTAIN'
    monitor_ok = True
    if monitor_view is not None:
        context = monitor_view['15m']['regime']
        own = monitor_view['5m']
        desired = 'UP' if side == 'LONG' else 'DOWN'
        monitor_ok = own['regime'] == desired
        if own['regime'] == 'RANGE':
            edge = own.get('resistance' if side == 'LONG' else 'support')
            monitor_ok = edge is not None and all(sign*b[4] > sign*edge + .25*atr(history) for b in five[-2:])
    opposite = context == ('DOWN' if side == 'LONG' else 'UP') or context == 'UNCERTAIN'
    return dict(pass_gate=broken and not opposite and monitor_ok, context=context, monitor=monitor_view, pivot=pivot,
                threshold=threshold, pivot_known_at=five[index+2][0]+300,
                reason='5M_PIVOT_BREAK' if broken else 'NO_TWO_CLOSE_BREAK')


# Embedded pure structure engine from V13.5.3; no BTC file imports, I/O or sender.
def aggregate15(bars):
    groups = {}
    for b in bars:
        groups.setdefault(int(b[0])//900*900, []).append(b)
    return [(s,g[0][1],max(b[2] for b in g),min(b[3] for b in g),g[-1][4])
            for s,g in sorted(groups.items()) if len(g)==3 and
            all(b[0]==s+i*300 for i,b in enumerate(g))]


def indicators(bars):
    closes = [b[4] for b in bars]
    ema = closes[0]
    values = []
    for close in closes:
        ema += 2/21*(close-ema)
        values.append(ema)
    tr = [max(b[2]-b[3],abs(b[2]-bars[i-1][4]),abs(b[3]-bars[i-1][4]))
          if i else b[2]-b[3] for i,b in enumerate(bars)]
    atr = sum(tr[-14:])/14
    if atr <= 0:
        atr = 1e-12  # Flat candles remain neutral; avoid division by zero.
    travel = sum(abs(closes[i]-closes[i-1]) for i in range(len(closes)-20,len(closes)))
    er = abs(closes[-1]-closes[-21])/travel if travel else 0.0
    return {'ema':ema, 'atr':atr, 'slope':(values[-1]-values[-6])/atr, 'er':er}


def quantile(values, q):
    values = sorted(values)
    return values[int((len(values)-1)*q)]


class Market:
    def __init__(self, tf, saved=None):
        self.tf = tf
        self.bars = []
        self.regime = 'UNCERTAIN'
        self.direction = 'FLAT'
        self.box = None
        self.proposal = None
        self.stop = None
        self.counter = 0
        self.counter_side = None
        self.last_transition = 0
        self.last_range_exit = 0
        self.range_candidate = None
        self.ind = None
        self.last3 = 0
        self.recent_three = []
        self.swing_high = None
        self.swing_low = None
        if saved:
            self.__dict__.update(saved)

    def switch(self, target, at, reason):
        old = self.regime
        if old=='RANGE' and target in ('UP','DOWN'):
            self.last_range_exit = at
        self.range_candidate = None
        self.regime = target
        self.last_transition = at
        self.counter = 0
        self.counter_side = None
        self.proposal = None
        return {'kind':'REGIME_CHANGED','tf':self.tf,'from':old,'to':target,'reason':reason,'at':at}

    def on_bar(self, bar):
        seconds = FRAMES[self.tf]
        if self.bars and bar[0] <= self.bars[-1][0]:
            return []
        if self.bars and bar[0] != self.bars[-1][0]+seconds:
            raise ValueError('FRAME_GAP_'+self.tf)
        self.bars.append(list(bar))
        self.bars = self.bars[-300:]
        if len(self.bars) < 65:
            return []
        self.ind = indicators(self.bars)
        atr, ema, slope, er = [self.ind[k] for k in ('atr','ema','slope','er')]
        at = int(bar[0])+seconds
        move = (bar[4]-self.bars[-4][4])/atr
        new_direction = 'UP' if move>0.25 else 'DOWN' if move < -0.25 else 'FLAT'
        events = []
        if new_direction != self.direction:
            events.append({'kind':'DIRECTION_CHANGED','tf':self.tf,'from':self.direction,'to':new_direction,'at':at})
            self.direction = new_direction
        pivot = self.bars[-3]
        neighbors = self.bars[-5:-3]+self.bars[-2:]
        if all(pivot[2]>x[2] for x in neighbors):
            self.swing_high = {'price':pivot[2],'open':pivot[0],'known_at':at}
        if all(pivot[3]<x[3] for x in neighbors):
            self.swing_low = {'price':pivot[3],'open':pivot[0],'known_at':at}
        # Cooldown follows a range EXIT, not every trend update; cap it at 30m.
        # Strong broad sideways structure may contain a directional short leg.
        can_range = self.box is None and (not self.last_range_exit or at-self.last_range_exit >= min(6*seconds,1800))
        range_matched = False
        if can_range:
            windows = {'3m':(72,48,36,24),'5m':(72,48,36,24),'15m':(40,32,24)}[self.tf]
            recent = self.bars[-6:]
            recent_travel = sum(abs(recent[i][4]-recent[i-1][4]) for i in range(1,6))
            recent_er = abs(recent[-1][4]-recent[0][4])/recent_travel if recent_travel else 0
            for n in windows:
                h = self.bars[-n-1:-1]
                if len(h)!=n:
                    continue
                low = quantile([b[3] for b in h],.10)
                high = quantile([b[2] for b in h],.90)
                width = high-low
                travel = sum(abs(h[i][4]-h[i-1][4]) for i in range(1,n))
                long_er = abs(h[-1][4]-h[0][4])/travel if travel else 0
                touches = all(any(b[3]<=low+.2*width for b in part) and
                              any(b[2]>=high-.2*width for b in part)
                              for part in (h[:n//2],h[n//2:]))
                if 0<width<=8*atr and long_er<=.30 and (recent_er<=.40 or long_er<=.20) and touches and low<=bar[4]<=high:
                    range_matched = True
                    candidate = self.range_candidate
                    overlap = max(0,min(high,candidate['resistance'])-max(low,candidate['support'])) if candidate else 0
                    union = max(high,candidate['resistance'])-min(low,candidate['support']) if candidate else width
                    if (candidate and at==candidate['last_seen']+seconds and overlap/union>=.70 and
                            candidate['support']<=bar[4]<=candidate['resistance']):
                        candidate['count'] += 1
                        candidate['last_seen'] = at
                    else:
                        candidate = {'support':low,'resistance':high,'buffer':.25*atr,'window':n,
                                     'first_seen':at,'last_seen':at,'count':1}
                    self.range_candidate = candidate
                    if candidate['count']>=2:
                        self.box = {'support':candidate['support'],'resistance':candidate['resistance'],
                                    'buffer':candidate['buffer'],'formed_at':at,'window':candidate['window']}
                        events.append(self.switch('RANGE',at,'TWO_OWN_TF_CLOSES_CONFIRM_RANGE'))
                        self.stop = None
                    break
        if self.box is None and not range_matched:
            self.range_candidate = None
        # Trail only after a pivot has two closed candles on each side.
        if self.regime in ('UP','DOWN') and self.stop is not None:
            pivot = self.bars[-3]
            neighbors = self.bars[-5:-3]+self.bars[-2:]
            candidate_stop = None
            if self.regime=='UP' and all(pivot[3]<x[3] for x in neighbors):
                candidate_stop = max(self.stop,pivot[3]-.15*atr)
            elif self.regime=='DOWN' and all(pivot[2]>x[2] for x in neighbors):
                candidate_stop = min(self.stop,pivot[2]+.15*atr)
            if candidate_stop is not None and candidate_stop!=self.stop:
                events.append({'kind':'INVALIDATION_MOVED','tf':self.tf,'from':self.stop,'to':candidate_stop,'at':at})
                self.stop = candidate_stop
                self.counter = 0
                self.counter_side = None
        history = self.bars[-7:-1]
        if self.regime == 'UNCERTAIN':
            side = 'UP' if slope>=.25 and er>=.20 and bar[4]>ema else 'DOWN' if slope<=-.25 and er>=.20 and bar[4]<ema else None
            if self.proposal and (self.proposal['side']!=side or at-self.proposal['created']>6*seconds):
                self.proposal = None
                self.counter = 0
                self.counter_side = None
            if side and self.proposal is None:
                self.proposal = {'side':side, 'created':at, 'buffer':.10*atr,
                                 'level':max(b[2] for b in history) if side=='UP' else min(b[3] for b in history),
                                 'stop':min(b[3] for b in history) if side=='UP' else max(b[2] for b in history)}
        return events

    def on_three(self, bar):
        at = int(bar[0])+180
        if at<=self.last3:
            return []
        self.last3 = at
        self.recent_three = (self.recent_three+[list(bar)])[-2:]
        if self.ind is None:
            return []
        if self.regime!='RANGE' and len(self.recent_three)==2 and self.swing_high and self.swing_low:
            high,low = self.swing_high,self.swing_low
            buffer = .10*self.ind['atr']
            up = low['open']>high['open'] and all(x[0]>=high['known_at'] and x[4]>high['price']+buffer for x in self.recent_three)
            down = high['open']>low['open'] and all(x[0]>=low['known_at'] and x[4]<low['price']-buffer for x in self.recent_three)
            side = 'UP' if up else 'DOWN' if down else None
            if side and side!=self.regime:
                event = self.switch(side,at,'TWO_3M_CLOSES_BEYOND_CONFIRMED_OWN_TF_PIVOT')
                self.stop = low['price']-.15*self.ind['atr'] if side=='UP' else high['price']+.15*self.ind['atr']
                return [event]
        target = None
        reason = None
        next_stop = None
        if self.regime=='RANGE' and self.box:
            b = self.box
            if bar[0] < b['formed_at']:
                return []
            target = 'UP' if bar[4]>b['resistance']+b['buffer'] else 'DOWN' if bar[4]<b['support']-b['buffer'] else None
            reason = 'TWO_3M_CLOSES_OUTSIDE_FROZEN_RANGE'
            next_stop = b['support']-b['buffer'] if target=='UP' else b['resistance']+b['buffer']
        elif self.regime in ('UP','DOWN') and self.stop is not None:
            crossed = bar[4]<self.stop if self.regime=='UP' else bar[4]>self.stop
            target = 'UNCERTAIN' if crossed else None
            reason = 'TWO_3M_CLOSES_BEYOND_TREND_INVALIDATION'
        elif self.regime=='UNCERTAIN' and self.proposal:
            p = self.proposal
            if bar[0] < p['created']:
                return []
            crossed = bar[4]>p['level']+p['buffer'] if p['side']=='UP' else bar[4]<p['level']-p['buffer']
            target = p['side'] if crossed else None
            reason = 'TWO_3M_CLOSES_BEYOND_OWN_TF_STRUCTURE'
            next_stop = p['stop']
        self.counter = self.counter+1 if target and target==self.counter_side else 1 if target else 0
        self.counter_side = target
        if self.counter<2:
            return []
        event = self.switch(target,at,reason)
        self.box = None
        self.stop = next_stop
        return [event]

    def view(self):
        last = self.bars[-1] if self.bars else None
        return {'regime':self.regime,'direction':self.direction,'close':last[4] if last else None,
                'candle_end':int(last[0])+FRAMES[self.tf] if last else None,
                'support':self.box['support'] if self.box else None,
                'resistance':self.box['resistance'] if self.box else None,
                'invalidation':self.stop,'pending':self.counter_side,'confirmation_closes':self.counter,
                'indicators':self.ind}


class Reversal:
    def __init__(self, saved=None):
        self.pending = {}
        self.active = []
        if saved:
            self.__dict__.update(saved)

    def step(self, bars):
        if len(bars)<11:
            return []
        b = bars[-1]
        at = int(b[0])+180
        events = []
        for a in list(self.active):
            invalid = b[4]<a['extreme'] if a['side']=='UP' else b[4]>a['extreme']
            zone = a.get('zone')
            zone_invalid = zone and (b[4]<zone['low'] if a['side']=='UP' else b[4]>zone['high'])
            if invalid or zone_invalid or at-a['confirmed_at']>3600:
                events.append({'kind':'EARLY_INVALIDATED' if invalid or zone_invalid else 'EARLY_EXPIRED',
                               'side':a['side'],'at':at,'origin':a['origin'],'zone':zone})
                self.active.remove(a)
        for side, c in list(self.pending.items()):
            invalid = b[4]<c['extreme'] if side=='UP' else b[4]>c['extreme']
            if invalid or at-c['origin']>6*180:
                if c['local_announced']:
                    events.append({'kind':'EARLY_INVALIDATED' if invalid else 'EARLY_EXPIRED','side':side,'at':at,'origin':c['origin'],'zone':c.get('zone')})
                del self.pending[side]
                continue
            outside = b[4]>c['trigger'] if side=='UP' else b[4]<c['trigger']
            context = b[4]>c['context'] if side=='UP' else b[4]<c['context']
            c['local_count'] = c['local_count']+1 if outside else 0
            c['context_count'] = c['context_count']+1 if context else 0
            if outside and c['local_count']==1 and not c['local_announced']:
                zone = None
                for x in reversed([x for x in bars if c['origin']-180<=x[0]<b[0]]):
                    if (x[4]<x[1] if side=='UP' else x[4]>x[1]):
                        zone = {'low':x[3],'high':x[2],'candle_open_ts':x[0]}
                        break
                c['zone'] = zone
            if c['local_count']>=2 and not c['local_announced']:
                c['local_announced'] = True
                if c.get('zone'):
                    c['zone']['known_at_close_ts'] = at
                events.append({'kind':'EARLY_LOCAL','side':side,'at':at,'origin':c['origin'],
                               'level':c['trigger'],'zone':c.get('zone')})
            if c['local_count']>=2 and c['context_count']>=2:
                events.append({'kind':'EARLY_CONTEXT_CONFIRMED','side':side,'at':at,'origin':c['origin'],
                               'level':c['context'],'zone':c.get('zone')})
                self.active.append(dict(c, confirmed_at=at))
                del self.pending[side]
        h = bars[-11:-1]
        low, high = min(x[3] for x in h), max(x[2] for x in h)
        sides = []
        if b[3]<low and b[4]>low and b[4]>b[1]:
            sides.append(('UP',b[3],b[2],high))
        if b[2]>high and b[4]<high and b[4]<b[1]:
            sides.append(('DOWN',b[2],b[3],low))
        for side, extreme, trigger, context in sides:
            previous = self.pending.get(side)
            if previous and previous['local_announced']:
                events.append({'kind':'EARLY_REPLACED','side':side,'at':at,'origin':previous['origin']})
            self.pending[side] = dict(side=side,extreme=extreme,trigger=trigger,context=context,
                                      origin=at,local_count=0,context_count=0,local_announced=False)
        return events


class StructureEngine:
    def __init__(self, saved=None):
        self.frames = {tf:Market(tf, saved['frames'][tf] if saved else None) for tf in FRAMES}
        self.reversal = Reversal(saved.get('reversal') if saved else None)
        self.last_decision = saved.get('last_decision',0) if saved else 0

    def revalidate_ranges(self):
        # Upgrade existing state without inventing bars or changing active range bounds.
        # Two already-closed own-TF bars are rechecked; only non-range states are eligible.
        changes = []
        for tf, machine in self.frames.items():
            if machine.regime=='RANGE' or len(machine.bars)<67:
                continue
            bars = list(machine.bars)
            before = machine.regime
            machine.bars = bars[:-2]
            machine.range_candidate = None
            for bar in bars[-2:]:
                machine.on_bar(bar)
            if before!=machine.regime:
                changes.append({'tf':tf,'from':before,'to':machine.regime})
        return changes

    def dump(self):
        return {'version':VERSION,'frames':{k:v.__dict__ for k,v in self.frames.items()},
                'reversal':self.reversal.__dict__,'last_decision':self.last_decision}

    def view(self):
        return {k:v.view() for k,v in self.frames.items()}

    def ingest(self, five, three):
        # Merge by close time, own 15m/5m structures BEFORE 3m confirmations on ties.
        fifteen = aggregate15(five)
        timeline = [(int(b[0])+300,1,'5m',b) for b in five]
        timeline += [(int(b[0])+900,0,'15m',b) for b in fifteen]
        timeline += [(int(b[0])+180,2,'3m',b) for b in three]
        events = []
        for at, _, tf, b in sorted(timeline):
            machine = self.frames[tf]
            if machine.bars and b[0]<=machine.bars[-1][0]:
                continue
            # Never consume a 3m decision until required 5m/15m closes are available.
            if tf=='3m' and at>self.last_decision:
                f = self.frames['5m'].bars
                q = self.frames['15m'].bars
                if f and int(f[-1][0])+300 < at//300*300:
                    break
                if q and int(q[-1][0])+900 < at//900*900:
                    break
            current_events = machine.on_bar(b)
            if tf=='3m':
                if machine.ind is not None:
                    for m in self.frames.values():
                        current_events += m.on_three(b)
                    current_events += self.reversal.step(machine.bars)
                self.last_decision = at
            for event in current_events:
                event['frames'] = self.view()
            events += current_events
        return events


class Feed(threading.Thread):
    def __init__(self, stop):
        super().__init__(daemon=True)
        self.stop = stop
        self.lock = threading.Lock()
        self.bids, self.asks = {}, {}
        self.trades, self.pending = deque(), deque()
        self.seen = set()
        self.book_at = self.trade_at = self.since = 0
        self.ready = False
        self.error = None
        self.overflow = False

    def reset(self):
        with self.lock:
            self.bids.clear(); self.asks.clear()
            self.trades.clear(); self.pending.clear(); self.seen.clear()
            self.book_at = self.trade_at = 0
            self.since = time.time()
            self.ready = False
            self.overflow = False

    def run(self):
        import websocket
        while not self.stop.is_set():
            ws = None
            self.reset()
            try:
                ws = websocket.create_connection(WS, timeout=8)
                ws.send(json.dumps(dict(op='subscribe', args=['orderbook.200.'+SYMBOL, 'publicTrade.'+SYMBOL])))
                ping = time.monotonic()
                while not self.stop.is_set():
                    if time.monotonic()-ping >= 20:
                        ws.send(json.dumps(dict(op='ping'))); ping = time.monotonic()
                    ws.settimeout(2)
                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue
                    if not raw:
                        raise RuntimeError('FEED_CLOSED')
                    msg = json.loads(raw)
                    if msg.get('op') == 'subscribe' and not msg.get('success'):
                        raise RuntimeError('SUBSCRIPTION_REJECTED')
                    topic, data = msg.get('topic', ''), msg.get('data')
                    now = time.time()
                    with self.lock:
                        if topic.startswith('orderbook.'):
                            if msg.get('type') == 'snapshot' or data.get('u') == 1:
                                self.bids.clear(); self.asks.clear(); self.ready = True
                            if not self.ready:
                                continue
                            for key, book in (('b', self.bids), ('a', self.asks)):
                                for p, q in data[key]:
                                    p, q = float(p), float(q)
                                    if q == 0:
                                        book.pop(p, None)
                                    else:
                                        book[p] = q
                            self.book_at = now
                        elif topic == 'publicTrade.'+SYMBOL:
                            for t in sorted(data, key=lambda t: int(t['T'])):
                                identity = t['i']
                                if identity in self.seen or t.get('BT'):
                                    continue
                                epoch, price, size = int(t['T'])/1000, float(t['p']), float(t['v'])
                                if not (0 < price and 0 < size and abs(now-epoch) <= 15):
                                    continue
                                self.seen.add(identity)
                                trade = (epoch, price, size, t['S'])
                                self.trades.append(trade); self.pending.append(trade)
                                self.trade_at = max(self.trade_at, epoch)
                            while self.trades and self.trades[0][0] < now-90:
                                self.trades.popleft()
                            if len(self.seen) > 200000 or len(self.pending) > 100000:
                                self.overflow = True
                                raise RuntimeError('FEED_OVERFLOW')
                        self.error = None
            except Exception as exc:
                with self.lock:
                    self.ready = False
                    self.error = type(exc).__name__
                self.stop.wait(3)
            finally:
                if ws:
                    ws.close()

    def snapshot(self):
        with self.lock:
            now = time.time()
            bid = max(self.bids, default=0)
            ask = min(self.asks, default=0)
            healthy = self.ready and 0 < bid < ask and now-self.book_at <= 5 and now-self.trade_at <= 15 and not self.overflow
            covered = healthy and now-self.since >= 60
            trades = [t for t in self.trades if now-60 <= t[0] <= now]
            buy = sum(t[1]*t[2] for t in trades if t[3] == 'Buy')
            sell = sum(t[1]*t[2] for t in trades if t[3] == 'Sell')
            pending = list(self.pending); self.pending.clear()
            return dict(healthy=healthy, covered=covered, bid=bid, ask=ask,
                        spread_bps=(ask-bid)/((ask+bid)/2)*10000 if bid and ask else None,
                        buy_usdt=buy, sell_usdt=sell, trades=len(trades),
                        imbalance=(buy-sell)/(buy+sell) if buy+sell else None,
                        book_bid_usdt=sum(p*q for p,q in self.bids.items()),
                        book_ask_usdt=sum(p*q for p,q in self.asks.items()),
                        error=self.error), pending


def entry_plan(frames, decision, live, tick, now, monitor_view=None,
               confirmed_side=None, received_at=None, market_price=None):
    side = confirmed_side if confirmed_side is not None else confirmation(frames, decision)
    if side not in ('LONG','SHORT'):
        return None, 'NO_BTC_TMCO_PAIR'
    received_at = decision if received_at is None else received_at
    if not 0 <= now-received_at <= 30:
        return None, 'STALE_DECISION'
    for tf,count in (('1m',240),('3m',79),('5m',100)):
        bars=frames.get(tf,[])
        if len(bars)<count or any(b[0]+TF[tf]>decision for b in bars):
            raise ValueError('INVALID_CANDLE_TIMING_OR_HISTORY_'+tf)
    f = evaluate(side, frames)
    if f['score'] < 4 or f['b3_reject']:
        return None, dict(reject='BTC_FILTERS',filters=f)
    if market_price is None:
        rows=get('/v5/market/tickers',category='linear',symbol=SYMBOL)['list']
        if len(rows)!=1 or rows[0]['symbol']!=SYMBOL:
            raise ValueError('INVALID_TICKER')
        market_price=float(rows[0]['lastPrice'])
        now=time.time()
    if not 0 <= now-received_at <= 30:
        return None, 'STALE_DECISION'
    entry=float(market_price)
    reference=frames['1m'][-1][4]
    if not math.isfinite(entry) or entry<=0 or reference<=0:
        raise ValueError('INVALID_PRICE')
    sign=1 if side=='LONG' else -1
    if sign*(entry-reference)/reference*100 > .10:
        return None, 'PRICE_CHASE'
    stop=structural_stop(side,frames['5m'][-100:],entry,tick)
    risk=sign*(entry-stop)
    if risk<=0:
        return None, 'INVALID_RISK'
    targets=[nearest_tick(entry+sign*risk*k,tick) for k in (.75,1.5,2)]
    if not (all(sign*(x-entry)>0 for x in targets) and len(set(targets))==3):
        return None, 'INVALID_TARGET_ORDER_AFTER_TICK_ROUNDING'
    atr5=sum(max(b[2]-b[3],abs(b[2]-a[4]),abs(b[3]-a[4]))
             for a,b in zip(frames['5m'][-15:-1],frames['5m'][-14:]))/14
    return dict(side=side,entry=entry,stop=stop,initial_stop=stop,targets=targets,
                hit=0,opened=now,decision=decision,remaining=1.0,net_return=0.0,
                uncertain=False,entry_minute_unverified=True,
                risk_atr=risk/atr5 if atr5>0 else None,filters=f,
                structure=dict(context=(monitor_view or {}).get('15m',{}).get('regime','UNCERTAIN')),
                management='BTC_FIXED_SL_FULL_POSITION',
                monitor_next=int(math.ceil(now/60))*60), 'ACCEPTED'



def nearest_tick(value,tick):
    q=Decimal(str(tick))
    return float((Decimal(str(value))/q).to_integral_value(rounding=ROUND_HALF_EVEN)*q)

def fixed_candle(position,bar):
    # Same precedence as BTC's active check_fixed_sl_candle, not its unused
    # trailing-stop helper. A stop candle does not add TP1/TP2 flags.
    p=position
    sign=1 if p['side']=='LONG' else -1
    high,low=bar[2],bar[3]
    stop_hit=low<=p['initial_stop'] if sign==1 else high>=p['initial_stop']
    hits=[high>=x if sign==1 else low<=x for x in p['targets']]
    at=bar[0]+60
    status='UNKNOWN' if stop_hit and hits[2] else 'EXIT_STOP' if stop_hit else 'TP3' if hits[2] else None
    events=[]
    if status:
        if status=='TP3': p['hit']=3
        p['remaining']=0.0
        p['stop']=p['initial_stop']
        p['uncertain']=bool(p.get('uncertain') or status=='UNKNOWN')
        exit_price=p['initial_stop'] if status=='EXIT_STOP' else p['targets'][2] if status=='TP3' else None
        p['net_return']=sign*(exit_price-p['entry'])/p['entry'] if exit_price is not None else None
        return [dict(kind=status,at=at,position=json.loads(json.dumps(p)),price=exit_price)]
    hit=max(p['hit'],2 if hits[1] else 1 if hits[0] else 0)
    for level in range(p['hit']+1,hit+1):
        p['hit']=level
        events.append(dict(kind='TP'+str(level),at=at,position=json.loads(json.dumps(p))))
    return events

def monitor_positions(store,through,telegram):
    # Sequential recovery of every missing full minute, including after restart.
    # No elapsed-time exit; old one-position state is handled separately.
    cache={}
    for p in list(store.state.get('positions',[])):
        for batch in range(20):
            start=p['monitor_next']
            end=min(int(through)//60*60,start+240*60)
            if end<=start: break
            key=(start,end)
            if key not in cache: cache[key]=candles('1m',end,(end-start)//60)
            bars=cache[key]
            if bars[0][0]!=start:
                raise ValueError('MISSING_MONITOR_CANDLES')
            events=[]
            for bar in bars:
                events.extend(fixed_candle(p,bar))
                p['monitor_next']=bar[0]+60
                if p['remaining']==0:
                    store.state['positions'].remove(p)
                    break
            store.commit(events,telegram=telegram)
            if p['remaining']==0: break
        else:
            raise RuntimeError('MONITOR_CATCHUP_LIMIT')


def paper_step(position, price, epoch, tick):
    """Ordered public-trade threshold observations, not exchange fills.

    Each target closes 1/3. Stops include adverse slippage. Net is fractional
    notional return, no leverage. Trades before the signal cannot hit targets.
    """
    p, events = position, []
    if epoch < p['opened']:
        return events
    if epoch < p.get('last_trade_at', p['opened']):
        p['uncertain'] = True
        return [dict(kind='DATA_GAP', at=epoch, position=json.loads(json.dumps(p)), detail='معامله با زمان عقب‌تر دریافت شد؛ ترتیب قیمت نامطمئن است')]
    p['last_trade_at'] = epoch
    sign = 1 if p['side']=='LONG' else -1
    if sign*(price-p['stop']) <= 0:
        fill = min(price, p['stop']) if sign==1 else max(price, p['stop'])
        fill *= 1-sign*POLICY['slippage_bps']/10000
        p['net_return'] += p['remaining']*(sign*(fill-p['entry'])/p['entry'] - POLICY['fee_bps']/10000*(1+fill/p['entry']))
        p['remaining'] = 0
        return [dict(kind='EXIT_STOP', price=fill, at=epoch, position=json.loads(json.dumps(p)))]
    while p['hit'] < 3 and sign*(price-p['targets'][p['hit']]) >= 0:
        fill = p['targets'][p['hit']] * (1-sign*POLICY['slippage_bps']/10000)
        amount = min(1/3, p['remaining'])
        p['net_return'] += amount*(sign*(fill-p['entry'])/p['entry'] - POLICY['fee_bps']/10000*(1+fill/p['entry']))
        p['remaining'] -= amount
        p['hit'] += 1
        p['stop'] = p['entry'] if p['hit']==1 else p['targets'][0] if p['hit']==2 else p['stop']
        if p['hit'] == 3:
            p['remaining'] = 0
        events.append(dict(kind='TP'+str(p['hit']), price=fill, at=epoch, position=json.loads(json.dumps(p))))
    return events


def legacy_text(event):
    labels = dict(STARTED='شروع پایش مستقل', ENTRY='ورود آزمایشی', EXIT_STOP='خروج با حد ضرر',
                  TP1='هدف اول؛ حد ضرر به ورود منتقل شد', TP2='هدف دوم؛ حد ضرر به هدف اول منتقل شد',
                  TP3='هدف سوم؛ پایان معامله', DATA_GAP='وقفه داده؛ نتیجه معامله نیازمند بررسی',
                  ERROR='خطای دریافت داده', EXIT_TIME='خروج به علت پایان زمان اسکلپ')
    lines = ['ADAUSDT | اسکلپ ۵ دقیقه', labels.get(event['kind'],event['kind']), 'زمان ایران: '+stamp(event['at'])]
    p = event.get('position')
    if p:
        lines += ['', 'جهت: '+('خرید' if p['side']=='LONG' else 'فروش'),
                  'ورود مرجع: '+format(p['entry'],'.10f').rstrip('0').rstrip('.'),
                  'حد ضرر فعلی: '+format(p['stop'],'.10f').rstrip('0').rstrip('.')]
        lines += ['هدف '+str(i+1)+': '+format(v,'.10f').rstrip('0').rstrip('.') for i,v in enumerate(p['targets'])]
        lines += ['امتیاز فیلتر: '+str(p['filters']['score'])+'/6',
                  'زمینه ۱۵ دقیقه: '+{'UP':'صعودی','DOWN':'نزولی','RANGE':'رنج','UNCERTAIN':'نامشخص'}[p['structure']['context']],
                  'تأیید: دو تقاطع متوالی هم‌جهت مانند BTC؛ فیلتر ساختار غیرفعال است']
        if p['remaining'] == 0:
            lines.append('بازده خالص مدل: '+format(p['net_return']*100,'.3f')+' درصد')
        if p['uncertain']:
            lines.append('هشدار: وقفه داده؛ بازده برای سنجش دقت معتبر نیست')
    if event.get('detail'):
        lines += ['', 'توضیح: '+event['detail']]
    return '\n'.join(lines+['','پژوهشی؛ بدون سفارش واقعی. اجرای آزمایشی و هزینه‌ها تخمینی‌اند.'])[:3900]


def text(event):
    p=event.get('position')
    if p and p.get('management')!='BTC_FIXED_SL_FULL_POSITION':
        return legacy_text(event)
    labels=dict(STARTED='شروع پایش مستقل',ENTRY='ورود آزمایشی',TP1='رسیدن به هدف اول',
                TP2='رسیدن به هدف دوم',TP3='خروج کامل در هدف سوم',EXIT_STOP='خروج کامل با حد ضرر',
                UNKNOWN='ترتیب برخورد استاپ و هدف سوم نامشخص',ERROR='خطای دریافت داده')
    lines=['ADAUSDT | قواعد BTC | نسخه ۱٫۳',labels.get(event['kind'],event['kind']),
           'زمان ایران: '+stamp(event['at'])]
    if p:
        fmt=lambda v: format(v,'.10f').rstrip('0').rstrip('.')
        lines+=['','شناسه سیگنال: '+str(p.get('signal_id','—')),
                'جهت: '+p['side'],'ورود مرجع: '+fmt(p['entry']),
                'حد ضرر ثابت: '+fmt(p['initial_stop'])]
        lines+=['هدف '+str(i+1)+': '+fmt(v) for i,v in enumerate(p['targets'])]
        lines+=['امتیاز فیلتر: '+str(p['filters']['score'])+' از ۶',
                'خروج کامل در استاپ یا هدف سوم؛ بدون خروج مرحله‌ای',
                'اهداف اول و دوم حد ضرر را جابه‌جا نمی‌کنند']
        if p['remaining']==0 and p.get('net_return') is not None:
            lines+=['بازده مرجع پیش از هزینه‌ها: '+format(p['net_return']*100,'.3f')+' درصد']
        lines+=['دقیقه ورود کامل بررسی نمی‌شود؛ نتیجه بر اساس کندل‌های کامل بعدی است']
        if p.get('uncertain'): lines+=['نتیجه نامشخص؛ برای سنجش بازده معتبر نیست']
    if event.get('detail'): lines+=['توضیح: '+event['detail']]
    return '\n'.join(lines+['','پژوهشی؛ بدون سفارش واقعی'])


class Store:
    def __init__(self, folder):
        self.path = folder/'ada_v1.sqlite3'
        self.db = sqlite3.connect(self.path, timeout=10)
        self.db.executescript('CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY, payload TEXT);'
                              'CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, at REAL, payload TEXT, sent INTEGER);'
                              'CREATE TABLE IF NOT EXISTS decisions (at INTEGER PRIMARY KEY, payload TEXT);')
        row = self.db.execute('SELECT payload FROM state WHERE id=1').fetchone()
        self.state = json.loads(row[0]) if row else dict(last=0, position=None)

    def commit(self, events=(), decision=None, telegram=False):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO state VALUES (1,?)',(json.dumps(self.state,allow_nan=False),))
            for event in events:
                body = json.dumps(event,ensure_ascii=False,allow_nan=False)
                identity = hashlib.sha256(body.encode()).hexdigest()
                self.db.execute('INSERT OR IGNORE INTO events VALUES (?,?,?,?)',
                                (identity,event['at'],body,0 if telegram else 1))
                print(body,flush=True)
            if decision:
                self.db.execute('INSERT OR IGNORE INTO decisions VALUES (?,?)',
                                (decision['at'],json.dumps(decision,allow_nan=False)))


def credentials(path):
    values = {}
    for raw in Path(path).read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if line.startswith('export '):
            line = line[7:]
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=',1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    if not values.get('BOT_TOKEN') or not values.get('CHAT_ID'):
        raise ValueError('TELEGRAM_CONFIGURATION_MISSING')
    return values['BOT_TOKEN'],values['CHAT_ID']


class Sender(threading.Thread):
    def __init__(self, path, token, chat, stop):
        super().__init__(daemon=True)
        self.path,self.token,self.chat,self.stop = path,token,chat,stop

    def run(self):
        db = sqlite3.connect(self.path,timeout=10)
        while not self.stop.wait(3):
            row = db.execute('SELECT id,payload FROM events WHERE sent=0 ORDER BY at,rowid LIMIT 1').fetchone()
            if not row:
                continue
            try:
                event = json.loads(row[1])
                if event['kind'] == 'ENTRY' and time.time()-event['at'] > POLICY['decision_age']:
                    with db:
                        db.execute('UPDATE events SET sent=2 WHERE id=?',(row[0],))
                    print('EXPIRED_ENTRY_MESSAGE_SUPPRESSED',flush=True)
                    continue
                payload = urllib.parse.urlencode(dict(chat_id=self.chat,text=text(event))).encode()
                req = urllib.request.Request('https://api.telegram.org/bot'+self.token+'/sendMessage', data=payload)
                with urllib.request.urlopen(req,timeout=8) as response:
                    answer = json.load(response)
                if not answer.get('ok'):
                    raise RuntimeError('TELEGRAM_REJECTED')
                with db:
                    db.execute('UPDATE events SET sent=1 WHERE id=?',(row[0],))
            except Exception:
                print('TELEGRAM_RETRY_PENDING',flush=True)
                self.stop.wait(20)
        db.close()


def legacy_self_test():
    assert rsi_series([1.0]*30,14)[-1] == 50
    assert rsi_series(list(range(30)),14)[-1] == 100
    assert extrema([3,2,2,3,1,2],True) == [2,1]
    assert rounded(.35129,.0001) == .3512
    assert rounded(.35121,.0001,True) == .3513
    bars = [[i*300,1,1.1,.9,1+.01*math.sin(i),100] for i in range(100)]
    validate(bars,300)
    events = crosses(bars,'5m')
    for n in range(10,101):
        assert crosses(bars[:n],'5m') == [e for e in events if e['at']<=n*300]
    frames = {'5m':bars,'15m':[[i*900,*b[1:]] for i,b in enumerate(bars)]}
    for side in ('LONG','SHORT'):
        result = structure(frames,side)
        if 'pivot_known_at' in result:
            assert result['pivot_known_at'] <= bars[-2][0]
    p = dict(side='LONG', entry=.4, stop=.39, initial_stop=.39, targets=[.4075,.415,.42],
             hit=0, opened=100, remaining=1., net_return=0., uncertain=False)
    assert paper_step(p,.5,99,.0001)==[]
    assert paper_step(p,.408,101,.0001)[0]['kind']=='TP1' and p['stop']==.4
    assert paper_step(p,.399,102,.0001)[0]['kind']=='EXIT_STOP' and p['remaining']==0
    p = dict(side='SHORT', entry=.4, stop=.41, targets=[.3925,.385,.38], hit=0,
             opened=100, remaining=1., net_return=0.)
    assert len(paper_step(p,.379,101,.0001))==3 and p['remaining']==0
    gate = BTCCrossGate()
    e = lambda at,tf,side: dict(at=at,tf=tf,side=side)
    assert gate.process(e(600,'1m','LONG'),601)[0] is None
    confirmed, _ = gate.process(e(660,'3m','LONG'),661)
    assert confirmed and confirmed['second']['tf']=='3m'
    assert gate.process(e(720,'5m','LONG'),721)[0] is None
    assert gate.process(e(780,'1m','LONG'),781)[0] is not None
    assert gate.process(e(780,'1m','LONG'),781)[1]=='DUPLICATE'
    assert gate.process(e(840,'3m','SHORT'),856)[1]=='LATE_CROSS'
    restored = BTCCrossGate(json.loads(json.dumps(gate.dump())))
    assert restored.dump()==gate.dump()
    print('SELF_TEST_OK: BTC cross parity, consumed pairs, duplicate/late rejection, state restore, ticks and paper exits')


def self_test():
    legacy_self_test()
    p=dict(side='LONG',entry=100,stop=90,initial_stop=90,targets=[107.5,115,120],
           hit=0,remaining=1,uncertain=False)
    assert fixed_candle(p,[0,100,108,99,107,1])[0]['kind']=='TP1'
    assert p['stop']==90 and p['remaining']==1
    assert fixed_candle(p,[60,107,116,99,115,1])[0]['kind']=='TP2'
    assert p['stop']==90 and p['remaining']==1
    assert fixed_candle(p,[120,115,119,89,90,1])[0]['kind']=='EXIT_STOP'
    assert p['net_return']==-.1
    print('BTC_RULES_SELF_TEST_OK: fixed initial SL, full-size exit, no entry flow/ATR gate')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument('--self-test',action='store_true')
    modes.add_argument('--check',type=int,metavar='SECONDS')
    modes.add_argument('--watch',action='store_true')
    parser.add_argument('--telegram',action='store_true')
    parser.add_argument('--telegram-env',default='/opt/tmco_webhook_v4/telegram.env')
    parser.add_argument('--data-dir',default='/opt/ada_scalp_5m_research')
    args = parser.parse_args()
    if args.self_test:
        self_test(); return
    if args.telegram and not args.watch:
        parser.error('--telegram requires --watch')
    token_chat = credentials(args.telegram_env) if args.telegram else None
    if args.check is not None and not 10 <= args.check <= 600:
        parser.error('--check must be 10..600 seconds')
    import websocket
    if not hasattr(websocket,'create_connection'):
        raise RuntimeError('WEBSOCKET_CLIENT_REQUIRED')
    import fcntl
    folder = Path(args.data_dir)
    folder.mkdir(parents=True,exist_ok=True)
    lock = (folder/'ada_v1.lock').open('a')
    fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    instrument = get('/v5/market/instruments-info',category='linear',symbol=SYMBOL)['list'][0]
    if instrument['symbol'] != SYMBOL or instrument['status'] != 'Trading':
        raise RuntimeError('INVALID_INSTRUMENT')
    tick = float(instrument['priceFilter']['tickSize'])
    stop = threading.Event()
    signal.signal(signal.SIGTERM,lambda *_:stop.set())
    signal.signal(signal.SIGINT,lambda *_:stop.set())
    feed = Feed(stop); feed.start()
    store = None
    cross_gate = BTCCrossGate()
    frame_cache = {}
    pair_ready = False
    structure_engine = StructureEngine()
    if args.watch:
        store = Store(folder)
        store.state.setdefault('positions',[])
        store.state.setdefault('next_signal_id',1)
        if store.state.get('structure_engine'):
            structure_engine = StructureEngine(store.state['structure_engine'])
        if store.state.get('cross_engine'):
            cross_gate = BTCCrossGate(store.state['cross_engine'])
        now = time.time()
        # No historical entry signals after downtime. Existing paper position is
        # restored but marked uncertain: missed ticks cannot be reconstructed.
        if store.state['position']:
            store.state['position']['uncertain'] = True
        store.state['last'] = int(now)//60*60
        store.commit([dict(kind='STARTED',at=now,position=store.state['position'],detail='قواعد ورود BTC؛ استاپ ثابت و خروج کامل در هدف سوم یا استاپ؛ چند سیگنال هم‌زمان مجاز است')],telegram=args.telegram)
        if args.telegram:
            token,chat = token_chat
            Sender(store.path,token,chat,stop).start()
    started = time.monotonic()
    last_poll, last_error, gap_reported, final_live, final_frames = 0, 0, False, None, None
    try:
        while not stop.is_set():
            now = time.time()
            live,pending = feed.snapshot()
            final_live = live
            if store and store.state['position']:
                p = store.state['position']
                if not live['covered'] and not gap_reported:
                    p['uncertain'] = True; gap_reported = True
                    store.commit([dict(kind='DATA_GAP',at=now,position=dict(p))],telegram=args.telegram)
                if live['healthy']:
                    gap_reported = False
                for epoch,price,size,side in pending:
                    events = paper_step(p,price,epoch,tick)
                    if events:
                        if p['remaining']==0:
                            store.state['position'] = None
                        store.commit(events,telegram=args.telegram)
                    if p['remaining']==0:
                        break
                if store.state['position'] and live['healthy'] and now-p['opened']>=POLICY['max_hold_minutes']*60:
                    quote = live['bid'] if p['side']=='LONG' else live['ask']
                    # Reuse conservative stop fill at the executable quote for a time exit.
                    p['stop'] = quote
                    events = paper_step(p,quote,now,tick)
                    events[0]['kind'] = 'EXIT_TIME'; events[0]['position'] = dict(p)
                    store.state['position'] = None
                    store.commit(events,telegram=args.telegram)
            decision = int(now)//60*60
            needs = (not store and (final_frames is None or now-last_poll>=30)) or (store and (not pair_ready or decision>store.state['last']))
            if needs and now-last_poll>=2:
                last_poll = now
                try:
                    cutoff = decision if store else time.time()
                    if store:
                        monitor_positions(store,decision,args.telegram)
                    frames = fetch_closed_frames(frame_cache,cutoff)
                    monitor_view = {}  # No structural entry veto in BTC.
                    final_frames = {tf:dict(end=b[-1][0]+TF[tf],close=b[-1][4]) for tf,b in frames.items()}
                    live,pending_extra = feed.snapshot()
                    # Restore trades drained by this metrics refresh so an existing
                    # paper position cannot silently miss its exit observations.
                    with feed.lock:
                        feed.pending.extendleft(reversed(pending_extra))
                    if store:
                        received_at = time.time()
                        confirmed, cross_diagnostic = cross_gate.advance(frames,decision,received_at)
                        starting = not pair_ready
                        pair_ready = True
                        events, attempts = [], []
                        reason = 'BOOTSTRAP_NO_ENTRY' if starting else 'NO_BTC_TMCO_PAIR'
                        if not starting:
                            for candidate in confirmed:
                                p, reason = entry_plan(frames,decision,live,tick,time.time(),monitor_view,candidate['side'],received_at=received_at)
                                attempts.append(dict(cross=candidate,result=reason))
                                if p:
                                    p['cross_pair'] = candidate
                                    p['policy_version'] = VERSION
                                    p['signal_id']=store.state['next_signal_id']
                                    store.state['next_signal_id']+=1
                                    store.state['positions'].append(p)
                                    events.append(dict(kind='ENTRY',at=p['opened'],position=json.loads(json.dumps(p))))
                        # Cross state, decisions and outbox commit together, including rejections.
                        store.state['last'] = decision
                        store.state['structure_engine'] = structure_engine.dump()
                        store.state['cross_engine'] = cross_gate.dump()
                        store.commit(events,dict(at=decision,result=reason,live=live,frames=frames,
                                     cross_diagnostic=cross_diagnostic,cross_confirmations=confirmed,
                                     candidate_attempts=attempts,policy_version=VERSION),telegram=args.telegram)
                    else:
                        print(json.dumps(dict(check=stamp(time.time()),frames=final_frames,structure=monitor_view,live=live,tick_size=tick)),flush=True)
                except Exception as exc:
                    print(json.dumps(dict(error=type(exc).__name__,detail=str(exc)[:120])),flush=True)
                    if store and time.time()-decision>POLICY['decision_age']:
                        store.state['last']=decision
                        store.commit(decision=dict(at=decision,result='DATA_ERROR'))
                    if store and now-last_error>300:
                        store.commit([dict(kind='ERROR',at=now,detail=type(exc).__name__)],telegram=args.telegram)
                        last_error=now
            if args.check and time.monotonic()-started>=args.check:
                current = time.time()
                for attempt in range(2):
                    if final_frames and all(v['end']==int(current)//TF[tf]*TF[tf] for tf,v in final_frames.items()):
                        break
                    cutoff = current
                    refreshed = fetch_closed_frames(frame_cache,cutoff)
                    final_frames = {tf:dict(end=b[-1][0]+TF[tf],close=b[-1][4]) for tf,b in refreshed.items()}
                    current = time.time()
                live,_ = feed.snapshot()
                fresh = final_frames is not None and all(v['end']==int(current)//TF[tf]*TF[tf] for tf,v in final_frames.items())
                ticker = get('/v5/market/tickers',category='linear',symbol=SYMBOL)['list']
                ticker_ok = len(ticker)==1 and ticker[0]['symbol']==SYMBOL and math.isfinite(float(ticker[0]['lastPrice'])) and float(ticker[0]['lastPrice'])>0
                passed = bool(fresh and ticker_ok)
                print(json.dumps(dict(check_passed=passed,live=live,candles_fresh=fresh,ticker_ok=ticker_ok,policy_version=VERSION)),flush=True)
                if not passed:
                    raise SystemExit(2)
                return
            stop.wait(.5)
    finally:
        stop.set()
        feed.join(timeout=5)
        if store:
            store.commit(); store.db.close()
        lock.close()


if __name__ == '__main__':
    main()
