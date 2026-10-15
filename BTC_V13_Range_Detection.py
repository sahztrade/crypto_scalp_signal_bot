#!/usr/bin/env python3
"""BTC V13: read-only, no-lookahead range classification at a historical signal time.

Requires the existing V8 candles() module and the BTC V4 paper database.
This is an experimental research classifier, not a validated trading filter.
"""
import argparse
import importlib.util
import json
import math
import sqlite3
from pathlib import Path

DEFAULT_V8 = '/opt/btc_v12_shadow/btc_v4_structure_context_v8_200_244.py'
DEFAULT_DB = '/opt/tmco_webhook_v4/data/v4_paper_live.db'

def load_module(path):
    spec = importlib.util.spec_from_file_location('btc_v13_v8', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def true_ranges(bars):
    out = []
    for i in range(1, len(bars)):
        _, _, hi, lo, _ = bars[i]
        prev = bars[i-1][4]
        out.append(max(hi-lo, abs(hi-prev), abs(lo-prev)))
    return out

def adx(bars, period=14):
    # Wilder smoothing: initial sum, subsequent prev - prev/n + new.
    tr, pdm, mdm = [], [], []
    for i in range(1, len(bars)):
        up = bars[i][2] - bars[i-1][2]
        down = bars[i-1][3] - bars[i][3]
        pdm.append(up if up > down and up > 0 else 0.0)
        mdm.append(down if down > up and down > 0 else 0.0)
        hi, lo, pc = bars[i][2], bars[i][3], bars[i-1][4]
        tr.append(max(hi-lo, abs(hi-pc), abs(lo-pc)))
    if len(tr) < period*2:
        return None
    st, sp, sm = sum(tr[:period]), sum(pdm[:period]), sum(mdm[:period])
    dx = []
    for i in range(period-1, len(tr)):
        if i >= period:
            st = st - st/period + tr[i]
            sp = sp - sp/period + pdm[i]
            sm = sm - sm/period + mdm[i]
        plus, minus = (100*sp/st, 100*sm/st) if st > 0 else (0,0)
        dx.append(100*abs(plus-minus)/(plus+minus) if plus+minus > 0 else 0)
    value = sum(dx[:period])/period
    for d in dx[period:]:
        value = (value*(period-1)+d)/period
    return value

def chop(bars, n=14):
    tr = true_ranges(bars)
    hi = max(b[2] for b in bars[-n:])
    lo = min(b[3] for b in bars[-n:])
    if hi <= lo:
        return None
    return 100 * math.log10(sum(tr[-n:])/(hi-lo))/math.log10(n)

def efficiency(bars, n=20):
    c = [b[4] for b in bars]
    traveled = sum(abs(c[i]-c[i-1]) for i in range(len(c)-n,len(c)))
    return abs(c[-1]-c[-n-1])/traveled if traveled else None

def bb_width_percentile(bars, n=20, lookback=100):
    c = [b[4] for b in bars]
    widths = []
    for end in range(len(c)-lookback+1,len(c)+1):
        window = c[end-n:end]
        if len(window) != n:
            return None, None
        avg = sum(window)/n
        variance = sum((x-avg)**2 for x in window)/n
        widths.append(4*math.sqrt(variance)/avg if avg else 0)
    current = widths[-1]
    percentile = 100*sum(w <= current for w in widths)/len(widths)
    return current, percentile

def ema_slope_atr(bars, n=20, lookback=5):
    c = [b[4] for b in bars]
    k = 2/(n+1)
    value = c[0]
    series = []
    for price in c:
        value = k*price+(1-k)*value
        series.append(value)
    trs = true_ranges(bars)
    atr = sum(trs[-14:])/14
    return abs(series[-1]-series[-1-lookback])/atr if atr else None

def structure(bars, window=72, atr_period=14):
    # Confirmed pivots: 2 bars on both sides, never inspect future candles.
    segment = bars[-window:]
    tr = true_ranges(bars)
    atr = sum(tr[-atr_period:])/atr_period
    highs, lows = [], []
    for i in range(2,len(segment)-2):
        hi, lo = segment[i][2], segment[i][3]
        if hi > max(segment[j][2] for j in (i-2,i-1,i+1,i+2)):
            highs.append(hi)
        if lo < min(segment[j][3] for j in (i-2,i-1,i+1,i+2)):
            lows.append(lo)
    if atr <= 0 or len(highs) < 2 or len(lows) < 2:
        return {'horizontal':False,'support':None,'resistance':None,
                'pivot_highs':len(highs),'pivot_lows':len(lows),'width_atr':None}
    # Median pivot levels are robust to isolated outliers; check recent pivots for clustering.
    sh = sorted(highs[-4:]); sl = sorted(lows[-4:])
    resistance = (sh[(len(sh)-1)//2]+sh[len(sh)//2])/2
    support = (sl[(len(sl)-1)//2]+sl[len(sl)//2])/2
    high_hits = sum(abs(x-resistance) <= 0.7*atr for x in highs[-6:])
    low_hits = sum(abs(x-support) <= 0.7*atr for x in lows[-6:])
    width_atr = (resistance-support)/atr
    horizontal = high_hits >= 2 and low_hits >= 2 and 1.5 <= width_atr <= 12
    return {'horizontal':horizontal,'support':support,'resistance':resistance,
            'pivot_highs':len(highs),'pivot_lows':len(lows),
            'high_hits':high_hits,'low_hits':low_hits,'width_atr':width_atr}

def classify(bars):
    if len(bars) != 288:
        raise ValueError(f'Expected 288 closed 5m bars; got {len(bars)}')
    a = adx(bars)
    ch = chop(bars)
    er = efficiency(bars)
    bw, bw_pct = bb_width_percentile(bars)
    slope = ema_slope_atr(bars)
    st = structure(bars)
    metrics = {'adx14':a,'chop14':ch,'er20':er,'bb_width20':bw,
               'bb_width_percentile100':bw_pct,'ema20_slope5_atr':slope,
               'structure':st}
    if any(x is None or not math.isfinite(x) for x in (a,ch,er,bw_pct,slope)):
        return 'UNCERTAIN',0,{},metrics
    flags = {'adx_low':a<20,'chop_high':ch>61.8,'er_low':er<0.25,
             'bb_compressed':bw_pct<=20,'horizontal_structure':st['horizontal'],
             'ema_flat':slope<0.35}
    weights = {'adx_low':1,'chop_high':2,'er_low':2,'bb_compressed':1,
               'horizontal_structure':2,'ema_flat':1}
    score = sum(weights[k] for k,v in flags.items() if v)
    close = bars[-1][4]
    # Breakout candidate only if a confirmed horizontal structure exists.
    # Buffer is ATR-based; price has to CLOSE outside prior levels.
    tr = true_ranges(bars)
    atr = sum(tr[-14:])/14
    breakout = bool(st['horizontal'] and atr > 0 and
                    (close > st['resistance']+0.2*atr or
                     close < st['support']-0.2*atr))
    if breakout:
        status='BREAKOUT_PENDING'
    elif score>=6 and st['horizontal']:
        status='RANGE_CONFIRMED'
    elif score<=2 and a>=25 and ch<61.8 and er>=0.25:
        status='TREND_CONFIRMED'
    else:
        status='UNCERTAIN'
    return status,score,flags,metrics

def main():
    p=argparse.ArgumentParser(description='V13 range classifier: read-only historical BTC signal')
    p.add_argument('--id',type=int,required=True)
    p.add_argument('--v8',default=DEFAULT_V8)
    p.add_argument('--db',default=DEFAULT_DB)
    args=p.parse_args()
    dburi=Path(args.db).resolve().as_uri()+'?mode=ro'
    with sqlite3.connect(dburi,uri=True) as db:
        row=db.execute('SELECT decision_ts,direction,entry FROM paper_trades_v4 WHERE signal_id=?',(args.id,)).fetchone()
    if not row:
        raise SystemExit('Signal not found')
    ts,side,entry=row
    try:
        bars=load_module(args.v8).candles(float(ts),288)
        status,score,flags,metrics=classify(bars)
        print(json.dumps({'signal_id':args.id,'side':side,'entry':entry,
                          'decision_ts':ts,'status':status,'range_score':score,
                          'flags':flags,'metrics':metrics,
                          'mode':'RESEARCH_READ_ONLY'},ensure_ascii=False,indent=2))
    except Exception as exc:
        print(json.dumps({'signal_id':args.id,'status':'INSUFFICIENT_DATA',
                          'error':f'{type(exc).__name__}: {exc}',
                          'mode':'RESEARCH_READ_ONLY'},ensure_ascii=False,indent=2))
        raise SystemExit(1)

if __name__=='__main__':
    main()
