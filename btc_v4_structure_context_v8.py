#!/usr/bin/env python3
"""BTC V8 historical shock-aware market-structure audit. READ ONLY.

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
DEFAULT_OUT = '/tmp/btc_v4_structure_context_v8'
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
        req = urllib.request.Request(URL + '?' + query, headers={'User-Agent': 'BTC-V8-readonly/1.0'})
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



# These labels are HISTORICAL annotations only, not training targets or live rules.
HISTORICAL_OUTCOME = {**{i:'TP3' for i in range(159,164)},164:'SL',165:'SL',
                      **{i:'TP3' for i in range(166,174)},**{i:'SL' for i in range(174,180)}}
# Known overlap from prior review. Parent positions should be evaluated separately.
PARENT_GROUP = {**{i:159 for i in range(159,164)}, **{i:166 for i in range(166,174)}}

def safe_ratio(a,b):
    return a/b if b and b>0 else None

def signed_direction(side):
    return 1 if side=='LONG' else -1

def round_or_blank(x,n=3):
    return round(x,n) if x is not None and math.isfinite(x) else ''

def shock_context(b,side,entry):
    """Shock analysis uses only bars CLOSED by decision time.

    Distinguishes recent displacement from PRE-shock market context. We do not
    classify the 24h trend using candles from the same shock being examined.
    """
    n=len(b); last=n-1; current_atr=atr(b,last)
    # Last 6/12/24/36 bars (30m/1h/2h/3h). Use closed candle close.
    output={}
    for w in (6,12,24,36):
        anchor=n-1-w
        change=b[-1][4]-b[anchor][4]
        window=b[-w:]
        high=max(x[2] for x in window);low=min(x[3] for x in window)
        travel=sum(abs(b[i][4]-b[i-1][4]) for i in range(n-w,n))
        output['MOVE_%dB_ATR'%w]=round_or_blank(change/current_atr)
        output['MOVE_%dB_TRADE_ATR'%w]=round_or_blank(signed_direction(side)*change/current_atr)
        output['RANGE_%dB_ATR'%w]=round_or_blank((high-low)/current_atr)
        output['EFFICIENCY_%dB'%w]=round_or_blank(abs(change)/travel)
    # Pre-shock baseline: ends 12 completed candles before the decision,
    # and has 3h of local context. This is NOT the same as full 24h trend.
    baseline_end=n-13
    baseline_start=baseline_end-36
    baseline=b[baseline_start:baseline_end+1]
    baseline_change=baseline[-1][4]-baseline[0][4]
    baseline_atr=atr(b,baseline_end)
    baseline_high=max(x[2] for x in baseline)
    baseline_low=min(x[3] for x in baseline)
    recent=b[-12:]
    recent_high=max(x[2] for x in recent)
    recent_low=min(x[3] for x in recent)
    recent_change=b[-1][4]-b[n-13][4]
    pre_move=baseline_change/baseline_atr if baseline_atr else None
    shock_move=recent_change/baseline_atr if baseline_atr else None
    output.update({
        'PRE_SHOCK_3H_NET_ATR':round_or_blank(pre_move),
        'PRE_SHOCK_3H_DIRECTION':('UP' if pre_move is not None and pre_move>.75 else
                                  'DOWN' if pre_move is not None and pre_move<-.75 else 'MIXED'),
        'PRE_SHOCK_3H_HIGH':round(baseline_high,2),
        'PRE_SHOCK_3H_LOW':round(baseline_low,2),
        'PRE_SHOCK_ATR':round_or_blank(baseline_atr,2),
        'RECENT_1H_NET_PRE_ATR':round_or_blank(shock_move),
        'RECENT_1H_HIGH':round(recent_high,2),
        'RECENT_1H_LOW':round(recent_low,2),
        'RECENT_VS_PRE_3H_MOVE_RATIO':round_or_blank(abs(recent_change)/abs(baseline_change)),
        'SHOCK_1H_3ATR': 'YES' if shock_move is not None and abs(shock_move)>=3 else 'NO',
        'SHOCK_ALIGNED_TRADE': 'YES' if shock_move is not None and signed_direction(side)*shock_move>=3 else 'NO',
        'SHOCK_AGAINST_TRADE': 'YES' if shock_move is not None and signed_direction(side)*shock_move<=-3 else 'NO',
        'PRE_SHOCK_LAST_CLOSED_UTC':fmt_timestamp(b,baseline_end),
    })
    # The prior 24h high/low excluding the last 1h: structural context that
    # cannot be inflated by the sudden final-hour move.
    earlier=b[:-12]
    prior_high=max(x[2] for x in earlier)
    prior_low=min(x[3] for x in earlier)
    output['PRE_SHOCK_23H_HIGH']=round(prior_high,2)
    output['PRE_SHOCK_23H_LOW']=round(prior_low,2)
    output['ENTRY_ABOVE_PRE_SHOCK_HIGH_ATR']=round_or_blank((entry-prior_high)/current_atr)
    output['ENTRY_BELOW_PRE_SHOCK_LOW_ATR']=round_or_blank((prior_low-entry)/current_atr)
    # Earliest bar in last 1h at which the 1h cumulative displacement reached
    # 2 baseline ATR in the direction of the 1h move. This is NOT a pivot.
    shock_start=n-13
    if shock_move is not None and abs(shock_move)>=2:
        direction=1 if shock_move>0 else -1
        for k in range(n-12,n):
            if direction*(b[k][4]-b[shock_start][4])>=2*baseline_atr:
                output['SHOCK_2ATR_REACHED_UTC']=fmt_timestamp(b,k)
                output['BARS_AFTER_SHOCK_2ATR']=n-1-k
                break
    output.setdefault('SHOCK_2ATR_REACHED_UTC','')
    output.setdefault('BARS_AFTER_SHOCK_2ATR','')
    # Pre-shock structural trend from historical data up to 1h before entry.
    pre_b=b[:-12]
    pre_major=major_pivots(pivots(pre_b))
    output['PRE_SHOCK_STRUCTURE']=trend(pre_major,atr(pre_b,len(pre_b)-1))
    output['PRE_SHOCK_MAJOR_PIVOTS']=len(pre_major)
    # Continuation vs reversal: compare current net direction and pre-shock
    # direction, without claiming the shock represents established trend.
    output['SHOCK_CONTEXT']=(
        'SHOCK_AFTER_UNCLEAR_CONTEXT' if output['SHOCK_1H_3ATR']=='YES' and output['PRE_SHOCK_STRUCTURE'] in ('INSUFFICIENT','TRANSITION','EXPANDING_RANGE','CONTRACTING_RANGE','RANGE') else
        'SHOCK_AGAINST_PRE_TREND' if output['SHOCK_1H_3ATR']=='YES' and output['PRE_SHOCK_STRUCTURE'] in ('UP','DOWN') and (('UP' if shock_move>0 else 'DOWN')!=output['PRE_SHOCK_STRUCTURE']) else
        'SHOCK_WITH_PRE_TREND' if output['SHOCK_1H_3ATR']=='YES' else 'NO_3ATR_SHOCK')
    # Deceleration: compare last 3 closed candles with preceding 3; descriptive only.
    recent3=b[-1][4]-b[-4][4]
    previous3=b[-4][4]-b[-7][4]
    output['LAST_3B_MOVE_ATR']=round_or_blank(recent3/current_atr)
    output['PREV_3B_MOVE_ATR']=round_or_blank(previous3/current_atr)
    output['DIRECTIONAL_DECELERATION']=(
        'YES' if previous3*recent3>0 and abs(recent3)<.5*abs(previous3) else 'NO')
    return output

def audit_rule(rec):
    """Exploratory flag, not a proposed live filter.
    Require aligned 1h shock, proximity to opposing structure and
    weak/unclear PRE-shock context. Thresholds are hypotheses, not optimized.
    """
    if rec.get('ERROR'):return 'UNKNOWN'
    if rec.get('SHOCK_ALIGNED_TRADE')!='YES':return 'NO'
    distance=rec.get('OPPOSITE_LEVEL_ATR')
    if not isinstance(distance,(int,float)):return 'UNKNOWN'
    if distance>.75:return 'NO'
    return 'YES' if rec.get('PRE_SHOCK_STRUCTURE') in ('INSUFFICIENT','TRANSITION','EXPANDING_RANGE','CONTRACTING_RANGE','RANGE') else 'NO'

def main():
    parser=argparse.ArgumentParser(description='V8 read-only shock-aware BTC 5m historical audit')
    parser.add_argument('--db',default=DEFAULT_DB)
    parser.add_argument('--out',default=DEFAULT_OUT)
    args=parser.parse_args()
    uri=Path(args.db).resolve().as_uri()+'?mode=ro'
    con=sqlite3.connect(uri,uri=True)
    try:
        trades=con.execute("""SELECT signal_id,decision_ts,direction,entry FROM paper_trades_v4
                              WHERE signal_id BETWEEN 159 AND 179 ORDER BY signal_id""").fetchall()
    finally:con.close()
    if len(trades)!=21:
        print('WARNING: expected 21 records, got',len(trades))
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    records=[]
    print('BTC V8 READ ONLY | 288 closed Bybit 5m bars | pre-shock structure isolated')
    print('ID SIDE OUTCOME GROUP PRE_STRUCT SHOCK_1H_PREATR ALIGNED LEVEL_ATR CONTEXT FLAG')
    for sid,ts,side,entry in trades:
        side=str(side).upper()
        rec={'ID':sid,'SIDE':side,'ENTRY':entry,
             'DECISION_UTC':datetime.datetime.fromtimestamp(float(ts),datetime.timezone.utc).isoformat(),
             'HISTORICAL_OUTCOME':HISTORICAL_OUTCOME.get(sid,'UNKNOWN'),
             'PARENT_ID':PARENT_GROUP.get(sid,sid),
             'GROUP_ROLE':'PARENT' if PARENT_GROUP.get(sid,sid)==sid else 'OVERLAP_CHILD'}
        try:
            b=candles(float(ts))
            rec.update(analyze(b,side,float(entry)))
            rec.update(shock_context(b,side,float(entry)))
            rec['EXPLORATORY_LATE_SHOCK_FLAG']=audit_rule(rec)
            print(sid,side,rec['HISTORICAL_OUTCOME'],rec['GROUP_ROLE'],
                  rec['PRE_SHOCK_STRUCTURE'],rec['RECENT_1H_NET_PRE_ATR'],
                  rec['SHOCK_ALIGNED_TRADE'],rec['OPPOSITE_LEVEL_ATR'],
                  rec['SHOCK_CONTEXT'],rec['EXPLORATORY_LATE_SHOCK_FLAG'])
        except Exception as exc:
            rec['ERROR']=str(exc)
            print('ERROR',sid,str(exc))
        records.append(rec)
    fields=list(dict.fromkeys(k for r in records for k in r))
    def write_csv(name,subset):
        path=out/name
        with path.open('w',newline='',encoding='utf-8') as f:
            w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(subset)
        print('REPORT:',path)
    write_csv('structure_context_report.csv',records)
    write_csv('key_comparisons.csv',[r for r in records if r['ID'] in (159,164,166,174)])
    parents=[r for r in records if r.get('GROUP_ROLE')=='PARENT' and not r.get('ERROR')]
    write_csv('parent_signals_only.csv',parents)
    for name,subset in [('ALL_SIGNALS',[r for r in records if not r.get('ERROR')]),('PARENTS_ONLY',parents)]:
        flagged=[r for r in subset if r.get('EXPLORATORY_LATE_SHOCK_FLAG')=='YES']
        saved=sum(r['HISTORICAL_OUTCOME']=='SL' for r in flagged)
        missed=sum(r['HISTORICAL_OUTCOME']=='TP3' for r in flagged)
        print('FLAG_AUDIT',name,'FLAGGED',len(flagged),'SL_FLAGGED',saved,'TP3_FLAGGED',missed,
              'NOTE: counts are descriptive; overlap and outcome labels require independent verification')
    print('CAUTION: sudden moves do not establish prior trend; pivot/BOS/CHoCH are heuristic.')
    print('FINISHED | no orders, no DB writes, no service changes')

if __name__=='__main__':
    main()
