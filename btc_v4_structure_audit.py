#!/usr/bin/env python3
"""Read-only BTC V4 entry structure audit. No future candles in features."""
import json, sqlite3, time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DB = '/opt/tmco_webhook_v4/data/v4_paper_live.db'
OUTCOMES = {i: ('TP3' if i in [159,160,161,162,163,166,167,168,169,170,171,172,173] else 'SL') for i in range(159,180)}

def klines(tf, start, end):
    args = urlencode(dict(category='linear',symbol='BTCUSDT',interval=str(tf),start=int(start*1000),end=int(end*1000),limit=1000))
    req = Request('https://api.bybit.com/v5/market/kline?'+args, headers={'User-Agent':'BTC-V4-Structure-Audit/1.0'})
    with urlopen(req,timeout=20) as f: data=json.load(f)
    if data.get('retCode') != 0: raise RuntimeError(f'Bybit {tf}m: {data.get("retMsg")}')
    result=[]
    for r in data['result']['list']:
        result.append(dict(t=int(r[0])//1000,o=float(r[1]),h=float(r[2]),l=float(r[3]),c=float(r[4])))
    return sorted(result,key=lambda x:x['t'])

def ema(xs,n):
    if len(xs)<n: return None
    v=sum(xs[:n])/n
    for x in xs[n:]: v += (x-v)*2/(n+1)
    return v

def atr(cs,n=14):
    if len(cs)<n+1: return None
    a=[]
    for i in range(1,len(cs)):
        c,p=cs[i],cs[i-1]['c']
        a.append(max(c['h']-c['l'],abs(c['h']-p),abs(c['l']-p)))
    return sum(a[-n:])/n

def pivots(cs):
    # Pivot at index i is only confirmed once candles i+1 and i+2 have closed.
    highs=[]; lows=[]
    for i in range(2,len(cs)-2):
        h,l=cs[i]['h'],cs[i]['l']
        if all(h>cs[j]['h'] for j in (i-2,i-1,i+1,i+2)): highs.append((i,h))
        if all(l<cs[j]['l'] for j in (i-2,i-1,i+1,i+2)): lows.append((i,l))
    return highs,lows

def structure(cs):
    highs,lows=pivots(cs)
    if len(highs)<2 or len(lows)<2: return 'UNKNOWN','-',len(highs),len(lows)
    hh=highs[-1][1]>highs[-2][1]
    hl=lows[-1][1]>lows[-2][1]
    lh=highs[-1][1]<highs[-2][1]
    ll=lows[-1][1]<lows[-2][1]
    state='UP' if hh and hl else 'DOWN' if lh and ll else 'MIXED'
    # BOS requires most recent candle close to cross a previously confirmed pivot.
    previous=cs[:-1]
    ph,pl=pivots(previous)
    bos='-'
    if ph and previous[-1]['c']<=ph[-1][1] and cs[-1]['c']>ph[-1][1]: bos='UP'
    if pl and previous[-1]['c']>=pl[-1][1] and cs[-1]['c']<pl[-1][1]: bos='DOWN' if bos=='-' else 'BOTH'
    return state,bos,len(highs),len(lows)

def analyze(cs,entry):
    if len(cs)<60: return None
    state,bos,nh,nl=structure(cs)
    a=atr(cs); e=ema([c['c'] for c in cs],20)
    recent=cs[-24:]
    lo=min(c['l'] for c in recent); hi=max(c['h'] for c in recent)
    loc=100*(entry-lo)/(hi-lo) if hi>lo else 50
    dist=(entry-e)/a if a else 0
    return state,bos,loc,dist,nh,nl

def main():
    con=sqlite3.connect(f'file:{DB}?mode=ro',uri=True)
    rows=con.execute('SELECT signal_id,direction,entry,decision_ts FROM paper_trades_v4 WHERE signal_id BETWEEN 159 AND 179 ORDER BY signal_id').fetchall()
    con.close()
    if not rows: raise RuntimeError('No trades found')
    # 36 hours history before earliest decision; all 21 signals in a compact window.
    start=min(r[3] for r in rows)-36*3600
    end=max(r[3] for r in rows)+60
    five=klines(5,start,end)
    fifteen=klines(15,start,end)
    if not five or not fifteen: raise RuntimeError('Empty Bybit response')
    if len(five)>=1000 or len(fifteen)>=1000: raise RuntimeError('Kline pagination required; stopped rather than use truncated data')
    print('DATA: 5m=',len(five),'15m=',len(fifteen),'(historical Bybit futures)')
    print('Pivot rule: 2 closed candles on each side; no future candles')
    print('BOS: last close crosses prior confirmed swing; - means no new break on last candle')
    print('OUTCOME: prior fixed-SL simulation; #176 and #179 marked SL per user')
    print('ID   SIDE RESULT  5M   15M  BOS5 BOS15 LOC%  EMA_ATR5 EMA_ATR15')
    for sid,side,entry,decision in rows:
        d=int(decision)
        c5=[c for c in five if c['t']+300<=d]
        c15=[c for c in fifteen if c['t']+900<=d]
        x=analyze(c5,entry); y=analyze(c15,entry)
        if x is None or y is None:
            print(f'{sid:<4} INSUFFICIENT_CANDLES 5m={len(c5)} 15m={len(c15)}')
            continue
        print(f'{sid:<4} {side:<5} {OUTCOMES.get(sid,"?"):<6} {x[0]:<5} {y[0]:<5} {x[1]:<4} {y[1]:<5} {x[2]:5.1f} {x[3]:9.2f} {y[3]:9.2f}')

if __name__=='__main__': main()
