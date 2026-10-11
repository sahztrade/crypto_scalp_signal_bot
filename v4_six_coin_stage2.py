#!/usr/bin/env python3
"""Read-only six-coin Bybit linear futures OHLCV stage-2 scenario study.
No database access, orders, Telegram or live bot imports. Requires stage-1 CSV.
Entry proxy: next 1m OPEN at/after confirmation close. Excludes fees/slippage.
"""
import argparse, bisect, csv, math, time
from collections import defaultdict, Counter
from datetime import datetime, timezone
from pathlib import Path
import requests
from v4_six_coin_stage1 import retrieve, unrounded_sl, SYMBOLS, PERIODS, START, END
from v4_3tp_candle_engine import check_3tp_candle

URL='https://api.bybit.com/v5/market/kline'

def simulate(signal, minute, five, mode):
    t=int(signal['epoch']); side=signal['direction']
    times1=[x['timestamp'] for x in minute]; times5=[x['timestamp']+300 for x in five]
    opens5=[x['timestamp'] for x in five]
    idx=bisect.bisect_left(times1,t)
    if idx>=len(minute) or minute[idx]['timestamp']!=t:
        return dict(status='NO_ENTRY_CANDLE',r=None,exit_ts=None)
    entry=float(minute[idx]['open'])
    fi=bisect.bisect_right(times5,t)
    prior=five[max(0,fi-24):fi]
    if len(prior)<24:return dict(status='NO_5M_HISTORY',r=None,exit_ts=None)
    try: sl,_=unrounded_sl(side,prior,entry)
    except ValueError:return dict(status='INVALID_SL_AT_ENTRY',r=None,exit_ts=None)
    risk=(entry-sl) if side=='LONG' else (sl-entry)
    if risk<=0:return dict(status='INVALID_RISK',r=None,exit_ts=None)
    sign=1 if side=='LONG' else -1
    tp1=entry+sign*.75*risk; tp2=entry+sign*1.5*risk; tp3=entry+sign*2*risk
    current_sl=sl; hit1=hit2=0
    key='low' if side=='LONG' else 'high'
    anchors=[i for i,c in enumerate(prior) if abs(c[key]-sl)<=max(1e-10,abs(sl)*1e-12)]
    anchor_ts=prior[anchors[-1]]['timestamp'] if anchors else None
    for c in minute[idx:]:
        if c['timestamp']>=END:break
        # The entry is assumed filled at the open of the first complete post-decision candle.
        # Do not infer path within this candle; SL+new target => UNKNOWN.
        old_hit1=hit1
        next_sl,n1,n2,status=check_3tp_candle(c,side,entry,current_sl,tp1,tp2,tp3,hit1,hit2)
        if status=='unknown':return dict(status='UNKNOWN',r=None,exit_ts=c['timestamp']+60)
        if status=='sl':
            r=sign*(current_sl-entry)/risk
            return dict(status='SL',r=r,exit_ts=c['timestamp']+60)
        if status=='tp3':return dict(status='TP3',r=2.,exit_ts=c['timestamp']+60)
        if mode=='dynamic' and not old_hit1 and n1 and not n2:
            # Closed 5m bars ONLY at or before the 1m bar open, no lookahead.
            end5=bisect.bisect_right(times5,c['timestamp'])
            eligible=[]
            if anchor_ts is not None:
                # after the LAST original SL-setting candle, before TP1's 1m candle
                eligible=[x[key] for x in five[bisect.bisect_right(opens5,anchor_ts):end5]]
            if eligible:
                structural=min(eligible) if side=='LONG' else max(eligible)
                next_sl=max(current_sl,structural) if side=='LONG' else min(current_sl,structural)
            else:next_sl=current_sl
        current_sl,hit1,hit2=next_sl,n1,n2
    return dict(status='OPEN_AT_END',r=None,exit_ts=None)

def main():
    p=argparse.ArgumentParser();p.add_argument('--stage1',default='/tmp/v4_six_coin_stage1.csv');p.add_argument('--output',default='/tmp/v4_six_coin_stage2.csv');args=p.parse_args()
    if Path(args.output).resolve().is_relative_to(Path('/opt/tmco_webhook_v4').resolve()):p.error('Output must be outside active bot directory')
    with open(args.stage1,newline='') as f: rows=list(csv.DictReader(f))
    selected=defaultdict(list)
    for r in rows:
        if r['research_pass']=='1':selected[r['symbol']].append(r)
    session=requests.Session();results=[]
    print('READ ONLY | Bybit linear 1m/5m | historical scenario, not live fills',flush=True)
    print('ENTRY: next 1m open; SL recomputed at entry; monitor until 2026-10-11 00:00 UTC; no fees',flush=True)
    for symbol in SYMBOLS:
        minute=retrieve(session,symbol,'1m',START,END)
        five=retrieve(session,symbol,'5m',START-86400,END)
        print(f'{symbol}: fetched {len(minute)} 1m and {len(five)} 5m candles; signals {len(selected[symbol])}',flush=True)
        for row in selected[symbol]:
            for mode in ('current','dynamic'):
                result=simulate(row,minute,five,mode)
                results.append(dict(symbol=symbol,epoch=row['epoch'],side=row['direction'],mode=mode,**result))
    with open(args.output,'w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=['symbol','epoch','side','mode','status','r','exit_ts']);w.writeheader();w.writerows(results)
    print('SYMBOL MODE RESOLVED TP3 SL UNKNOWN OPEN INVALID SUM_R (resolved only)')
    for symbol in SYMBOLS:
        for mode in ('current','dynamic'):
            group=[x for x in results if x['symbol']==symbol and x['mode']==mode]
            ct=Counter(x['status'] for x in group)
            rs=[x['r'] for x in group if x['r'] is not None]
            print(symbol,mode,len(rs),ct['TP3'],ct['SL'],ct['UNKNOWN'],ct['OPEN_AT_END'],len(group)-len(rs)-ct['UNKNOWN']-ct['OPEN_AT_END'],round(sum(rs),4),flush=True)
    print('CSV:',args.output)
    print('CAUTION: Signals overlap; totals are NOT a portfolio return. Open/unknown excluded. No fees or slippage.')
    print('NO DB WRITES | NO BOT CHANGES | NO ORDERS | NO TELEGRAM')
if __name__=='__main__':main()
