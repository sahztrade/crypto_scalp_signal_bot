#!/usr/bin/env python3
"""Independent read-only Bybit linear standard-candle six-symbol TMCO V4 stage-1 study.
No database, Telegram, trading or bot modifications. Python requests required.
"""
import argparse
import bisect
import csv
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import requests

from tmco_2244_backtest import crossings
from market_filters_v4_test import evaluate
from structural_sl_v4 import calculate_structural_sl
from b3_short_filter_test import b3_reject_short

URL = 'https://api.bybit.com/v5/market/kline'
SYMBOLS = ('BTCUSDT','ADAUSDT','SUIUSDT','AVAXUSDT','DOGEUSDT','SOLUSDT')
PERIODS = {'1m':60,'3m':180,'5m':300}
START = int(datetime(2026,10,4,tzinfo=timezone.utc).timestamp())
END = int(datetime(2026,10,11,tzinfo=timezone.utc).timestamp())
WARMUP = 24*3600


def retrieve(session, symbol, tf, begin, end):
    seconds = PERIODS[tf]
    # end is exclusive; request closed candles with OPEN timestamps before end.
    cursor = end - 1
    collected = {}
    calls = 0
    while cursor >= begin:
        params = dict(category='linear',symbol=symbol,interval=str(seconds//60),
                      end=cursor*1000,limit=1000)
        last_error = None
        for attempt in range(5):
            try:
                resp = session.get(URL,params=params,timeout=25)
                resp.raise_for_status()
                payload = resp.json()
                if payload.get('retCode') != 0:
                    raise RuntimeError(str(payload.get('retMsg')))
                rows = payload['result']['list']
                break
            except (requests.RequestException, ValueError, KeyError, RuntimeError) as exc:
                last_error = exc
                if attempt == 4: raise RuntimeError(f'{symbol} {tf} download: {exc}')
                time.sleep(1.5*(attempt+1))
        if not rows:
            raise RuntimeError(f'{symbol} {tf}: empty response near {cursor}')
        oldest = min(int(r[0])//1000 for r in rows)
        if oldest > cursor:
            raise RuntimeError(f'{symbol} {tf}: invalid paging')
        for r in rows:
            ts = int(r[0])//1000
            if begin <= ts and ts+seconds <= end and ts%seconds==0:
                collected[ts] = dict(timestamp=ts,open=float(r[1]),high=float(r[2]),
                                     low=float(r[3]),close=float(r[4]),volume=float(r[5]))
        if oldest <= begin: break
        cursor = oldest - 1
        calls += 1
        if calls > 100: raise RuntimeError('paging limit exceeded')
        time.sleep(.08)
    result = [collected[t] for t in sorted(collected)]
    expected = list(range(begin,end,seconds))
    if [x['timestamp'] for x in result] != expected:
        missing = sorted(set(expected)-set(collected))
        raise RuntimeError(f'{symbol} {tf}: missing/misaligned {len(missing)} candles; first={missing[:3]}')
    return result


def confirmations(events):
    # Historical proxy of confirmation_engine: ordered by close time; each cross consumed once.
    # Does not claim to reproduce network arrival order, lateness or concurrent event handling.
    events = sorted(events,key=lambda x:(x['time'],int(x['tf'][:-1])))
    used = set()
    confirmed = []
    for i,a in enumerate(events):
        if i in used: continue
        for j in range(i+1,len(events)):
            if j in used: continue
            b = events[j]
            if b['time']-a['time'] > 600: break
            if a['direction'] != b['direction'] or a['tf']==b['tf']: continue
            if b['time'] < a['time']: continue
            used.update((i,j))
            confirmed.append(dict(epoch=b['time'],direction=b['direction'],tf1=a['tf'],tf2=b['tf']))
            break
    return confirmed


def evaluate_symbol(symbol, candles):
    times={tf:[c['timestamp']+PERIODS[tf] for c in arr] for tf,arr in candles.items()}
    events=[]
    for tf,arr in candles.items():
        events.extend(e for e in crossings(arr,tf) if START<=e['time']<END)
    confirms=confirmations(events)
    stats=Counter()
    stats['crosses']=len(events)
    stats['confirmations']=len(confirms)
    records=[]
    for event in confirms:
        t=event['epoch']; side=event['direction']
        # strict <= decision time, never future candles
        chunks={}
        for tf,n in [('1m',240),('3m',80),('5m',100)]:
            index=bisect.bisect_right(times[tf],t)
            chunks[tf]=candles[tf][max(0,index-n):index]
        record=dict(symbol=symbol,epoch=t,utc=datetime.fromtimestamp(t,timezone.utc).isoformat(),
                    direction=side,tf1=event['tf1'],tf2=event['tf2'],score='',
                    score_pass=0,sl_valid=0,b3_reject='',research_pass=0,
                    sl='',sl_method='',reason='')
        if any(len(chunks[tf])<n for tf,n in [('1m',240),('3m',80),('5m',100)]):
            record['reason']='INSUFFICIENT_WARMUP';stats['warmup_missing']+=1
            records.append(record);continue
        try:
            result=evaluate(side,chunks['1m'],chunks['3m'],chunks['5m'])
            record['score']=result['score']
            if result['score']<4:
                record['reason']='SCORE_LT_4';stats['score_rejected']+=1
                records.append(record);continue
            record['score_pass']=1;stats['score_pass']+=1
            # Use last CLOSED 1m close as research reference; no real fill/latency assumption.
            proxy=chunks['1m'][-1]['close']
            # Original structural SL returns round(stop,2). For sub-dollar assets,
            # reimplement same pivot selection without rounding, strictly for stage-1 validity.
            stop,method=unrounded_sl(side,chunks['5m'],proxy)
            record['sl']=format(stop,'.12g');record['sl_method']=method
            record['sl_valid']=1;stats['sl_valid']+=1
            if side=='SHORT':
                b3=b3_reject_short(chunks['5m'])
                record['b3_reject']=int(b3['reject'])
                if b3['reject']:
                    record['reason']='B3_REJECT';stats['b3_rejected']+=1
                    records.append(record);continue
            record['research_pass']=1;record['reason']='RESEARCH_PASS'
            stats['research_pass']+=1
        except Exception as exc:
            record['reason']='ERROR:'+str(exc)[:100]
            stats['errors']+=1
        records.append(record)
    return stats,records


def unrounded_sl(side,candles,entry):
    values=[float(c['low' if side=='LONG' else 'high']) for c in candles[-24:]]
    if side=='LONG':
        pivots=[values[i] for i in range(1,len(values)-1) if values[i]<values[i-1] and values[i]<values[i+1]]
    else:
        pivots=[values[i] for i in range(1,len(values)-1) if values[i]>values[i-1] and values[i]>values[i+1]]
    if not pivots:raise ValueError('NO_FIRST_PIVOT')
    first=pivots[-1]
    if side=='LONG':
        second=next((x for x in reversed(pivots[:-1]) if x<first),None)
        extreme=min(values);valid=lambda x:x<first and x<entry
    else:
        second=next((x for x in reversed(pivots[:-1]) if x>first),None)
        extreme=max(values);valid=lambda x:x>first and x>entry
    if second is not None and valid(second):stop,method=second,'SECOND_SWING'
    elif valid(extreme):stop,method=extreme,'WINDOW_EXTREME'
    else:stop,method=first,'FIRST_SWING_FALLBACK'
    if not ((stop<entry) if side=='LONG' else (stop>entry)):
        raise ValueError('NO_VALID_STRUCTURAL_SL')
    return stop,method


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',default='/tmp/v4_six_coin_stage1.csv')
    args=parser.parse_args()
    session=requests.Session()
    print('READ ONLY | Bybit linear standard OHLCV | UTC 2026-10-04 to 2026-10-11 (exclusive)',flush=True)
    print('Historical confirmation proxy; actual alert latency and entry fills NOT simulated',flush=True)
    summaries=[];all_records=[]
    for symbol in SYMBOLS:
        try:
            data={}
            for tf,seconds in PERIODS.items():
                begin=START-WARMUP
                begin=(begin//seconds)*seconds
                data[tf]=retrieve(session,symbol,tf,begin,END)
                print(f'{symbol} {tf}: {len(data[tf])} complete standard futures candles',flush=True)
            stats,records=evaluate_symbol(symbol,data)
            summaries.append((symbol,stats))
            all_records.extend(records)
            print(f'{symbol}: crosses={stats["crosses"]} confirms={stats["confirmations"]} score>=4={stats["score_pass"]} B3reject={stats["b3_rejected"]} research_pass={stats["research_pass"]} errors={stats["errors"]}',flush=True)
        except Exception as exc:
            print(f'{symbol}: FAILED {exc}',flush=True)
            summaries.append((symbol,None))
    if all_records:
        output=Path(args.output)
        if output.resolve().is_relative_to(Path('/opt/tmco_webhook_v4').resolve()):
            raise ValueError('Output must be outside active bot directory')
        with output.open('w',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=list(all_records[0]))
            writer.writeheader();writer.writerows(all_records)
        print('CSV:',output,flush=True)
    print('SUMMARY | SYMBOL | CROSSES | CONFIRMED | SCORE>=4 | B3_REJECT | RESEARCH_PASS | ERRORS')
    for symbol,stats in summaries:
        print(symbol,*(stats[k] for k in ('crosses','confirmations','score_pass','b3_rejected','research_pass','errors')) if stats else ('FAILED',))
    print('NO DB WRITES | NO BOT CHANGES | NO ORDERS | NO TELEGRAM')

if __name__=='__main__':main()
