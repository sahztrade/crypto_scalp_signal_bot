#!/usr/bin/env python3
"""Read-only stage 3: one active position per symbol and management mode, fee-adjusted R.
Uses stage-2 outcomes and independently re-downloads standard Bybit linear 1m/5m candles
for entry/risk reconstruction. No DB, order, Telegram or live bot writes.
"""
import argparse,bisect,csv,collections
from pathlib import Path
import requests
from v4_six_coin_stage1 import retrieve,unrounded_sl,SYMBOLS,START,END


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--stage2',default='/tmp/v4_six_coin_stage2.csv')
    ap.add_argument('--output',default='/tmp/v4_six_coin_stage3.csv')
    ap.add_argument('--taker-fee',type=float,default=0.00055,help='Per side fraction, default 0.00055 = 0.055%%')
    args=ap.parse_args()
    if not (0<=args.taker_fee<0.01):ap.error('Invalid fee')
    if Path(args.output).resolve().is_relative_to(Path('/opt/tmco_webhook_v4').resolve()):ap.error('Output must be outside bot directory')
    with open(args.stage2,newline='') as f: rows=list(csv.DictReader(f))
    by=collections.defaultdict(list)
    for r in rows:
        if r['symbol'] not in SYMBOLS or r['mode'] not in ('current','dynamic'):continue
        by[(r['symbol'],r['mode'])].append(r)
    output=[]; session=requests.Session()
    print('READ ONLY | 1 active position per symbol per mode | Bybit linear futures',flush=True)
    print('Fee each side:',args.taker_fee,'| no slippage | end:',END,flush=True)
    for symbol in SYMBOLS:
        one=retrieve(session,symbol,'1m',START,END)
        five=retrieve(session,symbol,'5m',START-86400,END)
        ts1=[c['timestamp'] for c in one]
        closed5=[c['timestamp']+300 for c in five]
        for mode in ('current','dynamic'):
            busy_until=START; counts=collections.Counter(); net_sum=0.;gross_sum=0.;fee_sum=0.
            ordered=sorted(by[(symbol,mode)],key=lambda r:int(r['epoch']))
            for row in ordered:
                t=int(row['epoch']);status=row['status']
                if t<busy_until:
                    counts['SKIPPED_OVERLAP']+=1
                    output.append(dict(**row,selection='SKIPPED_OVERLAP',gross_r='',fee_r='',net_r=''))
                    continue
                # A rejected/unfilled hypothetical position does not block future signals.
                if status in ('INVALID_SL_AT_ENTRY','INVALID_RISK','NO_ENTRY_CANDLE','NO_5M_HISTORY'):
                    counts['INVALID']+=1
                    output.append(dict(**row,selection='INVALID',gross_r='',fee_r='',net_r=''))
                    continue
                # Unknown is conservatively blocked until its ambiguity candle; open until END.
                exit_ts=int(row['exit_ts']) if row['exit_ts'] else END
                busy_until=max(t+60,exit_ts)
                if status not in ('SL','TP3'):
                    counts[status]+=1
                    output.append(dict(**row,selection='ACCEPTED_'+status,gross_r='',fee_r='',net_r=''))
                    continue
                i=bisect.bisect_left(ts1,t)
                j=bisect.bisect_right(closed5,t)
                if i>=len(one) or ts1[i]!=t or j<24:
                    raise RuntimeError(f'Cannot reconstruct {symbol} {t}')
                entry=one[i]['open'];sl,_=unrounded_sl(row['side'],five[j-24:j],entry)
                risk=abs(entry-sl)
                if risk<=0:raise RuntimeError(f'Invalid reconstructed risk {symbol} {t}')
                gross=float(row['r'])
                # SL exit price reconstructed from reported realized R, TP3 likewise.
                side=1 if row['side']=='LONG' else -1
                exit_price=entry+side*gross*risk
                if exit_price<=0:raise RuntimeError(f'Nonpositive exit price {symbol} {t}')
                fee_r=args.taker_fee*(entry+exit_price)/risk
                net=gross-fee_r
                gross_sum+=gross;fee_sum+=fee_r;net_sum+=net;counts[status]+=1
                output.append(dict(**row,selection='ACCEPTED_RESOLVED',gross_r=f'{gross:.8f}',fee_r=f'{fee_r:.8f}',net_r=f'{net:.8f}'))
            print(symbol,mode,'accepted_resolved',counts['SL']+counts['TP3'], 'TP3',counts['TP3'],'SL',counts['SL'], 'unknown',counts['UNKNOWN'],'open',counts['OPEN_AT_END'],'overlap_skipped',counts['SKIPPED_OVERLAP'],'gross_R',round(gross_sum,3),'fee_R',round(fee_sum,3),'net_R',round(net_sum,3),flush=True)
    fields=['symbol','epoch','side','mode','status','r','exit_ts','selection','gross_r','fee_r','net_r']
    with open(args.output,'w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(output)
    print('CSV:',args.output)
    print('CAVEATS: No actual fills, funding, slippage or latency. UNKNOWN/OPEN excluded from R. Fees hypothetical. Each mode independently sequential.')
    print('NO DB WRITES | NO BOT CHANGES | NO ORDERS | NO TELEGRAM')
if __name__=='__main__':main()
