#!/usr/bin/env python3
"""V10 BTC read-only structural location audit. Never trades or writes to DB.
Uses V8's verified 288 *closed* Bybit candles, and V8 historical metadata.
Features computed without future candles or outcomes. Research only.
"""
import argparse
import csv
import importlib.util
import sqlite3
from pathlib import Path

DEFAULT_DB='/opt/tmco_webhook_v4/data/v4_paper_live.db'
DEFAULT_V8='/tmp/btc_v4_structure_context_v8.py'
DEFAULT_V8_CSV='/tmp/btc_v4_structure_context_v8/structure_context_report.csv'
DEFAULT_OUT='/tmp/btc_v4_structure_context_v10'

def load_v8(path):
    spec=importlib.util.spec_from_file_location('btc_v8_helpers',path)
    mod=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def num(x, digits=3):
    return round(x,digits) if x is not None else ''

def window(b,n):
    w=b[-n:]
    return max(x[2] for x in w),min(x[3] for x in w)

def features(v8,b,side,entry):
    """All lookbacks end at the final closed candle; no future confirmation."""
    if len(b)!=288: raise ValueError('Expected exactly 288 closed 5m bars')
    side=side.upper()
    if side not in ('LONG','SHORT'): raise ValueError('Unexpected direction '+side)
    direction=1 if side=='LONG' else -1
    a=v8.atr(b,len(b)-1)
    pre_a=v8.atr(b,len(b)-13)
    if not a or not pre_a: raise ValueError('Invalid ATR')
    last=b[-1][4]
    result={'ATR_NOW':num(a),'ATR_PRE_1H':num(pre_a),
            'ENTRY_VS_LAST_CLOSE_ATR':num((entry-last)/a)}
    for hours in (1,3,6):
        n=hours*12
        hi,lo=window(b,n)
        width=hi-lo
        result.update({
            f'RANGE_{hours}H_HIGH':num(hi,2),f'RANGE_{hours}H_LOW':num(lo,2),
            f'RANGE_{hours}H_POSITION_PCT':num(100*(entry-lo)/width,1) if width>0 else '',
            f'RANGE_{hours}H_TO_HIGH_ATR':num((hi-entry)/a),
            f'RANGE_{hours}H_TO_LOW_ATR':num((entry-lo)/a),
        })
    # Earlier 3h/6h reference zones, ending BEFORE the most recent 1h.
    for hours in (3,6):
        n=hours*12
        w=b[-(n+12):-12]
        hi=max(x[2] for x in w); lo=min(x[3] for x in w)
        result.update({
            f'PRIOR_{hours}H_HIGH':num(hi,2),f'PRIOR_{hours}H_LOW':num(lo,2),
            f'PRIOR_{hours}H_DIST_HIGH_ATR':num((hi-entry)/pre_a),
            f'PRIOR_{hours}H_DIST_LOW_ATR':num((entry-lo)/pre_a),
        })
    # Pivots are available only after confirmation; V8 enforces right=2.
    ps=v8.pivots(b)
    for typ,name in (('H','HIGH'),('L','LOW')):
        eligible=[p for p in ps if p['type']==typ and p['confirmed']<len(b)]
        p=eligible[-1] if eligible else None
        result[f'LAST_CONFIRMED_{name}']=num(p['price'],2) if p else ''
        result[f'LAST_CONFIRMED_{name}_AGE_BARS']=(len(b)-1-p['confirmed']) if p else ''
        result[f'DIST_LAST_{name}_ATR']=num(((p['price']-entry) if typ=='H' else (entry-p['price']))/a) if p else ''
    # Explicitly identify sequence: 30m shock vs 60m net move, and final 15m.
    def move(n):return direction*(b[-1][4]-b[-n-1][4])/pre_a
    result['MOVE_TRADE_15M_PREATR']=num(move(3))
    result['MOVE_TRADE_30M_PREATR']=num(move(6))
    result['MOVE_TRADE_60M_PREATR']=num(move(12))
    result['MOVE_TRADE_EARLIER_30M_PREATR']=num(direction*(b[-7][4]-b[-13][4])/pre_a)
    result['LAST_15M_VS_PREV_15M_PREATR']=num(direction*((b[-1][4]-b[-4][4])-(b[-4][4]-b[-7][4]))/pre_a)
    # Entry position relative to last hour, not assuming the exact entry equals candle close.
    hi,lo=window(b,12)
    result['ENTRY_TO_FAVORABLE_EXTREME_ATR']=num((hi-entry)/pre_a if side=='LONG' else (entry-lo)/pre_a)
    result['ENTRY_TO_ADVERSE_EXTREME_ATR']=num((entry-lo)/pre_a if side=='LONG' else (hi-entry)/pre_a)
    # Descriptive location labels only; no threshold tuned to historical outcome.
    p3=result['RANGE_3H_POSITION_PCT']
    p6=result['RANGE_6H_POSITION_PCT']
    result['EXTREME_ZONE_3H']=('UPPER_20' if p3>=80 else 'LOWER_20' if p3<=20 else 'MIDDLE') if p3!='' else 'UNKNOWN'
    result['EXTREME_ZONE_6H']=('UPPER_20' if p6>=80 else 'LOWER_20' if p6<=20 else 'MIDDLE') if p6!='' else 'UNKNOWN'
    result['DIRECTIONAL_EXTREME_3H']=('YES' if (side=='LONG' and p3>=80) or (side=='SHORT' and p3<=20) else 'NO') if p3!='' else 'UNKNOWN'
    return result

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--db',default=DEFAULT_DB)
    ap.add_argument('--v8',default=DEFAULT_V8)
    ap.add_argument('--v8-csv',default=DEFAULT_V8_CSV)
    ap.add_argument('--out',default=DEFAULT_OUT)
    args=ap.parse_args()
    v8=load_v8(args.v8)
    with open(args.v8_csv,newline='',encoding='utf-8') as f:
        metadata={int(r['ID']):r for r in csv.DictReader(f)}
    uri=Path(args.db).resolve().as_uri()+'?mode=ro'
    con=sqlite3.connect(uri,uri=True)
    try:
        trades=con.execute('SELECT signal_id,decision_ts,direction,entry FROM paper_trades_v4 WHERE signal_id BETWEEN 159 AND 179 ORDER BY signal_id').fetchall()
    finally:con.close()
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    rows=[]
    print('V10 READ ONLY | 288 completed 5m bars | structure, range position, shock sequence')
    print('ID SIDE OUTCOME ROLE POS3H POS6H PRIOR3H_HIGH_ATR PRIOR3H_LOW_ATR MOVE15 MOVE30 MOVE60 EXTREME ERROR')
    for sid,ts,side,entry in trades:
        m=metadata.get(sid,{})
        row={'ID':sid,'SIDE':str(side).upper(),'ENTRY':entry,
             'HISTORICAL_OUTCOME':m.get('HISTORICAL_OUTCOME','UNKNOWN'),
             'GROUP_ROLE':m.get('GROUP_ROLE','UNKNOWN'),
             'PRE_SHOCK_STRUCTURE':m.get('PRE_SHOCK_STRUCTURE','UNKNOWN'),
             'PARENT_ID':m.get('PARENT_ID','')}
        try:
            b=v8.candles(float(ts))
            row.update(features(v8,b,str(side),float(entry)))
            print(sid,row['SIDE'],row['HISTORICAL_OUTCOME'],row['GROUP_ROLE'],
                  row['RANGE_3H_POSITION_PCT'],row['RANGE_6H_POSITION_PCT'],
                  row['PRIOR_3H_DIST_HIGH_ATR'],row['PRIOR_3H_DIST_LOW_ATR'],
                  row['MOVE_TRADE_15M_PREATR'],row['MOVE_TRADE_30M_PREATR'],
                  row['MOVE_TRADE_60M_PREATR'],row['DIRECTIONAL_EXTREME_3H'],'-')
        except Exception as exc:
            row['ERROR']=str(exc)
            print(sid,'ERROR',str(exc))
        rows.append(row)
    fields=list(dict.fromkeys(k for row in rows for k in row))
    def save(name,items):
        path=out/name
        with path.open('w',newline='',encoding='utf-8') as f:
            w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(items)
        print('REPORT',path)
    save('all_signals.csv',rows)
    parents=[r for r in rows if r['GROUP_ROLE']=='PARENT' and not r.get('ERROR')]
    save('parent_signals.csv',parents)
    save('key_comparisons.csv',[r for r in rows if r['ID'] in (159,164,166,174,175,177,178,179)])
    print('PARENT_AUDIT',len(parents),'SL',sum(r['HISTORICAL_OUTCOME']=='SL' for r in parents),
          'TP3',sum(r['HISTORICAL_OUTCOME']=='TP3' for r in parents))
    print('NOTE: no predictive validation; outcome is displayed only, not used to calculate features.')
    print('NOTE: 3h/6h ranges are descriptive; confirmed pivots require 2 right-hand closed bars.')
    print('FINISHED | no orders, no DB writes, no service changes')

if __name__=='__main__':main()
