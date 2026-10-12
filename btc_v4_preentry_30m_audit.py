#!/usr/bin/env python3
"""Read-only, no-lookahead 30-minute pre-entry BTC V4 audit (signals 159-179)."""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, '/mnt/data')
# On server, the existing structure audit is in /tmp.
sys.path.insert(0, '/tmp')
from btc_v4_structure_audit import klines, atr, ema, OUTCOMES

DB = '/opt/tmco_webhook_v4/data/v4_paper_live.db'

def metrics(candles, side, entry):
    # Exactly six COMPLETED 5m bars before the decision.
    recent = candles[-6:]
    prev = candles[-7]
    start = prev['c']
    end = recent[-1]['c']
    a = atr(candles)
    if not a or a <= 0:
        raise ValueError('ATR unavailable')
    high = max(x['h'] for x in recent)
    low = min(x['l'] for x in recent)
    up = sum(x['c'] > x['o'] for x in recent)
    down = sum(x['c'] < x['o'] for x in recent)
    net = (end-start)/a
    path = sum(abs(x['c']-x['o']) for x in recent)/a
    rng = (high-low)/a
    loc = 100*(entry-low)/(high-low) if high>low else 50
    # Direction-aware movement in the preceding 30 minutes.
    signed_net = net if side == 'LONG' else -net
    signed_loc = loc if side == 'LONG' else 100-loc
    # Largest opposing candle-body retracement (ATR) within 30m.
    adverse_body = max((max(0, x['o']-x['c']) if side=='LONG' else max(0, x['c']-x['o'])) for x in recent)/a
    # 5m-close-to-close maximum counter-directional movement from any earlier close.
    closes = [start]+[x['c'] for x in recent]
    counter = max((closes[i]-closes[j] if side=='LONG' else closes[j]-closes[i])
                  for i in range(len(closes)) for j in range(i+1,len(closes)))/a
    # Distance from entry to most recent 5m closed close (slippage/context, not a trading fill).
    entry_gap = (entry-end)/a
    return signed_net, signed_loc, rng, path, adverse_body, counter, up, down, entry_gap

def main():
    con=sqlite3.connect(f'file:{DB}?mode=ro', uri=True)
    rows=con.execute('SELECT signal_id,direction,entry,decision_ts FROM paper_trades_v4 WHERE signal_id BETWEEN 159 AND 179 ORDER BY signal_id').fetchall()
    con.close()
    if not rows: raise RuntimeError('No signals 159-179 found')
    start=int(min(x[3] for x in rows))-36*3600
    end=int(max(x[3] for x in rows))+60
    five=klines(5,start,end)
    if not five or len(five)>=1000: raise RuntimeError('Historical 5m data missing or truncated')
    print('READ ONLY | completed 5m bars only | pre-entry 30m | ATR14 on 5m')
    print('SIGNED_NET: + means 30m move WITH trade; LOC_DIR: 100=at direction extreme')
    print('COUNTER: largest against-trade close-to-close pullback within prior 30m, in ATR')
    print('ID  SIDE  RES  NET_ATR LOC_DIR RANGE_ATR PATH_ATR COUNTER_ATR OPP_BODY UP DN GAP_ATR')
    for sid,side,entry,ts in rows:
        decision=int(ts)
        cs=[c for c in five if c['t']+300<=decision]
        if len(cs)<25:
            print(f'{sid} INSUFFICIENT_CANDLES {len(cs)}'); continue
        try:
            net,loc,rng,path,body,counter,up,down,gap=metrics(cs,side,entry)
            print(f'{sid:<3} {side:<5} {OUTCOMES[sid]:<4} {net:7.2f} {loc:7.1f}% {rng:9.2f} {path:8.2f} {counter:11.2f} {body:8.2f} {up:2d} {down:2d} {gap:7.2f}')
        except Exception as exc:
            print(f'{sid} ERROR {exc}')

if __name__=='__main__': main()
