#!/usr/bin/env python3
"""Read-only, causal 24h BTC wave-structure audit V5.

Builds connected swings, merges weak intermediate waves, then reports confirmed
structure, last break, and the currently unconfirmed leg separately. It does
NOT trade, modify services, or write databases. V4 remains unchanged.
"""
import argparse
import csv
import importlib.util
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

PARENT = Path(__file__).with_name('btc_v4_market_structure_24h_v4.py')
if not PARENT.exists():
    PARENT = Path('/tmp/btc_v4_market_structure_24h_v4.py')
spec = importlib.util.spec_from_file_location('btc_structure_base_v4', PARENT)
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)

def stamp(t):
    return datetime.fromtimestamp(t, timezone.utc).strftime('%m-%d %H:%M')

def simplify(pivots, bars, atrs, min_atr=1.8):
    """Causally merge shallow zigzags. Only confirmed pivots may be processed.

    A middle pivot is deleted when the following counter-swing is small;
    keep the more extreme same-type pivot. This is a visualization/structure
    simplifier, not an entry signal. No future data beyond current decision.
    """
    seq = []
    for p in pivots:
        p = dict(p)
        seq.append(p)
        while len(seq) >= 3:
            a, b, c = seq[-3:]
            if a['type'] != c['type']:
                break
            atr = max(atrs[c['confirm_i']], 1e-9)
            counter = abs(c['price'] - b['price'])
            # A small pullback between two same-side extremes is not a new main wave.
            if counter >= min_atr * atr:
                break
            if a['type'] == 'H':
                keep = c if c['price'] >= a['price'] else a
            else:
                keep = c if c['price'] <= a['price'] else a
            seq[-3:] = [keep]
    return seq

def classify(pivots, atr_now, tolerance):
    hs = [p for p in pivots if p['type']=='H'][-3:]
    ls = [p for p in pivots if p['type']=='L'][-3:]
    if len(hs)<2 or len(ls)<2:
        return 'INSUFFICIENT', hs, ls
    eps = tolerance * atr_now
    dh = hs[-1]['price']-hs[-2]['price']
    dl = ls[-1]['price']-ls[-2]['price']
    if dh>eps and dl>eps: state='UP'
    elif dh< -eps and dl< -eps: state='DOWN'
    elif abs(dh)<=eps and abs(dl)<=eps: state='RANGE'
    else: state='TRANSITION'
    return state, hs, ls

def latest_break(bars, pivots, lookback=48):
    """Last confirmed structural high/low crossed by a CLOSE, within last 4h.

    Historical break detection uses only pivots whose confirmation candle was
    already closed BEFORE the breaking candle. Prevents lookahead.
    """
    events=[]
    for i in range(max(1,len(bars)-lookback),len(bars)):
        prior = [p for p in pivots if p['confirm_i'] < i]
        highs = [p for p in prior if p['type']=='H']
        lows = [p for p in prior if p['type']=='L']
        if highs and bars[i-1]['close']<=highs[-1]['price']<bars[i]['close']:
            events.append(('UP', i, highs[-1]['price']))
        if lows and bars[i-1]['close']>=lows[-1]['price']>bars[i]['close']:
            events.append(('DOWN', i, lows[-1]['price']))
    return events[-1] if events else None

def active_leg(bars, pivots, atr_now):
    """Price displacement since last confirmed structural pivot; NOT a confirmed wave."""
    if not pivots: return 'UNKNOWN', 0.0
    last = pivots[-1]
    current = bars[-1]['close']
    displacement = (current-last['price'])/max(atr_now,1e-9)
    return ('UP' if displacement>0 else 'DOWN' if displacement<0 else 'FLAT'), displacement

def chart(bars, raw, waves, row, result, path):
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
    except ImportError:
        return 'matplotlib unavailable'
    fig, ax=plt.subplots(figsize=(18,8))
    for i,b in enumerate(bars):
        color='#119e90' if b['close']>=b['open'] else '#e45559'
        ax.vlines(i,b['low'],b['high'],color=color,lw=.65)
        ax.add_patch(Rectangle((i-.3,min(b['open'],b['close'])),.6,max(.05,abs(b['close']-b['open'])),color=color,lw=0))
    if raw:
        ax.plot([p['pivot_i'] for p in raw],[p['price'] for p in raw],color='#d2a376',lw=1,alpha=.45,label='Raw confirmed pivots')
    if waves:
        ax.plot([p['pivot_i'] for p in waves],[p['price'] for p in waves],color='#1764ce',lw=2.4,label='Connected major waves')
        for p in waves[-8:]:
            x=p['pivot_i']; y=p['price']
            ax.annotate(f"{p['type']} {y:,.0f}",(x,y),xytext=(0,10 if p['type']=='H' else -16),textcoords='offset points',ha='center',fontsize=8,color='#174ea6',fontweight='bold')
            ax.scatter([p['confirm_i']],[bars[p['confirm_i']]['close']],s=35,facecolors='none',edgecolors='#1764ce',zorder=5)
    if waves:
        p=waves[-1]
        ax.plot([p['pivot_i'],len(bars)-1],[p['price'],bars[-1]['close']],color='#f59e0b',lw=2,ls=':',label='Unconfirmed active leg')
    ax.axhline(row['entry'],color='green',ls=':',lw=1,label=f"Entry {row['entry']:,.1f}")
    ax.axvline(len(bars)-.5,color='gray',ls='--',lw=1)
    ax.set_xlim(-2,len(bars)+3)
    ticks=list(range(0,len(bars),24))+[len(bars)-1]
    ax.set_xticks(ticks)
    ax.set_xticklabels([stamp(bars[i]['timestamp']) for i in ticks],rotation=25,ha='right',fontsize=8)
    ax.set_title(f"#{row['signal_id']} {row['direction']} | Wave {result['wave_structure']} | Last break {result['last_break']} | Active leg {result['active_leg']}")
    ax.set_ylabel('BTCUSDT Bybit perpetual, USDT')
    ax.grid(alpha=.15); ax.legend(loc='upper left')
    fig.text(.5,.015,'Blue = connected CONFIRMED structural waves; orange = unconfirmed last leg; circles = confirmation bars. No future candles.',ha='center',fontsize=9)
    fig.tight_layout(rect=(0,.03,1,1)); fig.savefig(path,dpi=150); plt.close(fig)
    return str(path)

def main():
    ap=argparse.ArgumentParser(description='Read-only BTC 24h connected-wave audit V5')
    ap.add_argument('--db',type=Path,default=base.DB)
    ap.add_argument('--first',type=int,default=159)
    ap.add_argument('--last',type=int,default=179)
    ap.add_argument('--pivot-atr',type=float,default=0.9)
    ap.add_argument('--merge-atr',type=float,default=1.8)
    ap.add_argument('--tolerance-atr',type=float,default=0.35)
    ap.add_argument('--charts',default='159,164,166,174')
    ap.add_argument('--output',type=Path,default=Path('/tmp/btc_v4_structure_24h_v5'))
    args=ap.parse_args()
    if args.pivot_atr<=0 or args.merge_atr<=0 or args.tolerance_atr<0: ap.error('invalid ATR parameters')
    with sqlite3.connect('file:'+str(args.db)+'?mode=ro',uri=True) as db:
        rows=[dict(zip(('signal_id','direction','entry','decision_ts'),r)) for r in db.execute('SELECT signal_id,direction,entry,decision_ts FROM paper_trades_v4 WHERE signal_id BETWEEN ? AND ? ORDER BY signal_id',(args.first,args.last))]
    if not rows: raise RuntimeError('No trades found')
    opens=[int(r['decision_ts'])//base.FIVE*base.FIVE for r in rows]
    history=base.fetch_bybit(min(opens)-base.DAY-20*base.FIVE,max(opens))
    index={b['timestamp']:b for b in history}
    args.output.mkdir(parents=True,exist_ok=True)
    chart_ids={int(x) for x in args.charts.split(',') if x.strip()}
    reports=[]
    print('V5 READ ONLY | causal connected-wave reconstruction | Bybit BTCUSDT 5m')
    print(f'Pivot ATR={args.pivot_atr} | merge ATR={args.merge_atr} | tolerance ATR={args.tolerance_atr}')
    print('ID  SIDE  STRUCTURE     BREAK   LEG     LEG_ATR  WAVES  H(last 3)                 L(last 3)')
    for row,op in zip(rows,opens):
        times=range(op-base.DAY,op,base.FIVE)
        if any(t not in index for t in times):
            print(row['signal_id'],'MISSING CANDLES');continue
        bars=[index[t] for t in times]
        atrs=base.atr_series(bars)
        raw=base.swings(bars,atrs,args.pivot_atr)
        waves=simplify(raw,bars,atrs,args.merge_atr)
        structure,hs,ls=classify(waves,atrs[-1],args.tolerance_atr)
        event=latest_break(bars,waves)
        direction,leg_atr=active_leg(bars,waves,atrs[-1])
        br=event[0] if event else '-'
        result={'signal_id':row['signal_id'],'direction':row['direction'],'decision_utc':datetime.fromtimestamp(row['decision_ts'],timezone.utc).isoformat(),
                'entry':row['entry'],'wave_structure':structure,'last_break':br,'last_break_utc':stamp(bars[event[1]]['timestamp']+base.FIVE) if event else '',
                'last_break_level':round(event[2],2) if event else '', 'active_leg':direction,'active_leg_atr':round(leg_atr,2),
                'confirmed_waves':len(waves),'raw_pivots':len(raw),'highs':base.pivot_text(hs,bars),'lows':base.pivot_text(ls,bars),
                'last_wave_confirmation_utc':stamp(bars[waves[-1]['confirm_i']]['timestamp']+base.FIVE) if waves else '',
                'atr5':round(atrs[-1],2),'note':'Wave trend and break are separate; current leg unconfirmed'}
        reports.append(result)
        print(f"{row['signal_id']:3} {row['direction']:5} {structure:13} {br:6} {direction:6} {leg_atr:7.2f} {len(waves):6} {'/'.join(f'{p['price']:.0f}' for p in hs):25} {'/'.join(f'{p['price']:.0f}' for p in ls)}")
        if row['signal_id'] in chart_ids:
            print('  CHART:',chart(bars,raw,waves,row,result,args.output/f"btc_24h_{row['signal_id']}.png"))
    if reports:
        with (args.output/'structure_report.csv').open('w',newline='',encoding='utf-8') as f:
            writer=csv.DictWriter(f,fieldnames=list(reports[0]));writer.writeheader();writer.writerows(reports)
        print('CSV:',args.output/'structure_report.csv')
    print('FINISHED | READ ONLY | no orders, database writes or service changes')
if __name__=='__main__':main()
