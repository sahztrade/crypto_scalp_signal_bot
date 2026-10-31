#!/usr/bin/env python3
"""Independent causal 5m range detector V2 and frozen-history scalp audit.
No network, Telegram, service edits or orders. Closed bars only.
"""
import argparse
import bisect
import collections
from contextlib import closing
import csv
import datetime as dt
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import sqlite3
import statistics

VERSION='BTC-5M-LOCAL-RANGE-V2'
OLD_SHA='f30f5c793c265d7a645be4d1cdc003a46799de5dd03bd1983f8fa05ac8ce527f'
HELPER_SHA='f8c4c7525411025b39a9d08c5a0e9bc035def32cb132d4e7ff7d5a8b2928d51d'
POLICY=dict(windows=[12,18,24],max_width_atr=4.,max_er=.35,max_half_drift=.25,
            min_center_crosses=3,touch_band=.2,confirm_range_bars=2,
            breakout_buffer_atr=.15,confirm_break_bars=2,
            stale_bars=6,max_box_bars=24,rearm_bars=4,
            trend_bars=6,trend_er=.60,trend_move_atr=1.)
IRAN=dt.timezone(dt.timedelta(hours=3,minutes=30))


def stamp(t):return dt.datetime.fromtimestamp(t,IRAN).isoformat()


def quantile(values,q):
    values=sorted(values)
    return values[int((len(values)-1)*q)]


def efficiency(closes):
    path=sum(abs(b-a) for a,b in zip(closes,closes[1:]))
    return abs(closes[-1]-closes[0])/path if path else 0.


def overlap(a,b,tick):
    union=max(a['resistance'],b['resistance'])-min(a['support'],b['support'])
    if union<=tick:return 1.
    intersection=max(0.,min(a['resistance'],b['resistance'])-max(a['support'],b['support']))
    return intersection/union


class Range5m:
    def __init__(self,tick=.1,saved=None):
        self.tick=float(tick)
        if self.tick<=0:raise ValueError('INVALID_TICK')
        self.bars=[];self.atr=None;self.trs=[]
        self.regime='UNCERTAIN';self.direction='FLAT';self.box=None
        self.candidate=None;self.counter=0;self.counter_side=None
        self.last_exit=None;self.last_box=None;self.last_at=None
        self.reason='WARMUP';self.events=[];self.metrics={}
        if saved is not None:
            if saved.get('version')!=VERSION or saved.get('policy')!=POLICY:
                raise ValueError('INCOMPATIBLE_RANGE_STATE')
            state=json.loads(json.dumps(saved['state']))
            self.__dict__.update(state)
            if self.tick!=tick:raise ValueError('TICK_STATE_MISMATCH')

    def dump(self):
        state={k:v for k,v in self.__dict__.items() if k!='events'}
        return dict(version=VERSION,policy=POLICY,state=json.loads(json.dumps(state)))

    def transition(self,target,at,reason):
        old=self.regime
        self.regime=target;self.reason=reason
        self.events.append(dict(at=at,from_regime=old,to=target,reason=reason))

    def trend(self):
        seq=self.bars[-POLICY['trend_bars']:]
        er=efficiency([b[4] for b in seq])
        move=(seq[-1][4]-seq[0][4])/max(self.atr,self.tick)
        sign='UP' if move>0 else 'DOWN' if move<0 else 'FLAT'
        return dict(er=er,move_atr=move,direction=sign,
                    strong=er>=POLICY['trend_er'] and abs(move)>=POLICY['trend_move_atr'])

    def find_range(self):
        atr=max(self.atr,self.tick)
        if self.trend()['strong']:return None
        for n in POLICY['windows']:
            h=self.bars[-n:]
            if len(h)<n:continue
            low=quantile([b[3] for b in h],.10)
            high=quantile([b[2] for b in h],.90)
            width=high-low
            if width>POLICY['max_width_atr']*atr:continue
            closes=[b[4] for b in h]
            er=efficiency(closes)
            drift=abs(statistics.median(closes[:n//2])-statistics.median(closes[n//2:]))/max(width,atr)
            if er>POLICY['max_er'] or drift>POLICY['max_half_drift']:continue
            if not low<=closes[-1]<=high:continue
            quiet=max(closes)-min(closes)<=.2*atr
            if not quiet:
                mid=(low+high)/2
                sides=[1 if c>mid else -1 if c<mid else 0 for c in closes]
                sides=[s for s in sides if s]
                crosses=sum(a!=b for a,b in zip(sides,sides[1:]))
                if crosses<POLICY['min_center_crosses']:continue
                if not all(any(b[3]<=low+POLICY['touch_band']*width for b in part) and
                           any(b[2]>=high-POLICY['touch_band']*width for b in part)
                           for part in (h[:n//2],h[n//2:])):continue
            return dict(support=low,resistance=high,buffer=max(self.tick,.15*atr),window=n,
                        width_atr=width/atr,er=er,half_drift=drift,quiet_closes=quiet)
        return None

    def on_closed_bar(self,bar,cutoff):
        if len(bar)!=6 or not all(math.isfinite(float(x)) for x in bar):
            raise ValueError('INVALID_BAR')
        b=list(bar);t=int(b[0]);at=t+300
        if not math.isfinite(cutoff) or t!=b[0] or t%300 or at>cutoff:
            raise ValueError('OPEN_OR_UNALIGNED_BAR')
        if not 0<b[3]<=min(b[1],b[4])<=max(b[1],b[4])<=b[2] or b[5]<0:
            raise ValueError('INVALID_OHLCV')
        if self.bars and t==self.bars[-1][0]:
            if b!=self.bars[-1]:raise ValueError('REVISED_CLOSED_BAR')
            return self.view()
        if self.bars and t!=self.bars[-1][0]+300:raise ValueError('CANDLE_GAP_OR_REORDER')
        previous=self.bars[-1][4] if self.bars else b[1]
        tr=max(b[2]-b[3],abs(b[2]-previous),abs(b[3]-previous))
        self.trs=(self.trs+[tr])[-14:]
        if self.atr is None and len(self.trs)==14:self.atr=sum(self.trs)/14
        elif self.atr is not None:self.atr=(13*self.atr+tr)/14
        self.bars=(self.bars+[b])[-288:]
        self.last_at=at
        if len(self.bars)<30:return self.view()
        move=(b[4]-self.bars[-4][4])/max(self.atr,self.tick)
        self.direction='UP' if move>.25 else 'DOWN' if move<-.25 else 'FLAT'
        trend=self.trend()
        self.metrics=dict(atr=self.atr,local_er=trend['er'],local_move_atr=trend['move_atr'])
        fresh=self.find_range()
        # Frozen box: no expansion, no refresh from historic bounds after a breakout.
        if self.regime=='RANGE' and self.box is not None:
            box=self.box
            target='UP' if b[4]>box['resistance']+box['buffer'] else 'DOWN' if b[4]<box['support']-box['buffer'] else None
            self.counter=self.counter+1 if target and target==self.counter_side else 1 if target else 0
            self.counter_side=target
            if self.counter>=POLICY['confirm_break_bars']:
                self.last_box=dict(box);self.last_exit=at
                self.box=None;self.candidate=None;self.counter=0;self.counter_side=None
                self.transition('NON_RANGE',at,'TWO_CLOSED_5M_BEYOND_FROZEN_BOX_'+target)
                return self.view()
            if fresh and overlap(box,fresh,self.tick)>=.65 and box['support']<=b[4]<=box['resistance']:
                box['last_validated_at']=at
                box['fresh_confirmations']=box.get('fresh_confirmations',0)+1
            else:
                box['fresh_confirmations']=0
            stale=(at-box['last_validated_at'])//300>=POLICY['stale_bars']
            old=(at-box['formed_at'])//300>=POLICY['max_box_bars']
            # One outside close is pending confirmation, not an automatic escape via expiry.
            pending_grace_over=(at-box['formed_at'])//300>=POLICY['max_box_bars']+1 or (at-box['last_validated_at'])//300>=POLICY['stale_bars']+1
            if (stale or old) and (self.counter==0 or pending_grace_over):
                if old and fresh and box.get('fresh_confirmations',0)>=2:
                    self.box=dict(fresh,formed_at=at,last_validated_at=at,fresh_confirmations=0)
                    self.events.append(dict(at=at,from_regime='RANGE',to='RANGE',reason='REBUILT_FROM_CURRENT_LOCAL_BARS'))
                    self.reason='REBUILT_FROM_CURRENT_LOCAL_BARS'
                else:
                    self.box=None;self.candidate=None
                    self.transition('UNCERTAIN',at,'STALE_RANGE_EXPIRED_REQUIRE_FRESH_EVIDENCE')
            return self.view()
        # Failed breakout does not resurrect the previous box. A new box needs new confirmations.
        failed=(self.last_exit is not None and at-self.last_exit<=6*300 and self.last_box is not None and
                self.last_box['support']<=b[4]<=self.last_box['resistance'])
        if failed:
            self.transition('UNCERTAIN',at,'BREAKOUT_RETURNED_INSIDE_NO_OLD_BOX_RESTORE')
        may_rearm=self.last_exit is None or at-self.last_exit>=POLICY['rearm_bars']*300
        if fresh and may_rearm:
            if self.candidate and at==self.candidate['last_seen']+300 and overlap(self.candidate,fresh,self.tick)>=.65:
                self.candidate['count']+=1;self.candidate['last_seen']=at
            else:self.candidate=dict(fresh,count=1,first_seen=at,last_seen=at)
            if self.candidate['count']>=2:
                c=self.candidate
                # Do not widen the original proposed boundaries on the confirmation bar.
                if c['support']<=b[4]<=c['resistance']:
                    self.box={k:v for k,v in c.items() if k not in ('count','first_seen','last_seen')}
                    self.box.update(formed_at=at,last_validated_at=at,fresh_confirmations=0)
                    self.candidate=None;self.counter=0;self.counter_side=None
                    self.transition('RANGE',at,'TWO_CLOSED_5M_CONFIRM_COMPACT_OSCILLATION')
                    return self.view()
        else:self.candidate=None
        if not failed:
            target='NON_RANGE' if trend['strong'] else 'UNCERTAIN'
            if self.regime!=target:
                self.transition(target,at,'EFFICIENT_LOCAL_MOVE' if trend['strong'] else 'LOCAL_EVIDENCE_INCONCLUSIVE')
        return self.view()

    def view(self):
        return dict(regime=self.regime,direction=self.direction,candle_end=self.last_at,
                    support=self.box['support'] if self.box else None,
                    resistance=self.box['resistance'] if self.box else None,
                    box_formed_at=self.box['formed_at'] if self.box else None,
                    last_validated_at=self.box['last_validated_at'] if self.box else None,
                    breakout_pending=self.counter_side,confirmation_closes=self.counter,
                    candidate_count=self.candidate['count'] if self.candidate else 0,
                    reason=self.reason,metrics=self.metrics,
                    range_only_block=self.regime=='RANGE',
                    fail_closed_block=self.regime!='NON_RANGE')


def import_checked(name,file,sha):
    file=Path(file)
    if hashlib.sha256(file.read_bytes()).hexdigest()!=sha:raise RuntimeError('CODE_HASH_MISMATCH: '+str(file))
    spec=importlib.util.spec_from_file_location(name,file)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def old_native_states(module,bars):
    machine=module.Market('5m');last_box=None;output=[]
    for b in bars:
        at=int(b[0])+300
        machine.on_bar(b)
        if machine.regime!='RANGE' and last_box and last_box['support']<=b[4]<=last_box['resistance']:
            machine.box=dict(last_box,formed_at=at)
            machine.switch('RANGE',at,'5M_CLOSE_RETURNED_INSIDE_RANGE');machine.stop=None
        if machine.regime=='RANGE' and machine.box and b[0]>=machine.box['formed_at']:
            box=machine.box
            target='UP' if b[4]>box['resistance']+box['buffer'] else 'DOWN' if b[4]<box['support']-box['buffer'] else None
            machine.counter=machine.counter+1 if target and target==machine.counter_side else 1 if target else 0
            machine.counter_side=target
            if machine.counter>=2:
                last_box=dict(box)
                machine.switch(target,at,'TWO_CLOSED_5M_NATIVE_RESEARCH_EXIT')
                machine.box=None;machine.stop=None
        output.append(machine.view())
    return output


def trade_replay(m,trade,one,times,funding,mark,end,fee):
    entry=trade['entry'];qty=1000./entry
    pos=dict(side=trade['side'],entry=entry,sl=trade['sl'],target=trade['tp2'])
    first=math.floor(trade['opened']/60)*60+60
    status='OPEN';price=None;exit_at=None
    for b in one[bisect.bisect_left(times,first):]:
        hit=m.exit_touch(pos,b)
        if hit:
            status,price=hit;exit_at=int(b[0]);break
    if status=='AMBIGUOUS_STOP_FIRST':status='UNKNOWN'
    sign=1 if trade['side']=='LONG' else -1
    funding_pnl=sum(-sign*qty*mark[t]*rate for t,rate in funding
                    if trade['opened']<t<end and (exit_at is None or t<=exit_at))
    gross=sign*qty*(price-entry) if price is not None and status!='UNKNOWN' else None
    fees=qty*(entry+price)*fee if gross is not None else None
    return dict(status=status,exit_price=price,exit_candle_open=exit_at,
                gross=gross,fees=fees,funding=funding_pnl if status!='UNKNOWN' else None,
                net=gross-fees+funding_pnl if gross is not None else None,
                floating=sign*qty*(one[-1][4]-entry) if status=='OPEN' else None)


def main():
    here=Path(__file__).resolve().parent
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--self-test',action='store_true')
    ap.add_argument('--compare',action='store_true')
    ap.add_argument('--reference-run',default='/opt/btc_swing_v1_research/run_20261031_194826_583186')
    ap.add_argument('--trade-db',default='/opt/btc_dual_h1_research/dual.sqlite3')
    ap.add_argument('--since-id',type=int,default=2)
    ap.add_argument('--base-file',default=str(here/'BTC_Swing_Structure_Research_V1.py'))
    ap.add_argument('--helper-file',default=str(here/'BTC_Swing_Valid_Levels_Comparison.py'))
    ap.add_argument('--old-monitor',default=str(here/'BTC_V13_5_3_Live_Market_Monitor.py'))
    args=ap.parse_args()
    if args.self_test:self_test();return
    if not args.compare:ap.error('Choose --self-test or --compare; no live mode or installer exists')
    helper=import_checked('frozen_reader',args.helper_file,HELPER_SHA)
    m=helper.load_base(args.base_file)
    old=import_checked('frozen_old_range',args.old_monitor,OLD_SHA)
    manifest,_,_,one,funding,mark=helper.cached_inputs(Path(args.reference_run))
    end=manifest['end']
    with closing(sqlite3.connect(Path(args.trade_db).resolve().as_uri()+'?mode=ro',uri=True,timeout=30)) as db:
        db.row_factory=sqlite3.Row
        rows=[dict(r) for r in db.execute('SELECT id,mode,side,entry,sl,tp2,opened,decision '
              'FROM trades WHERE COALESCE(legacy,0)=0 AND id>=? ORDER BY id',(args.since_id,))]
    eligible=[];skipped=[]
    for row in rows:
        if not one[0][0]+14*86400<=row['opened']<end:
            skipped.append(row['id'])
        else:eligible.append(row)
    if not eligible:raise RuntimeError('NO_TRADES_INSIDE_FROZEN_HISTORY_WITH_14D_WARMUP')
    five=m.aggregate(one,300,60)
    first=math.floor(min(t['opened'] for t in eligible)/300)*300-14*86400
    five=[b for b in five if b[0]>=first]
    machine=Range5m(manifest['tick']);new_states=[]
    for b in five:new_states.append(machine.on_closed_bar(b,int(b[0])+300))
    old_states=old_native_states(old,five)
    ends=[int(b[0])+300 for b in five];times=[b[0] for b in one]
    evaluated=[]
    for trade in eligible:
        at=trade['decision']
        if not math.isfinite(at) or at>trade['opened']:raise RuntimeError('NONCAUSAL_RECORDED_DECISION')
        idx=bisect.bisect_right(ends,at)-1
        if idx<0:raise RuntimeError('NO_CAUSAL_5M_STATE')
        a=old_states[idx];b=new_states[idx]
        evaluated.append(dict(id=trade['id'],mode=trade['mode'],side=trade['side'],
            opened=trade['opened'],time_iran=stamp(trade['opened']),decision=at,
            entry=trade['entry'],stop=trade['sl'],target=trade['tp2'],
            old_range=a,new_range=b,
            outcome=trade_replay(m,trade,one,times,funding,mark,end,manifest['fee_bps_assumed']/10000)))
    gates={'ALL_RECORDED':lambda r:True,
           'OLD_RANGE_ONLY':lambda r:r['old_range']['regime']!='RANGE',
           'NEW_RANGE_ONLY':lambda r:not r['new_range']['range_only_block'],
           'NEW_FAIL_CLOSED_DIAGNOSTIC':lambda r:not r['new_range']['fail_closed_block']}
    summary={}
    for label,gate in gates.items():
        kept=[r for r in evaluated if gate(r)];removed=[r for r in evaluated if not gate(r)]
        closed=[r for r in kept if r['outcome']['net'] is not None]
        summary[label]=dict(kept=len(kept),removed=len(removed),
            kept_ids=[r['id'] for r in kept],removed_ids=[r['id'] for r in removed],
            outcomes=dict(collections.Counter(r['outcome']['status'] for r in kept)),
            closed_gross=sum(r['outcome']['gross'] for r in closed),
            closed_fees=sum(r['outcome']['fees'] for r in closed),
            closed_funding=sum(r['outcome']['funding'] for r in closed),
            closed_net=sum(r['outcome']['net'] for r in closed),
            removed_targets=[r['id'] for r in removed if r['outcome']['status']=='TARGET'],
            removed_stops=[r['id'] for r in removed if r['outcome']['status'] in ('SL','SL_GAP')],
            uncertain_kept_ids=[r['id'] for r in kept if r['new_range']['regime']=='UNCERTAIN'])
        print('RESULT',label,json.dumps(summary[label]),flush=True)
    output=Path(args.reference_run).parent/('range5m_v2_'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S_%f'))
    output.mkdir()
    meta=dict(version=VERSION,policy=POLICY,reference_manifest=manifest,
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        replay_end_iran=stamp(end),skipped_outside_frozen_history_ids=skipped,
        study='FILTER_OF_RECORDED_TRADES_NOT_FULL_COUNTERFACTUAL_SIGNAL_RERUN',
        source='Actual saved entries/stops/targets; not newly generated entries',
        old_model='Historical native-5m research wrapper, not active production entry gate',
        costs='1000 USDT independent notional per trade; assumed historical fee; funding estimate; no added slippage',
        limitations=['Entry minute excluded; intraminute historical execution not verified',
            'UNKNOWN excluded from known realized PNL; open trades separate',
            'No capital constraint; overlapping recorded positions remain independent',
            'No historical price chase or reentry chain re-simulation',
            'Frozen data ends before current time; later trades skipped explicitly',
            'Thresholds exploratory; no parameter optimization or live deployment'],
        notes='RANGE_ONLY allows UNCERTAIN, reported explicitly. FAIL_CLOSED also blocks uncertainty and is a separate diagnostic.')
    for filename,data in [('manifest.json',meta),('summary.json',summary),('trades.json',evaluated),('events.json',machine.events)]:
        (output/filename).write_text(json.dumps(data,indent=2,allow_nan=False),encoding='utf-8')
    with (output/'timeline_5m.csv').open('w',newline='',encoding='utf-8-sig') as f:
        fields=['at','time_iran','old_regime','new_regime','direction','support','resistance','box_formed_at','reason']
        writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader()
        for at,a,b in zip(ends,old_states,new_states):
            writer.writerow(dict(at=at,time_iran=stamp(at),old_regime=a['regime'],new_regime=b['regime'],
                direction=b['direction'],support=b['support'],resistance=b['resistance'],
                box_formed_at=b['box_formed_at'],reason=b['reason']))
    report=['# اصلاح پژوهشی رنج ۵ دقیقه‌ای','',
        'صرفاً فیلتر روی معاملات ثبت‌شده؛ صدور کامل سیگنال با زنجیره ورود مجدد بازپخش نشده است.',
        'محدوده محلی ۱۲ تا۲۴ کندل، عرض حداکثر۴ATR، انقضا و بازسازی با داده تازه، خروج دو کندل بسته۵دقیقه‌ای.',
        'قیمت ورود، استاپ و هدف از معاملات واقعی ثبت‌شده؛ حجم مستقل۱۰۰۰USDT، هزینه طبق مفروضات گزارش.',
        'پایان داده: '+stamp(end),'',
        '| حالت | باقی | حذف | هدف | استاپ | باز | نامشخص | خالص بسته |',
        '|---|---:|---:|---:|---:|---:|---:|---:|']
    for label,s in summary.items():
        o=s['outcomes']
        report.append('| %s | %d | %d | %d | %d | %d | %d | %.2f |' %
            (label,s['kept'],s['removed'],o.get('TARGET',0),o.get('SL',0)+o.get('SL_GAP',0),o.get('OPEN',0),o.get('UNKNOWN',0),s['closed_net']))
    report+=['','حالت فقط رنج، نامشخص را مجاز می‌کند؛ حالت سخت‌گیرانه آن را هم می‌بندد. نتیجه این دو را یکی نگیرید.',
        'سیگنال‌های خارج از تاریخچه کنار گذاشته شده‌اند: '+str(skipped),
        'هیچ سرویس، سفارش، تلگرام یا قانون صدور سیگنال فعال تغییر نکرده است.']
    (output/'report_FA.md').write_text('\n'.join(report)+'\n',encoding='utf-8')
    print('REPORT_DIRECTORY',str(output))
    print('READ_ONLY_RESEARCH_COMPLETE; NO BOT, SERVICE, TELEGRAM OR ORDER CHANGES')


def self_test():
    # Repeatable sideways candles must form a compact local range, not retain a huge historic box.
    seq=[]
    pattern=[0.,1.,0.,-1.]
    for i in range(70):
        c=100.+pattern[i%4];o=100.+pattern[(i-1)%4]
        seq.append([i*300,o,max(o,c)+.4,min(o,c)-.4,c,1.])
    x=Range5m(.1)
    for b in seq:x.on_closed_bar(b,int(b[0])+300)
    assert x.regime=='RANGE' and x.box['window']<=24
    frozen=dict(x.box);t=seq[-1][0]+300
    b=[t,100.,104.,99.8,103.5,1.]
    x.on_closed_bar(b,t+300)
    assert x.regime=='RANGE' and x.counter==1
    assert x.box['support']==frozen['support'] and x.box['resistance']==frozen['resistance']
    x.on_closed_bar([t+300,103.5,105.,103.,104.5,1.],t+600)
    assert x.regime=='NON_RANGE' and x.box is None
    x.on_closed_bar([t+600,104.5,104.5,99.,100.,1.],t+900)
    assert x.regime=='UNCERTAIN' and x.box is None
    assert 'NO_OLD_BOX_RESTORE' in x.reason
    # Bearish exit is symmetric; one excursion and wick must not confirm an exit.
    y=Range5m(.1)
    for b in seq:y.on_closed_bar(b,int(b[0])+300)
    y.on_closed_bar([t,100,104,99,100,1],t+300)
    assert y.regime=='RANGE' and y.counter==0
    y.on_closed_bar([t+300,100,100.1,96,96.5,1],t+600)
    assert y.regime=='RANGE' and y.counter==1
    y.on_closed_bar([t+600,96.5,97,95,95.5,1],t+900)
    assert y.regime=='NON_RANGE'
    # Efficient trend cannot be swallowed by a stale broad box.
    z=Range5m(.1)
    for i in range(100):
        b=[i*300,100+i,101.2+i,99.8+i,101+i,1.]
        z.on_closed_bar(b,i*300+300)
    assert z.regime=='NON_RANGE' and z.direction=='UP' and z.box is None
    # New bars must be closed; identical duplicates are idempotent; gaps fail closed.
    before=z.dump()
    last=list(z.bars[-1]);assert z.on_closed_bar(last,last[0]+300)==z.view()
    assert z.dump()==before
    try:z.on_closed_bar([30000,200,202,199,201,1],30000)
    except ValueError:pass
    else:raise AssertionError('OPEN_CANDLE_ACCEPTED')
    try:z.on_closed_bar([30300,200,202,199,201,1],30600)
    except ValueError:pass
    else:raise AssertionError('GAP_ACCEPTED')
    # State persistence and complete prefix causality, including frozen boundaries and expiry.
    import random
    rng=random.Random(905);bars=[];c=1000.
    for i in range(500):
        o=c;c+=rng.uniform(-2,2)
        bars.append([i*300,o,max(o,c)+.2,min(o,c)-.2,c,1])
    def run(bars,saved=None):
        q=Range5m(.1,saved);views=[]
        for b in bars:views.append(q.on_closed_bar(b,b[0]+300))
        return q,views
    full,views=run(bars);a,head=run(bars[:250]);rest,tail=run(bars[250:],a.dump())
    assert views[:250]==head and views[250:]==tail and full.dump()==rest.dump()
    # Stale range expires to uncertainty; it is never silently labelled a confirmed trend.
    q=Range5m(.1)
    for b in seq:q.on_closed_bar(b,b[0]+300)
    q.box['last_validated_at']=q.last_at-6*300
    q.find_range=lambda:None
    q.on_closed_bar([t,100,100.2,99.8,100,1],t+300)
    assert q.regime=='UNCERTAIN' and q.box is None
    # Alternating single closes cannot keep an unvalidated old range alive forever.
    q=Range5m(.1)
    for b in seq:q.on_closed_bar(b,b[0]+300)
    q.find_range=lambda:None
    for i in range(8):
        close=104. if i%2==0 else 96.
        q.on_closed_bar([t+i*300,100.,105.,95.,close,1.],t+(i+1)*300)
    assert q.box is None and q.regime!='RANGE'
    print('RANGE5M_V2_SELF_TEST_OK: local range, both breakouts, two closed bars, wick rejection, '
          'no old-box restore, trend escape, stale expiry, duplicate/gap/open rejection, state restore, prefix causality')


if __name__=='__main__':main()
