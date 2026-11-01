#!/usr/bin/env python3
"""Persistent non-range structure V3. Frozen-cohort research only; no live installer."""
import argparse
import bisect
import collections
import csv
import datetime as dt
import hashlib
import importlib.util
import json
import math
from pathlib import Path

VERSION='BTC-5M-LOCAL-RANGE-V3-PERSISTENT'
V2_SHA='c1ab47fce5e69ea2f393343b5bd5fb308cde432fbb5d571b87d119bf776ead11'
PERSISTENCE=dict(pivot_left=2,pivot_right=2,break_closes=2,break_buffer_atr=.15,
                 invalidation_closes=2,proposal_expiry_bars=6,
                 bootstrap='Two closed 5m bars beyond a known confirmed pivot; opposite pivot protects structure',
                 range_exit='Frozen range opposite edge protects initial breakout; later confirmed pivots trail it',
                 reset='Confirmed fresh range or two closes beyond protected level; no elapsed-time expiry')


def checked_import(name,file,sha):
    file=Path(file)
    if hashlib.sha256(file.read_bytes()).hexdigest()!=sha:raise RuntimeError('CODE_HASH_MISMATCH: '+str(file))
    spec=importlib.util.spec_from_file_location(name,file)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def persistent_class(v2):
    class Range5mV3(v2.Range5m):
        def __init__(self,tick=.1,saved=None):
            super().__init__(tick)
            self.latch=None
            self.pivot_highs=[];self.pivot_lows=[]
            self.break_proposals={}
            if saved is not None:
                if saved.get('version')!=VERSION or saved.get('range_policy')!=v2.POLICY or saved.get('persistence')!=PERSISTENCE:
                    raise ValueError('INCOMPATIBLE_V3_STATE')
                state=json.loads(json.dumps(saved['state']))
                if state['tick']!=tick:raise ValueError('TICK_STATE_MISMATCH')
                self.__dict__.update(state)

        def dump(self):
            return dict(version=VERSION,range_policy=v2.POLICY,persistence=PERSISTENCE,
                        state=json.loads(json.dumps({k:v for k,v in self.__dict__.items() if k!='events'})))

        def activate(self,side,at,level,buffer,origin,protected_open=None):
            sign=1 if side=='UP' else -1
            self.latch=dict(side=side,activated_at=at,protected_level=level,buffer=buffer,
                            invalidation=level-sign*buffer,protected_open=protected_open,
                            invalidation_count=0,origin=origin)
            self.break_proposals={}
            self.regime='NON_RANGE'

        def transition(self,target,at,reason):
            # V2 used momentary six-bar efficiency as a regime. It is now only a range-detection input.
            if target=='NON_RANGE' and reason=='EFFICIENT_LOCAL_MOVE':
                if self.latch is None:self.reason='WAITING_FOR_CONFIRMED_5M_STRUCTURE'
                return
            if self.latch and target=='UNCERTAIN' and reason in (
                    'LOCAL_EVIDENCE_INCONCLUSIVE','BREAKOUT_RETURNED_INSIDE_NO_OLD_BOX_RESTORE'):
                self.reason='NON_RANGE_STRUCTURE_RETAINED_DURING_PULLBACK'
                return
            super().transition(target,at,reason)
            if target=='RANGE':
                self.latch=None;self.break_proposals={}
            elif target=='NON_RANGE' and reason.startswith('TWO_CLOSED_5M_BEYOND_FROZEN_BOX_'):
                side=reason.rsplit('_',1)[-1]
                box=self.last_box
                level=box['support'] if side=='UP' else box['resistance']
                self.activate(side,at,level,box['buffer'],'FROZEN_RANGE_BREAKOUT')

        def update_pivots(self,at):
            if len(self.bars)<5 or self.atr is None:return
            w=self.bars[-5:];p=w[2];others=w[:2]+w[3:]
            buffer=max(self.tick,.15*self.atr)
            if all(p[2]>b[2] for b in others):
                self.pivot_highs.append(dict(price=p[2],open=p[0],known=at,buffer=buffer))
            if all(p[3]<b[3] for b in others):
                self.pivot_lows.append(dict(price=p[3],open=p[0],known=at,buffer=buffer))
            self.pivot_highs=self.pivot_highs[-288:]
            self.pivot_lows=self.pivot_lows[-288:]

        def sustain(self,b,at):
            latch=self.latch
            sign=1 if latch['side']=='UP' else -1
            if at<=latch['activated_at']:return
            crossed=sign*(b[4]-latch['invalidation'])<0
            latch['invalidation_count']=latch['invalidation_count']+1 if crossed else 0
            if latch['invalidation_count']>=PERSISTENCE['invalidation_closes']:
                previous=dict(latch)
                self.latch=None;self.break_proposals={}
                self.regime='UNCERTAIN';self.reason='TWO_CLOSED_5M_INVALIDATE_PERSISTENT_STRUCTURE'
                self.events.append(dict(at=at,from_regime='NON_RANGE',to='UNCERTAIN',reason=self.reason,
                                        invalidated_structure=previous))
                return
            if crossed:
                self.reason='STRUCTURE_INVALIDATION_PENDING_ONE_CLOSE'
                return
            pivots=self.pivot_lows if sign==1 else self.pivot_highs
            if pivots:
                p=pivots[-1]
                next_stop=p['price']-sign*p['buffer']
                # A new protected pivot must form after activation and be known before use.
                if p['open']>=latch['activated_at'] and p['known']<=at and sign*(next_stop-latch['invalidation'])>0 and sign*(b[4]-next_stop)>0:
                    old=latch['invalidation']
                    latch.update(protected_level=p['price'],buffer=p['buffer'],invalidation=next_stop,protected_open=p['open'])
                    self.events.append(dict(at=at,from_regime='NON_RANGE',to='NON_RANGE',reason='CONFIRMED_5M_PIVOT_TIGHTENS_INVALIDATION',
                                            previous_invalidation=old,invalidation=next_stop,pivot=p))
            self.reason='NON_RANGE_STRUCTURE_RETAINED_DURING_PULLBACK'

        def bootstrap(self,b,at):
            winners=[]
            if not self.pivot_highs or not self.pivot_lows:return
            for side,levels,protectors in (('UP',self.pivot_highs,self.pivot_lows),('DOWN',self.pivot_lows,self.pivot_highs)):
                sign=1 if side=='UP' else -1
                proposal=self.break_proposals.get(side)
                if proposal and (at-proposal['created'])//300>PERSISTENCE['proposal_expiry_bars']:
                    self.break_proposals.pop(side,None);proposal=None
                if proposal is None:
                    level=levels[-1];protect=protectors[-1]
                    if b[0]<level['known'] or sign*(b[4]-level['price'])<=level['buffer']:
                        continue
                    invalidation=protect['price']-sign*protect['buffer']
                    if sign*(b[4]-invalidation)<=0:continue
                    proposal=dict(side=side,level=level,protect=protect,created=at,count=0)
                    self.break_proposals[side]=proposal
                level=proposal['level']
                crossed=b[0]>=level['known'] and sign*(b[4]-level['price'])>level['buffer']
                if not crossed:
                    self.break_proposals.pop(side,None)
                    continue
                proposal['count']+=1
                if proposal['count']>=PERSISTENCE['break_closes']:winners.append(dict(proposal))
            if len(winners)>1:
                self.break_proposals={};self.reason='CONFLICTING_STRUCTURE_BREAKS';return
            if winners:
                p=winners[0];protect=p['protect']
                sign=1 if p['side']=='UP' else -1
                if sign*(b[4]-(protect['price']-sign*protect['buffer']))<=0:
                    self.break_proposals={};return
                self.activate(p['side'],at,protect['price'],protect['buffer'],'CONFIRMED_5M_PIVOT_BREAK',protect['open'])
                self.reason='TWO_CLOSED_5M_CONFIRM_INITIAL_STRUCTURE_'+p['side']
                self.events.append(dict(at=at,from_regime='UNCERTAIN',to='NON_RANGE',reason=self.reason,
                                        broken_pivot=p['level'],structure=dict(self.latch)))

        def on_closed_bar(self,bar,cutoff):
            duplicate=bool(self.bars and bar[0]==self.bars[-1][0])
            super().on_closed_bar(bar,cutoff)
            if duplicate:return self.view()
            at=int(bar[0])+300
            self.update_pivots(at)
            if len(self.bars)<30:return self.view()
            if self.regime=='RANGE':
                self.latch=None;self.break_proposals={}
            elif self.latch:
                self.sustain(bar,at)
            else:
                self.bootstrap(bar,at)
            return self.view()

        def view(self):
            result=super().view()
            latch=getattr(self,'latch',None)
            result.update(persistent_direction=latch['side'] if latch else None,
                          structure_activated_at=latch['activated_at'] if latch else None,
                          invalidation=latch['invalidation'] if latch else None,
                          invalidation_confirmation_closes=latch['invalidation_count'] if latch else 0,
                          structure_origin=latch['origin'] if latch else None,
                          initial_break_proposals=json.loads(json.dumps(getattr(self,'break_proposals',{}))))
            result['fail_closed_block']=self.regime!='NON_RANGE' or latch is None or latch['invalidation_count']>0
            return result
    return Range5mV3


def summarize(rows,gate,state_key):
    kept=[r for r in rows if gate(r)];removed=[r for r in rows if not gate(r)]
    closed=[r for r in kept if r['outcome']['net'] is not None]
    return dict(kept=len(kept),removed=len(removed),kept_ids=[r['id'] for r in kept],removed_ids=[r['id'] for r in removed],
        outcomes=dict(collections.Counter(r['outcome']['status'] for r in kept)),
        closed_gross=sum(r['outcome']['gross'] for r in closed),closed_fees=sum(r['outcome']['fees'] for r in closed),
        closed_funding=sum(r['outcome']['funding'] for r in closed),closed_net=sum(r['outcome']['net'] for r in closed),
        removed_targets=[r['id'] for r in removed if r['outcome']['status']=='TARGET'],
        removed_stops=[r['id'] for r in removed if r['outcome']['status'] in ('SL','SL_GAP')],
        uncertain_kept_ids=[r['id'] for r in kept if r[state_key]['regime']=='UNCERTAIN'])


def main():
    here=Path(__file__).resolve().parent
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--self-test',action='store_true')
    ap.add_argument('--compare',action='store_true')
    ap.add_argument('--v2-file',default=str(here/'BTC_5m_Local_Range_V2_Research.py'))
    ap.add_argument('--base-file',default=str(here/'BTC_Swing_Structure_Research_V1.py'))
    ap.add_argument('--helper-file',default=str(here/'BTC_Swing_Valid_Levels_Comparison.py'))
    ap.add_argument('--reference-run',help='Optional original frozen run; manifest must match the V2 report')
    ap.add_argument('--v2-report',default='/opt/btc_swing_v1_research/range5m_v2_20261031_210638_143374')
    args=ap.parse_args()
    v2=checked_import('range_v2_frozen',args.v2_file,V2_SHA)
    cls=persistent_class(v2)
    if args.self_test:self_test(v2,cls);return
    if not args.compare:ap.error('Select --self-test or --compare; no live mode exists')
    helper=v2.import_checked('cache_reader',args.helper_file,v2.HELPER_SHA)
    m=helper.load_base(args.base_file)
    report=Path(args.v2_report)
    reference_meta=json.loads((report/'manifest.json').read_text(encoding='utf-8'))
    if reference_meta.get('script_sha256')!=V2_SHA:raise RuntimeError('V2_REFERENCE_CODE_MISMATCH')
    # Reuse the exact cohort, entry prices, stops and final targets recorded by the previous study.
    cohort=json.loads((report/'trades.json').read_text(encoding='utf-8'))
    prior_summary=json.loads((report/'summary.json').read_text(encoding='utf-8'))
    reference=Path(args.reference_run) if args.reference_run else Path(args.v2_report).parent/'run_20261031_194826_583186'
    manifest,_,_,one,funding,mark=helper.cached_inputs(reference)
    if manifest!=reference_meta['reference_manifest']:raise RuntimeError('V2_REFERENCE_MANIFEST_MISMATCH')
    if not cohort:raise RuntimeError('EMPTY_FROZEN_COHORT')
    first=math.floor(min(t['opened'] for t in cohort)/300)*300-14*86400
    five=[b for b in m.aggregate(one,300,60) if b[0]>=first]
    old=v2.Range5m(manifest['tick']);new=cls(manifest['tick'])
    old_states=[];new_states=[]
    for b in five:
        at=int(b[0])+300
        old_states.append(old.on_closed_bar(b,at));new_states.append(new.on_closed_bar(b,at))
    ends=[b[0]+300 for b in five];times=[b[0] for b in one]
    rows=[]
    for t in cohort:
        if t['decision']>t['opened']:raise RuntimeError('NONCAUSAL_COHORT_DECISION')
        idx=bisect.bisect_right(ends,t['decision'])-1
        if idx<0:raise RuntimeError('MISSING_DECISION_HISTORY')
        if old_states[idx]!=t['new_range']:raise RuntimeError('V2_SNAPSHOT_REPRODUCTION_FAILED: '+str(t['id']))
        trade=dict(t,sl=t['stop'],tp2=t['target'])
        outcome=v2.trade_replay(m,trade,one,times,funding,mark,manifest['end'],manifest['fee_bps_assumed']/10000)
        if outcome!=t['outcome']:raise RuntimeError('TRADE_REPLAY_REPRODUCTION_FAILED: '+str(t['id']))
        rows.append(dict(t,v2_range=t['new_range'],v3_range=new_states[idx]))
    baseline_gates={'ALL_RECORDED':lambda r:True,
        'OLD_RANGE_ONLY':lambda r:r['old_range']['regime']!='RANGE',
        'NEW_RANGE_ONLY':lambda r:not r['v2_range']['range_only_block'],
        'NEW_FAIL_CLOSED_DIAGNOSTIC':lambda r:not r['v2_range']['fail_closed_block']}
    summary={}
    for name,gate in baseline_gates.items():
        s=summarize(rows,gate,'v2_range');helper.compare_baseline(s,prior_summary[name]);summary[name]=s
    print('V2_REPRODUCTION_OK: all snapshots, outcomes and four baseline summaries match',flush=True)
    for label,gate in [('V3_RANGE_ONLY',lambda r:not r['v3_range']['range_only_block']),
                       ('V3_CONFIRMED_NON_RANGE',lambda r:not r['v3_range']['fail_closed_block'])]:
        summary[label]=summarize(rows,gate,'v3_range')
    for name,s in summary.items():print('RESULT',name,json.dumps(s),flush=True)
    output=report.parent/('range5m_v3_'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S_%f'))
    output.mkdir()
    meta=dict(version=VERSION,v2_report=str(report),range_policy=v2.POLICY,persistence=PERSISTENCE,
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),reference_manifest=manifest,
        replay_end_iran=v2.stamp(manifest['end']),cohort_ids=[r['id'] for r in rows],
        source_trade_json_sha256=hashlib.sha256((report/'trades.json').read_bytes()).hexdigest(),
        method='Static filter of the exact prior recorded-trade cohort, not full counterfactual entries',
        costs=reference_meta['costs'],limitations=reference_meta['limitations'],
        note='Higher-timeframe direction, actual entry/SL/target and all execution assumptions unchanged. No local-side alignment gate added.',
        validation='Same previously inspected history; exploratory. V2 and prices reproduced before comparison.',
        pending_invalidation='First invalidating close temporarily blocks strict entry; second close cancels structure')
    for filename,data in [('manifest.json',meta),('summary.json',summary),('trades.json',rows),('events.json',new.events)]:
        (output/filename).write_text(json.dumps(data,indent=2,allow_nan=False),encoding='utf-8')
    with (output/'timeline_5m.csv').open('w',newline='',encoding='utf-8-sig') as f:
        fields=['at','time_iran','v2_regime','v3_regime','short_direction','persistent_direction','invalidation',
                'invalidation_closes','support','resistance','reason']
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader()
        for at,a,b in zip(ends,old_states,new_states):
            w.writerow(dict(at=at,time_iran=v2.stamp(at),v2_regime=a['regime'],v3_regime=b['regime'],
                short_direction=b['direction'],persistent_direction=b['persistent_direction'],invalidation=b['invalidation'],
                invalidation_closes=b['invalidation_confirmation_closes'],support=b['support'],resistance=b['resistance'],reason=b['reason']))
    text=['# مقایسه رنج۵دقیقه‌ای با حفظ ساختار','',
        'شکست معتبر، غیررنج را تا ابطال ساختار یا تشکیل رنج تازه حفظ می‌کند. افت حرکت شش کندل، مجوز را حذف نمی‌کند.',
        'مقایسه روی همان معاملات و همان پایان تاریخچه است؛ سیگنال جدید یا نتیجه زنده نیست.',
        '| حالت | باقی | حذف | هدف | استاپ | باز | نامشخص | خالص بسته |',
        '|---|---:|---:|---:|---:|---:|---:|---:|---:|']
    for label,s in summary.items():
        o=s['outcomes']
        text.append('| %s | %d | %d | %d | %d | %d | %d | %.2f |' %
            (label,s['kept'],s['removed'],o.get('TARGET',0),o.get('SL',0)+o.get('SL_GAP',0),o.get('OPEN',0),o.get('UNKNOWN',0),s['closed_net']))
    text+=['','RANGE_ONLY نامشخص را مجاز می‌کند؛ حالت تأییدشده نامشخص و اولین کندل ابطال را مسدود می‌کند.',
        'جهت کوتاه‌مدت، جهت ساختار حفظ‌شده و روند تایم بالاتر سه مفهوم جدا هستند.',
        'استاپ ابطال ساختار فقط برای فیلتر است؛ استاپ واقعی معاملات تغییر نکرده است.',
        'هیچ سرویس، سفارش یا پیامی تغییر نکرده است.']
    (output/'report_FA.md').write_text('\n'.join(text)+'\n',encoding='utf-8')
    print('REPORT_DIRECTORY',str(output))
    print('READ_ONLY_RESEARCH_COMPLETE; NO BOT, SOURCE DATA, SERVICE, TELEGRAM OR ORDER CHANGES')


def self_test(v2,cls):
    v2.self_test()
    seq=[];pattern=[0.,1.,0.,-1.]
    for i in range(70):
        c=100+pattern[i%4];o=100+pattern[(i-1)%4]
        seq.append([i*300,o,max(o,c)+.4,min(o,c)-.4,c,1.])
    def breakout(side):
        q=cls(.1)
        for b in seq:q.on_closed_bar(b,b[0]+300)
        t=seq[-1][0]+300
        if side=='UP':extra=[[t,100,104,99.8,103.5,1],[t+300,103.5,105,103,104.5,1]]
        else:extra=[[t,100,100.1,96,96.5,1],[t+300,96.5,97,95,95.5,1]]
        for b in extra:q.on_closed_bar(b,b[0]+300)
        assert q.regime=='NON_RANGE' and q.latch['side']==side
        return q,t+600
    for side in ('UP','DOWN'):
        q,t=breakout(side)
        # The efficient move can fade while protected structure and entry permission persist.
        for i in range(4):
            c=(104. if side=='UP' else 96.)+(.1 if i%2 else 0.)
            b=[t+i*300,c,c+.2,c-.2,c,1]
            q.on_closed_bar(b,b[0]+300)
        assert q.regime=='NON_RANGE' and not q.view()['fail_closed_block']
        assert not q.trend()['strong']
        saved=q.dump();restored=cls(.1,saved)
        assert restored.view()==q.view()
        q.find_range=lambda:None
        t=q.last_at;stop=q.latch['invalidation'];sign=1 if side=='UP' else -1
        # A wick alone does not invalidate; one close blocks strict entry; two cancel the latch.
        c=stop+sign*1.
        q.on_closed_bar([t,c,max(c,stop)+1,min(c,stop)-1,c,1],t+300)
        assert q.latch and q.latch['invalidation_count']==0
        t+=300;c=q.latch['invalidation']-sign*1.
        q.on_closed_bar([t,c,c+.2,c-.2,c,1],t+300)
        assert q.regime=='NON_RANGE' and q.view()['fail_closed_block']
        q.on_closed_bar([t+300,c,c+.2,c-.2,c,1],t+600)
        assert q.regime=='UNCERTAIN' and q.latch is None
    # A new validated range can replace an otherwise unbroken directional structure.
    q,t=breakout('UP')
    for i in range(50):
        c=104.+(.1 if i%2 else -.1)
        q.on_closed_bar([t+i*300,104,c+.3,c-.3,c,1],t+(i+1)*300)
    assert q.regime=='RANGE' and q.latch is None
    # Trailing uses only post-activation confirmed pivots, never future knowledge or a looser level.
    q,t=breakout('UP');at=q.latch['activated_at'];old_stop=q.latch['invalidation']
    q.pivot_lows.append(dict(price=102.,open=at,known=at+900,buffer=.2))
    q.sustain([at+300,104,105,103,104,1],at+600)
    assert q.latch['invalidation']==old_stop
    q.sustain([at+600,104,105,103,104,1],at+900)
    assert q.latch['invalidation']==101.8
    q.pivot_lows.append(dict(price=101.,open=at+300,known=at+1200,buffer=.2))
    q.sustain([at+900,104,105,103,104,1],at+1200)
    assert q.latch['invalidation']==101.8
    # Initial structure break requires known pivots and two CLOSED candles.
    q=cls(.1)
    for b in seq[:29]:q.on_closed_bar(b,b[0]+300)
    q.box=None;q.regime='UNCERTAIN';q.candidate=None;q.find_range=lambda:None
    q.pivot_highs=[dict(price=102,open=0,known=600,buffer=.2)]
    q.pivot_lows=[dict(price=98,open=300,known=900,buffer=.2)]
    t=q.last_at
    q.on_closed_bar([t,101,104,100,103,1],t+300)
    assert q.latch is None
    q.on_closed_bar([t+300,103,105,102,104,1],t+600)
    assert q.latch and q.latch['origin']=='CONFIRMED_5M_PIVOT_BREAK'
    # All outputs and persistent state are invariant to future candles and state restoration.
    import random
    rng=random.Random(483);bars=[];c=1000.
    for i in range(600):
        o=c;c+=rng.uniform(-2,2)
        bars.append([i*300,o,max(o,c)+.2,min(o,c)-.2,c,1])
    def run(bars,saved=None):
        x=cls(.1,saved);out=[]
        for b in bars:out.append(x.on_closed_bar(b,b[0]+300))
        return x,out
    full,views=run(bars);head,first=run(bars[:300]);tail,last=run(bars[300:],head.dump())
    assert views[:300]==first and views[300:]==last and full.dump()==tail.dump()
    print('RANGE5M_V3_SELF_TEST_OK: persistent pullback permission, both sides, wick rejection, '
          'two-close structural invalidation, fresh range replacement, known-pivot startup, causal monotonic trailing, state restore, prefix causality')


if __name__=='__main__':main()
