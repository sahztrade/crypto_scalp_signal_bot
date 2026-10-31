#!/usr/bin/env python3
"""Compare frozen Swing V1 with permanently retired, broken obstacle levels.
Research only: cached input database is opened read-only; no network or services.
"""
import argparse
import bisect
import csv
from contextlib import closing
import datetime as dt
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import sqlite3

VERSION='BTC-SWING-VALID-LEVELS-COMPARISON-V1'
BASE_SHA256='5e84dddc29d0213bec8611b237a9f36854b6027c6df8e3c9aea354893085c9cb'


def load_base(path):
    path=Path(path)
    actual=hashlib.sha256(path.read_bytes()).hexdigest()
    if actual!=BASE_SHA256:
        raise RuntimeError('BASE_CODE_HASH_MISMATCH: '+actual)
    spec=importlib.util.spec_from_file_location('swing_frozen_base',path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def corrected_classes(m):
    original_structure=m.Structure
    original_strategy=m.Strategy
    class ValidStructure(original_structure):
        def step(self,b):
            super().step(b)
            at=int(b[0])+self.seconds
            for pivots,sign in ((self.highs,1),(self.lows,-1)):
                for p in pivots:
                    # A wick or equality does not retire a level. Once broken it stays retired.
                    if p['known']<=at and p.get('retired_at') is None and sign*(b[4]-p['price'])>0:
                        p['retired_at']=at
    class ValidStrategy(original_strategy):
        def __init__(self,hs,ls,tick):
            super().__init__(hs,ls,tick)
            self.high=ValidStructure(hs)
            self.low=ValidStructure(ls)
            self.audit=[]
        def on_low(self,b,allowed):
            intent=super().on_low(b,allowed)
            if intent is None:
                return None
            sign=1 if intent['side']=='LONG' else -1
            at=intent['decision']
            candidates=[]
            for label,engine in (('ENTRY_TF',self.low),('TREND_TF',self.high)):
                for p in (engine.highs if sign==1 else engine.lows):
                    if p['known']<=at and sign*(p['price']-intent['reference'])>0:
                        candidates.append(dict(p,timeframe=label,seconds=engine.seconds))
            valid=[p for p in candidates if p.get('retired_at') is None]
            removed=[p for p in candidates if p.get('retired_at') is not None]
            nearest=lambda seq: min(seq,key=lambda p:sign*(p['price']-intent['reference'])) if seq else None
            old=nearest(candidates)
            new=nearest(valid)
            intent['obstacle']=new['price'] if new else None
            intent['obstacle_policy']='UNBROKEN_OWN_TF_CLOSED_LEVELS'
            intent['removed_broken_obstacles']=len(removed)
            intent['old_obstacle']=old['price'] if old else None
            self.audit.append(dict(decision=at,time_iran=m.stamp(at),side=intent['side'],
                reference=intent['reference'],stop=intent['sl'],old_nearest=old,
                valid_nearest=new,removed_count=len(removed),nearest_removed=nearest(removed)))
            return intent
    return ValidStructure,ValidStrategy


def digest(data):
    return hashlib.sha256(json.dumps(data,separators=(',',':')).encode()).hexdigest()


def cached_inputs(reference):
    manifest=json.loads((reference/'manifest.json').read_text(encoding='utf-8'))
    prior=json.loads((reference/'results.json').read_text(encoding='utf-8'))
    if manifest.get('script_sha256')!=BASE_SHA256:
        raise RuntimeError('REFERENCE_CODE_HASH_MISMATCH')
    start,end=manifest['start'],manifest['end']
    cache=reference.parent/'public_bybit_cache.sqlite3'
    with closing(sqlite3.connect(cache.resolve().as_uri()+'?mode=ro',uri=True,timeout=30)) as db:
        def bars(kind,minutes,left,right):
            result=[list(r) for r in db.execute('SELECT t,o,h,l,c,v FROM bars '
                'WHERE kind=? AND minutes=? AND t>=? AND t<? ORDER BY t',(kind,minutes,left,right))]
            sec=minutes*60
            if len(result)!=(right-left)//sec or any(b[0]!=left+i*sec for i,b in enumerate(result)):
                raise RuntimeError('CACHE_GAP: '+kind+' '+str(minutes))
            return result
        fifteen=bars('trade',15,start-90*86400,end)
        one=bars('trade',1,start,end)
        marks=bars('mark',60,start,end)
        funding=list(db.execute('SELECT t,rate FROM funding WHERE t>=? AND t<? ORDER BY t',(start,end)))
        for label,data in [('trade15',fifteen),('trade1',one),('mark60',marks),('funding',funding)]:
            if digest(data)!=manifest.get(label+'_sha256'):
                raise RuntimeError('REFERENCE_DATA_HASH_MISMATCH: '+label)
            print('MATCHED_DATA_HASH',label,flush=True)
        mark_open={int(b[0]):b[1] for b in marks}
        for t,_ in funding:
            if t not in mark_open:
                mark_open[t]=bars('mark',1,t,t+60)[0][1]
    return manifest,prior,fifteen,one,funding,mark_open


def compare_baseline(actual,expected):
    if set(actual)!=set(expected):
        raise RuntimeError('BASELINE_SUMMARY_KEYS_DIFFER')
    for key,value in actual.items():
        old=expected[key]
        if isinstance(value,(int,float)) and not isinstance(value,bool):
            if not isinstance(old,(int,float)) or not math.isclose(value,old,rel_tol=1e-10,abs_tol=1e-8):
                raise RuntimeError('BASELINE_REPRODUCTION_FAILED: '+key)
        elif value!=old:
            raise RuntimeError('BASELINE_REPRODUCTION_FAILED: '+key)


def trade_key(p):
    return (p['decision'],p['side'])


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--base-file',default='/opt/btc_v13_research/BTC_Swing_Structure_Research_V1.py')
    ap.add_argument('--reference-run',default='/opt/btc_swing_v1_research/run_20261031_194826_583186')
    ap.add_argument('--self-test',action='store_true')
    args=ap.parse_args()
    m=load_base(args.base_file)
    valid_structure,valid_strategy=corrected_classes(m)
    if args.self_test:
        self_test(m,valid_structure,valid_strategy)
        return
    reference=Path(args.reference_run)
    manifest,prior,fifteen,one,funding,mark_open=cached_inputs(reference)
    times=[b[0] for b in one]
    original=m.Strategy
    results={'OLD_ALL_PIVOTS':{},'VALID_UNBROKEN_LEVELS':{}}
    audits={}
    try:
        for scenario,klass in [('OLD_ALL_PIVOTS',original),('VALID_UNBROKEN_LEVELS',valid_strategy)]:
            m.Strategy=klass
            for name,hs,ls in [('A_4H_1H',14400,3600),('B_1H_15M',3600,900)]:
                results[scenario][name]={}
                lower=m.aggregate(fifteen,ls)
                higher=m.aggregate(fifteen,hs)
                for segment,left,right in [('DEVELOPMENT',manifest['start'],manifest['split']),
                                           ('PREVIOUS_HOLDOUT_NOW_EXPLORATORY',manifest['split'],manifest['end'])]:
                    # Capture diagnostic intents only; execution and capital remain the frozen V1 engine.
                    engines=[]
                    class RecordingStrategy(klass):
                        def __init__(self,*a):
                            super().__init__(*a)
                            engines.append(self)
                    m.Strategy=RecordingStrategy
                    lo=bisect.bisect_left(times,left);hi=bisect.bisect_left(times,right)
                    r=m.simulate(one[lo:hi],lower,higher,funding,mark_open,left,right,
                        hs,ls,manifest['tick'],manifest['initial_capital'],
                        manifest['fee_bps_assumed']/10000,manifest['slippage_bps_assumed'],
                        manifest['risk_pct']/100,manifest['max_notional_equity'])
                    if scenario=='OLD_ALL_PIVOTS':
                        previous_segment='HOLDOUT' if segment.startswith('PREVIOUS_') else segment
                        expected=prior[name][previous_segment]
                        compare_baseline(r['summary'],expected['summary'])
                        # Exact trade records, not just totals, must reproduce.
                        if r['trades']!=expected['trades'] or r['open_position']!=expected['open_position']:
                            raise RuntimeError('BASELINE_TRADE_REPRODUCTION_FAILED')
                        print('BASELINE_REPRODUCED',name,segment,flush=True)
                    results[scenario][name][segment]=r
                    if scenario=='VALID_UNBROKEN_LEVELS':
                        audits[name+'_'+segment]=engines[0].audit
                    print('RESULT',scenario,name,segment,json.dumps(r['summary']),flush=True)
            if scenario=='OLD_ALL_PIVOTS':
                print('ALL_FOUR_BASELINES_EXACTLY_REPRODUCED',flush=True)
    finally:
        m.Strategy=original
    output=reference.parent/('comparison_valid_levels_'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S_%f'))
    output.mkdir()
    changes={}
    for name in results['OLD_ALL_PIVOTS']:
        changes[name]={}
        for segment in results['OLD_ALL_PIVOTS'][name]:
            def all_trades(r):
                return r['trades']+([dict(r['open_position'],status='OPEN')] if r['open_position'] else [])
            old=all_trades(results['OLD_ALL_PIVOTS'][name][segment])
            new=all_trades(results['VALID_UNBROKEN_LEVELS'][name][segment])
            old_keys={trade_key(p) for p in old};new_keys={trade_key(p) for p in new}
            changes[name][segment]=dict(
                common=[dict(decision=t,side=s,time_iran=m.stamp(t)) for t,s in sorted(old_keys&new_keys)],
                removed=[dict(decision=t,side=s,time_iran=m.stamp(t)) for t,s in sorted(old_keys-new_keys)],
                added=[dict(decision=t,side=s,time_iran=m.stamp(t)) for t,s in sorted(new_keys-old_keys)])
            if new:
                readable=[dict(p,decision_time_iran=m.stamp(p['decision']),entry_time_iran=m.stamp(p['opened']),
                    exit_candle_open_iran=m.stamp(p['closed_candle_open']) if p.get('closed_candle_open') is not None else None)
                    for p in new]
                keys=sorted({k for p in readable for k in p})
                with (output/(name+'_'+segment+'_trades.csv')).open('w',newline='',encoding='utf-8-sig') as f:
                    writer=csv.DictWriter(f,fieldnames=keys);writer.writeheader();writer.writerows(readable)
    comparison_manifest=dict(version=VERSION,base_manifest=manifest,reference_run=str(reference),
        comparator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        correction='Only obstacle eligibility changed: own-timeframe strict closed break permanently retires level',
        break_rule='resistance close>price; support close<price; wicks/equality ignored; no automatic role flip',
        unchanged='direction, breakout, ATR, retest, stop, target, chase, costs, sizing, one-position execution',
        data_access='Cached database read-only; no download, new candle or time-window extension',
        baseline_reproduced=True,
        validation_note='Previous holdout already inspected; revised results there are exploratory, not untouched validation')
    for filename,data in [('manifest.json',comparison_manifest),('results.json',results),
                          ('changes.json',changes),('obstacle_audit.json',audits)]:
        (output/filename).write_text(json.dumps(data,indent=2,allow_nan=False),encoding='utf-8')
    report=['# مقایسه فیلتر موانع BTC Swing','',
        'تنها تفاوت: حذف پیوت مانع پس از شکست با بسته‌شدن کندل تایم‌فریم خودش.',
        'تمام چهار نتیجه نسخه قبلی و رکورد معاملات، قبل از مقایسه بازتولید شدند.',
        '۹۰ روز پایانی قبلاً دیده شده؛ نتیجه اصلاح‌شده در آن، پژوهشی است و آزمون مستقل دست‌نخورده نیست.','',
        '| فیلتر | روش | بخش | بسته | باز | هدف | استاپ/مبهم | خالص بسته | افت سرمایه |',
        '|---|---|---|---:|---:|---:|---:|---:|---:|']
    for scenario,variants in results.items():
        for name,segments in variants.items():
            for segment,r in segments.items():
                s=r['summary'];outcomes=s['outcomes']
                report.append('| %s | %s | %s | %d | %d | %d | %d | %.2f | %.2f%% |' %
                    (scenario,name,segment,s['closed'],s['open'],outcomes.get('TARGET',0),
                     sum(v for k,v in outcomes.items() if k!='TARGET'),s['closed_net_pnl'],
                     s['max_drawdown_minute_close_pct']))
    report+=['','نتایج پس از هزینه‌های مدل قبلی‌اند؛ معامله باز جداست و سودآوری زنده اثبات نشده است.',
        'یک معامله باز، زنجیره فرصت‌های بعدی را تغییر می‌دهد؛ معاملات افزوده/حذف‌شده الزاماً فقط رد مستقیم یک فیلتر نیستند.',
        'مانع شکسته‌شده خودکار به حمایت یا مقاومت جدید تبدیل نمی‌شود؛ پیوت تازه لازم است.',
        'هیچ سرویس یا تنظیمی روی ربات فعال تغییر نکرده است.']
    (output/'report_FA.md').write_text('\n'.join(report)+'\n',encoding='utf-8')
    print('REPORT_DIRECTORY',str(output))
    print('RESEARCH_COMPLETE; NO BOT, SERVICE, TELEGRAM, SOURCE CACHE OR ORDER CHANGES')


def self_test(m,ValidStructure,ValidStrategy):
    m.self_test()
    def make():
        x=ValidStructure(60)
        x.highs=[dict(price=110.,open=-600,known=-300)]
        x.lows=[dict(price=90.,open=-540,known=-240)]
        return x
    x=make()
    x.step([0,100,112,88,100,1])
    assert x.highs[0].get('retired_at') is None and x.lows[0].get('retired_at') is None
    x.step([60,100,111,89,110,1])
    assert x.highs[0].get('retired_at') is None
    x.step([120,110,113,109,111,1])
    assert x.highs[0]['retired_at']==180
    x.step([180,111,111,98,100,1])
    assert x.highs[0]['retired_at']==180
    x.step([240,100,101,88,89,1])
    assert x.lows[0]['retired_at']==300
    x.step([300,89,101,89,100,1])
    assert x.lows[0]['retired_at']==300
    # Future-known levels cannot be retired before their knowledge timestamp.
    y=ValidStructure(60)
    y.highs=[dict(price=110,open=-60,known=180)]
    y.step([0,100,113,99,111,1]);assert y.highs[0].get('retired_at') is None
    # Prefix snapshots must not acquire retirement timestamps from future candles.
    bars=[[0,100,112,98,100,1],[60,100,113,99,111,1],[120,111,112,98,100,1]]
    def snapshots(seq):
        z=make();out=[]
        for b in seq:
            z.step(b);out.append(json.loads(json.dumps(z.highs)))
        return out
    assert snapshots(bars)[:2]==snapshots(bars[:2])
    # Candidate selection differs only for retired levels, for both directions.
    for side in ('LONG','SHORT'):
        strategy=ValidStrategy(240,60,.1)
        strategy.high.direction=side
        if side=='LONG':
            strategy.high.highs=[dict(price=105,open=-1000,known=-500,retired_at=-100),
                                 dict(price=130,open=-900,known=-400)]
            strategy.setup=dict(side=side,at=0,level=100,atr=10,band=2,touched=False,extreme=float('inf'))
            b=[0,101,104,100,103,1]
            expected=130
        else:
            strategy.high.lows=[dict(price=95,open=-1000,known=-500,retired_at=-100),
                                dict(price=70,open=-900,known=-400)]
            strategy.setup=dict(side=side,at=0,level=100,atr=10,band=2,touched=False,extreme=-float('inf'))
            b=[0,99,100,96,97,1]
            expected=70
        result=strategy.on_low(b,True)
        assert result and result['obstacle']==expected and result['removed_broken_obstacles']==1
    # Direction and ATR must be identical before/after the change for every prefix.
    import random
    rng=random.Random(802)
    a=m.Structure(60);b=ValidStructure(60);close=100.
    for i in range(1500):
        o=close;close=max(10.,close+rng.uniform(-3,3))
        bar=[i*60,o,max(o,close)+rng.random(),min(o,close)-rng.random(),close,1]
        a.step(bar);b.step(bar)
        assert a.direction==b.direction and a.atr==b.atr
        for name in ('highs','lows'):
            clean=[{k:v for k,v in p.items() if k!='retired_at'} for p in getattr(b,name)]
            assert getattr(a,name)==clean
    print('VALID_LEVEL_TESTS_OK: wick/equality retained, both closed-break directions, permanent retirement, '
          'known-time causality, prefix invariance, obstacle selection, unchanged trend/ATR')


if __name__=='__main__':
    main()
