#!/usr/bin/env python3
"""Nine fixed entry-method experiments on frozen public Bybit history.
Research only. No network, orders, service edits or Telegram.
"""
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

VERSION='BTC-ENTRY-METHODS-NINE-V1'
HELPER_SHA='f8c4c7525411025b39a9d08c5a0e9bc035def32cb132d4e7ff7d5a8b2928d51d'
ENTRY_POLICY=dict(ema_fast=20,ema_slow=50,channel_bars=20,
                  breakout_atr=.1,retest_atr=.2,retest_bars=6,
                  stop_atr=1.5,target_R=2.,chase_R=.1)
METHODS=('BREAKOUT','BREAKOUT_RETEST','TREND_PULLBACK')
IRAN=dt.timezone(dt.timedelta(hours=3,minutes=30))


def dependencies(base_file,helper_file):
    helper_file=Path(helper_file)
    if hashlib.sha256(helper_file.read_bytes()).hexdigest()!=HELPER_SHA:
        raise RuntimeError('CACHED_INPUT_HELPER_HASH_MISMATCH')
    spec=importlib.util.spec_from_file_location('swing_cached_reader',helper_file)
    helper=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    return helper.load_base(base_file),helper


def classes(m):
    class Indicators(m.Structure):
        def __init__(self,seconds):
            super().__init__(seconds)
            self.ema20=None
            self.ema50=None
            self.count=0
        def step(self,b):
            super().step(b)
            self.count+=1
            for name,length in (('ema20',20),('ema50',50)):
                old=getattr(self,name)
                setattr(self,name,b[4] if old is None else old+(b[4]-old)*2/(length+1))
        def trend(self):
            if self.count<50:return None
            return 'LONG' if self.ema20>self.ema50 else 'SHORT' if self.ema20<self.ema50 else None
    class EntryStrategy:
        def __init__(self,method,hs,ls,tick):
            self.method=method
            self.low=Indicators(ls)
            self.high=m.Structure(hs) # Unused: no higher-timeframe input in this experiment.
            self.tick=tick
            self.setup=None
            self.reasons=collections.Counter()
        def intent(self,side,b,atr,reason,level=None):
            sign=1 if side=='LONG' else -1
            stop=m.tick_round(b[4]-sign*ENTRY_POLICY['stop_atr']*atr,self.tick,
                              'down' if sign==1 else 'up')
            self.reasons['ENTRY_INTENTS']+=1
            return dict(side=side,decision=int(b[0])+self.low.seconds,
                        reference=b[4],sl=stop,atr=atr,obstacle=None,
                        entry_method=self.method,entry_reason=reason,trigger_level=level)
        def on_low(self,b,allowed):
            old_bars=self.low.bars[:]
            atr=self.low.atr
            previous_ema=self.low.ema20
            # Only the current CLOSED candle updates EMA direction. ATR/levels use the prior close.
            self.low.step(b)
            self.reasons['EVALUATED_CLOSED_BARS']+=1
            side=self.low.trend()
            if not allowed:
                self.setup=None
                self.reasons['BARS_WITH_ENTRY_DISABLED']+=1
                return None
            if not side or not atr or len(old_bars)<20:
                self.setup=None
                self.reasons['NO_TREND_OR_WARMUP']+=1
                return None
            sign=1 if side=='LONG' else -1
            if self.method=='TREND_PULLBACK':
                band=ENTRY_POLICY['retest_atr']*atr
                was_outside=sign*(old_bars[-1][4]-previous_ema)>band
                touched=b[3]<=previous_ema+band if sign==1 else b[2]>=previous_ema-band
                confirms=sign*(b[4]-previous_ema)>0 and sign*(b[4]-b[1])>0
                if was_outside and touched and confirms:
                    return self.intent(side,b,atr,'EMA20_PULLBACK_FROM_OUTSIDE_BAND',previous_ema)
                return None
            at=int(b[0])+self.low.seconds
            if self.method=='BREAKOUT_RETEST' and self.setup is not None:
                s=self.setup
                age=(at-s['at'])//self.low.seconds
                if s['side']!=side or age>ENTRY_POLICY['retest_bars']:
                    self.reasons['RETEST_EXPIRED_OR_TREND_CHANGED']+=1
                    self.setup=None
                    return None
                if sign*(b[4]-s['level']) < -s['band']:
                    self.reasons['BREAKOUT_FAILED']+=1
                    self.setup=None
                    return None
                touched=b[3]<=s['level']+s['band'] if sign==1 else b[2]>=s['level']-s['band']
                s['touched']=s['touched'] or touched
                if s['touched'] and sign*(b[4]-s['level'])>0 and sign*(b[4]-b[1])>0:
                    self.setup=None
                    return self.intent(side,b,atr,'CHANNEL_BREAKOUT_THEN_RETEST',s['level'])
                return None
            prior=old_bars[-20:]
            level=max(x[2] for x in prior) if sign==1 else min(x[3] for x in prior)
            crossed=sign*(old_bars[-1][4]-level)<=0 and sign*(b[4]-level)>=ENTRY_POLICY['breakout_atr']*atr
            if not crossed:return None
            self.reasons['CHANNEL_BREAKOUTS']+=1
            if self.method=='BREAKOUT':
                return self.intent(side,b,atr,'CLOSED_20_BAR_CHANNEL_BREAK',level)
            self.setup=dict(side=side,level=level,at=at,
                            band=ENTRY_POLICY['retest_atr']*atr,touched=False)
            return None
    return Indicators,EntryStrategy


def replay(m, strategy_factory, one, low_bars, high_bars, funding, mark_open, start, end,
             high_seconds, low_seconds, tick, initial, fee, slip, risk_pct, cap):
    engine = strategy_factory(high_seconds, low_seconds, tick)
    low_map = {int(b[0])+low_seconds: b for b in low_bars}
    high_map = {int(b[0])+high_seconds: b for b in high_bars}
    fund_map = dict(funding)
    balance = initial
    peak = initial
    max_dd = 0.
    pos = None
    pending = None
    trades = []
    rejected = collections.Counter()
    curve = []
    monthly = {}
    previous_equity = initial
    first = int(one[0][0])
    # Warm history chronologically without generating entries or consuming breakouts.
    for at in sorted(set(t for t in high_map if t <= first) | set(t for t in low_map if t <= first)):
        if at in high_map:
            engine.high.step(high_map[at])
        if at in low_map:
            engine.low.step(low_map[at])
    for b in one:
        t = int(b[0])
        close_at = t+60
        month = dt.datetime.fromtimestamp(t,IRAN).strftime('%Y-%m')
        bucket = monthly.setdefault(month,dict(month=month,equity_start=previous_equity,
            equity_end=previous_equity,entries=0,closed=0,targets=0,stops=0,ambiguous=0,
            gross_cash_pnl=0.,fees_paid=0.,funding_pnl=0.))
        if t >= end:
            break
        if t in fund_map and pos is not None and pos['opened'] < t:
            if t not in mark_open:
                raise RuntimeError('MISSING_SETTLEMENT_MARK')
            payment = -(1 if pos['side'] == 'LONG' else -1) * pos['qty'] * mark_open[t] * fund_map[t]
            balance += payment
            pos['funding_pnl'] += payment
            bucket['funding_pnl'] += payment
        if pending is not None:
            intent = pending
            pending = None
            if start <= t < end and pos is None:
                plan, reason = m.entry_plan(intent, b[1], tick, slip)
                if reason:
                    rejected[reason] += 1
                elif balance <= 0:
                    rejected['NO_CAPITAL'] += 1
                else:
                    qty = min(balance*risk_pct/plan['risk'], balance*cap/plan['entry'])
                    entry_fee = qty*plan['entry']*fee
                    balance -= entry_fee
                    bucket['entries'] += 1
                    bucket['fees_paid'] += entry_fee
                    pos = dict(plan, opened=t, qty=qty, fees=entry_fee,
                               funding_pnl=0., intended_risk_usdt=qty*plan['risk'],
                               capital_capped=qty*plan['risk'] < (balance+entry_fee)*risk_pct-1e-8)
        if pos is not None:
            touched = m.exit_touch(pos, b)
            if touched:
                status, raw_exit = touched
                sign = 1 if pos['side'] == 'LONG' else -1
                price = m.tick_round(raw_exit*(1-sign*slip/10000), tick, 'down' if sign == 1 else 'up')
                gross = sign*pos['qty']*(price-pos['entry'])
                exit_fee = pos['qty']*price*fee
                balance += gross-exit_fee
                bucket['closed'] += 1
                bucket['targets'] += int(status=='TARGET')
                bucket['stops'] += int(status!='TARGET')
                bucket['ambiguous'] += int(status=='AMBIGUOUS_STOP_FIRST')
                bucket['gross_cash_pnl'] += gross
                bucket['fees_paid'] += exit_fee
                pos.update(status=status, closed_candle_open=t, closed_by=close_at,
                           exit=price, gross_pnl=gross, fees=pos['fees']+exit_fee)
                pos['net_pnl'] = gross-pos['fees']+pos['funding_pnl']
                pos['net_R'] = pos['net_pnl']/pos['intended_risk_usdt']
                trades.append(pos)
                pos = None
        # At simultaneous closes, high timeframe is available before the low decision.
        if close_at in high_map:
            engine.high.step(high_map[close_at])
        if close_at in low_map:
            allowed = start <= close_at < end and pos is None and pending is None
            pending = engine.on_low(low_map[close_at], allowed)
        if t >= start:
            unrealized = 0. if pos is None else (1 if pos['side']=='LONG' else -1)*pos['qty']*(b[4]-pos['entry'])
            equity = balance+unrealized
            bucket['equity_end'] = equity
            previous_equity = equity
            peak = max(peak, equity)
            max_dd = max(max_dd, (peak-equity)/peak if peak > 0 else 0)
            if close_at % 86400 == 0 or close_at == end:
                curve.append(dict(at=close_at, equity=equity))
    gross = sum(p['gross_pnl'] for p in trades)
    fees = sum(p['fees'] for p in trades)
    fund = sum(p['funding_pnl'] for p in trades)
    profits = sum(max(p['net_pnl'], 0.) for p in trades)
    losses = -sum(min(p['net_pnl'], 0.) for p in trades)
    floating = 0. if pos is None else (1 if pos['side']=='LONG' else -1)*pos['qty']*(one[-1][4]-pos['entry'])
    summary = dict(trades=len(trades)+(pos is not None), closed=len(trades), open=int(pos is not None),
                   outcomes=dict(collections.Counter(p['status'] for p in trades)),
                   closed_gross_pnl=gross, closed_fees=fees, closed_funding_pnl=fund,
                   closed_net_pnl=gross-fees+fund,
                   profit_factor=profits/losses if losses else None,
                   net_winners=sum(p['net_pnl'] > 0 for p in trades),
                   ending_cash=balance, ending_equity=balance+floating,
                   total_return_pct=(balance+floating-initial)/initial*100,
                   max_drawdown_minute_close_pct=max_dd*100,
                   open_price_pnl=floating, rejects=dict(rejected), setups=dict(engine.reasons))
    for row in monthly.values():
        row['equity_change_pnl']=row['equity_end']-row['equity_start']
        row['equity_return_pct']=row['equity_change_pnl']/row['equity_start']*100 if row['equity_start']>0 else None
    loss_streak=0; max_loss_streak=0
    for p in trades:
        loss_streak=loss_streak+1 if p['net_pnl']<0 else 0
        max_loss_streak=max(max_loss_streak,loss_streak)
    summary['max_consecutive_net_losses']=max_loss_streak
    summary['mean_hold_hours']=sum((p['closed_by']-p['opened'])/3600 for p in trades)/len(trades) if trades else None
    summary['positive_months']=sum(r['equity_change_pnl']>1e-8 for r in monthly.values())
    summary['negative_months']=sum(r['equity_change_pnl'] < -1e-8 for r in monthly.values())
    summary['months_with_entries']=sum(r['entries']>0 for r in monthly.values())
    summary['calendar_months']=len(monthly)
    return dict(summary=summary, trades=trades, open_position=pos, equity_daily=curve, monthly=list(monthly.values()))



def main():
    here=Path(__file__).resolve().parent
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--base-file',default=str(here/'BTC_Swing_Structure_Research_V1.py'))
    ap.add_argument('--helper-file',default=str(here/'BTC_Swing_Valid_Levels_Comparison.py'))
    ap.add_argument('--reference-run',default='/opt/btc_swing_v1_research/run_20261031_194826_583186')
    ap.add_argument('--self-test',action='store_true')
    args=ap.parse_args()
    m,helper=dependencies(args.base_file,args.helper_file)
    if args.self_test:
        self_test(m);return
    manifest,prior,fifteen,one,funding,mark=helper.cached_inputs(Path(args.reference_run))
    if m.POLICY['target_R']!=2. or m.POLICY['max_chase_R']!=.1:
        raise RuntimeError('UNEXPECTED_SHARED_EXIT_POLICY')
    print('EXPERIMENT_POLICY',json.dumps(ENTRY_POLICY),flush=True)
    print('SAME_FROZEN_DATA; NINE_PREDECLARED_VARIANTS; NO_PARAMETER_SEARCH',flush=True)
    times=[b[0] for b in one]
    results={}
    for label,seconds in [('15M',900),('1H',3600),('4H',14400)]:
        bars=m.aggregate(fifteen,seconds)
        for method in METHODS:
            key=label+'_'+method
            results[key]={}
            _,klass=classes(m)
            def factory(hs,ls,tick):
                return klass(method,hs,ls,tick)
            for segment,left,right in [('FULL_YEAR_CONTINUOUS',manifest['start'],manifest['end']),
                                       ('FIRST_PERIOD_RESEARCH',manifest['start'],manifest['split']),
                                       ('LAST90_RESEARCH',manifest['split'],manifest['end'])]:
                lo=bisect.bisect_left(times,left);hi=bisect.bisect_left(times,right)
                r=replay(m,factory,one[lo:hi],bars,[],funding,mark,left,right,
                         seconds,seconds,manifest['tick'],manifest['initial_capital'],
                         manifest['fee_bps_assumed']/10000,manifest['slippage_bps_assumed'],
                         manifest['risk_pct']/100,manifest['max_notional_equity'])
                results[key][segment]=r
                print('RESULT',key,segment,json.dumps(r['summary']),flush=True)
    output=Path(args.reference_run).parent/('entry_methods_nine_'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S_%f'))
    output.mkdir()
    metadata=dict(version=VERSION,entry_policy=ENTRY_POLICY,reference_manifest=manifest,
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        helper_sha256=HELPER_SHA,variant_count=9,
        direction='EMA20 vs EMA50 of each own closed timeframe, no higher-timeframe filter',
        stop='Reference confirmation close +/- 1.5 prior-candle Wilder ATR14; fixed at entry',
        exits='Frozen V1 execution: full 2R exit, same costs, funding, gaps and stop-first ambiguity',
        excluded_filters=['TMCO','RSI','B3','pivot direction','range gate','obstacle gate'],
        monthly='Gregorian calendar months in Iran timezone; mark-to-market equity change, partial months included',
        period_note='Full-year book continuous; first and last subperiod books reset independently and are not additive',
        validation='All reused data are exploratory, including previous holdout. No untouched validation or optimized parameters',
        source='Same reference hashes; database read-only; no network or service actions')
    (output/'manifest.json').write_text(json.dumps(metadata,indent=2),encoding='utf-8')
    (output/'results.json').write_text(json.dumps(results,indent=2,allow_nan=False),encoding='utf-8')
    compact={}
    for variant,segments in results.items():
        compact[variant]={}
        for segment,r in segments.items():
            compact[variant][segment]=r['summary']
            rows=r['trades']+([dict(r['open_position'],status='OPEN')] if r['open_position'] else [])
            if rows:
                rows=[dict(p,entry_time_iran=m.stamp(p['opened']),decision_time_iran=m.stamp(p['decision']),
                    exit_candle_open_iran=m.stamp(p['closed_candle_open']) if p.get('closed_candle_open') is not None else None)
                    for p in rows]
                write_csv(output/(variant+'_'+segment+'_trades.csv'),rows)
            write_csv(output/(variant+'_'+segment+'_monthly.csv'),r['monthly'])
    (output/'summary.json').write_text(json.dumps(compact,indent=2,allow_nan=False),encoding='utf-8')
    report=['# آزمایش ۹ روش ورود BTC','',
        'سه روش ورود روی سه تایم؛ بدون تنظیم پارامتر بر اساس نتیجه. تمام داده‌ها پژوهشی و قبلاً دیده‌شده‌اند.',
        'جهت EMA20/50 همان تایم، استاپ۱٫۵ ATR، هدف۲R، هزینه و ریسک مشترک.',
        'سرمایه هر نسخه و بخش مستقل است؛ جمع بخش‌ها لزوماً با اجرای پیوسته یک‌ساله برابر نیست.','']
    for segment in ('FULL_YEAR_CONTINUOUS','FIRST_PERIOD_RESEARCH','LAST90_RESEARCH'):
        report += ['## '+segment,'',
            '| روش | بسته | باز | هدف | استاپ/مبهم | خالص بسته | بازده با شناور | افت سرمایه | ماه مثبت/کل |',
            '|---|---:|---:|---:|---:|---:|---:|---:|---:|']
        for variant,runs in results.items():
            s=runs[segment]['summary'];o=s['outcomes']
            report.append('| %s | %d | %d | %d | %d | %.2f | %.2f%% | %.2f%% | %d/%d |' %
                (variant,s['closed'],s['open'],o.get('TARGET',0),sum(v for k,v in o.items() if k!='TARGET'),
                 s['closed_net_pnl'],s['total_return_pct'],s['max_drawdown_minute_close_pct'],
                 s['positive_months'],s['calendar_months']))
        report += ['']
    report += ['کارمزد و لغزش فرضی، فاندینگ تاریخی با برآورد ارزش تسویه. افت سرمایه بر اساس پایان دقیقه.',
        'عدد ماه مثبت از تغییر ارزش حساب شامل شناور است؛ ماه‌های ابتدایی/انتهایی می‌توانند ناقص باشند.',
        'هدف و استاپ هم‌زمان در یک دقیقه با استاپ مقدم و برچسب جدا محاسبه شده است.',
        'معامله باز اجباری بسته نشده؛ کمبود معامله یا سود مثبت گذشته، اعتبارسنجی سودآوری زنده نیست.',
        'هیچ ربات، سرویس، تلگرام یا سفارش واقعی تغییر نکرده است.']
    (output/'report_FA.md').write_text('\n'.join(report)+'\n',encoding='utf-8')
    print('REPORT_DIRECTORY',str(output))
    print('RESEARCH_COMPLETE; NINE_VARIANTS; NO BOT, SERVICE, TELEGRAM, SOURCE CACHE OR ORDER CHANGES')


def write_csv(file,rows):
    if not rows:return
    keys=sorted({k for p in rows for k in p})
    with file.open('w',newline='',encoding='utf-8-sig') as f:
        w=csv.DictWriter(f,fieldnames=keys);w.writeheader();w.writerows(rows)


def self_test(m):
    m.self_test()
    Indicators,Strategy=classes(m)
    def seeded(method,side='LONG'):
        s=Strategy(method,60,60,.1)
        s.low.ema20=100.;s.low.ema50=95. if side=='LONG' else 105.
        s.low.count=50;s.low.atr=10.
        s.low.bars=[[i*60,100,101,99,100,1] for i in range(20)]
        return s
    for side in ('LONG','SHORT'):
        b=[1200,100,104,99,103,1] if side=='LONG' else [1200,100,101,96,97,1]
        s=seeded('BREAKOUT',side)
        p=s.on_low(b,True)
        assert p and p['side']==side and p['decision']==1260
        assert abs(p['reference']-p['sl'])==15.
        s=seeded('BREAKOUT_RETEST',side)
        assert s.on_low(b,True) is None and s.setup
        c=[1260,102,105,101,104,1] if side=='LONG' else [1260,98,99,95,96,1]
        p=s.on_low(c,True)
        assert p and p['side']==side and p['decision']==1320
        s=seeded('TREND_PULLBACK',side)
        s.low.bars[-1][4]=104. if side=='LONG' else 96.
        c=[1200,101,105,100,104,1] if side=='LONG' else [1200,99,100,95,96,1]
        assert s.on_low(c,True)['side']==side
    s=seeded('BREAKOUT_RETEST')
    s.on_low([1200,100,104,99,103,1],True)
    assert s.setup
    s.on_low([1260,103,104,102,103,1],False)
    assert s.setup is None
    # Prefix causality for complete entry intent sequences, not just indicators.
    import random
    rng=random.Random(624);bars=[];close=1000.
    for i in range(500):
        o=close;close+=rng.uniform(-8,8)
        bars.append([i*60,o,max(o,close)+rng.random()*2,min(o,close)-rng.random()*2,close,1])
    for method in METHODS:
        def decisions(seq):
            s=Strategy(method,60,60,.1)
            return [s.on_low(b,True) for b in seq]
        assert decisions(bars)[:300]==decisions(bars[:300])
    print('ENTRY_METHOD_TESTS_OK: both directions, three methods, no retest same candle, '
          'fixed ATR stop, closed-bar prefix causality, disabled entry clears pending setup')


if __name__=='__main__':
    main()
