#!/usr/bin/env python3
"""Causal local context research. No live mode, network, orders or service changes."""
import json, argparse, math
from pathlib import Path
VERSION='BTC-5M-CONTEXT-V6-RESEARCH'
POLICY={'windows':[6,12],'er_max':.35,'regression_drift_max':.45,'overlap_min':.35,'crossings':[2,3],'confirmation':2,'stale':2,'directional_er':.60,'directional_drift':.55,'break_buffer_atr':.15,'entry_er':.35}
class Context:
 def __init__(self):
  self.bars=[]; self.box=None; self.range_count=0; self.break_side=None; self.break_count=0; self.escape_count=0; self.age=0
 def on_bar(self,b,cutoff):
  b=list(b); t,o,h,l,c=b[:5]
  if t+300>cutoff: raise ValueError('OPEN_CANDLE')
  if not all(math.isfinite(float(x)) for x in b[:5]) or not l<=min(o,c)<=max(o,c)<=h: raise ValueError('INVALID_BAR')
  if self.bars and t<=self.bars[-1][0]: raise ValueError('DUPLICATE_OR_ORDER')
  if self.bars and t-self.bars[-1][0]!=300: raise ValueError('GAP')
  prior=self.bars[-2:]; self.bars.append(b)
  if len(self.bars)<15:return {'at':t+300,'regime':'WARMUP','entry_side':None}
  recent=self.bars[-15:]; atr=sum(max(x[2]-x[3],abs(x[2]-p[4]),abs(x[3]-p[4])) for p,x in zip(recent,recent[1:]))/14
  def features(n):
   w=self.bars[-n:]; cs=[x[4] for x in w]; lo=min(x[3] for x in w);hi=max(x[2] for x in w)
   travel=sum(abs(y-x) for x,y in zip(cs,cs[1:])); er=abs(cs[-1]-cs[0])/travel if travel else 0
   center=sum(cs)/n;crossings=sum((a-center)*(z-center)<0 for a,z in zip(cs,cs[1:]))
   mid=(n-1)/2; slope=sum((i-mid)*(x-center) for i,x in enumerate(cs))/sum((i-mid)**2 for i in range(n))
   drift=abs(slope)*(n-1)/max(hi-lo,1e-9)
   overlap=sum(max(0,min(a[2],z[2])-max(a[3],z[3]))/max(min(a[2]-a[3],z[2]-z[3]),1e-9) for a,z in zip(w,w[1:]))/(n-1)
   return dict(er=er,drift=drift,crossings=crossings,overlap=overlap,low=lo,high=hi)
  fast=features(6); slow=features(12)
  def horizontal(f,min_cross):return f['er']<=.35 and f['drift']<=.45 and f['crossings']>=min_cross and f['overlap']>=.35
  # Short horizon discovers a new plateau; slow horizon cannot override a directional short leg.
  fast_range=horizontal(fast,2)
  fast_trend=fast['er']>=.60 and fast['drift']>=.55
  candidate=fast_range or (horizontal(slow,3) and not fast_trend and fast['overlap']>=.35)
  self.range_count=self.range_count+1 if candidate else 0
  self.age=0 if candidate else self.age+1
  if self.box:
   side='UP' if c>self.box['high']+self.box['buffer'] else 'DOWN' if c<self.box['low']-self.box['buffer'] else None
   self.break_count=self.break_count+1 if side and side==self.break_side else (1 if side else 0);self.break_side=side
   if self.break_count>=2 or self.age>=2 or fast_trend:
    self.box=None;self.range_count=0
  if self.box is None and self.range_count>=2 and not fast_trend:
   f=fast if fast_range else slow
   self.box={'low':f['low'],'high':f['high'],'buffer':.15*atr};self.break_count=0;self.break_side=None
  er=slow['er'];drift=slow['drift'];crossings=slow['crossings']
  # A fresh local range does not inherit prior structural direction.
  r=self.bars[-6:]; moves=sum(abs(y[4]-x[4]) for x,y in zip(r,r[1:])); signed=(r[-1][4]-r[0][4])/moves if moves else 0
  direction='UP' if signed>=.35 else 'DOWN' if signed<=-.35 else 'FLAT'
  # Resume evidence uses only levels known before the current bar.
  resume='UP' if prior and c>max(x[2] for x in prior) else 'DOWN' if prior and c<min(x[3] for x in prior) else None
  entry=direction if not self.box and resume==direction else None
  return {'at':t+300,'regime':'RANGE' if self.box else 'NON_RANGE' if direction!='FLAT' else 'UNCERTAIN','local_direction':direction,'entry_side':entry,'box':dict(self.box) if self.box else None,'er12':er,'drift12':drift,'crossings':crossings,'reason':'LOCAL_BOX_BLOCK' if self.box else 'RESUMPTION_CONFIRMED' if entry else 'WAIT_RESUMPTION'}
def self_test():
 bars=[[i*300,100+i,101+i,99+i,100.5+i,1] for i in range(35)]
 a=Context();out=[a.on_bar(b,b[0]+300) for b in bars]
 for n in range(15,35):
  q=Context(); got=[q.on_bar(b,b[0]+300) for b in bars[:n]];assert got==out[:n]
 assert out[-1]['entry_side']=='UP'
 try:a.on_bar(bars[-1],bars[-1][0]+300);raise AssertionError('duplicate')
 except ValueError:pass
 try:Context().on_bar(bars[0],299);raise AssertionError('open')
 except ValueError:pass
 down=[[i*300,200-i,201-i,199-i,199.5-i,1] for i in range(35)]
 q=Context();z=[q.on_bar(b,b[0]+300) for b in down];assert z[-1]['entry_side']=='DOWN'
 print('V6_SELF_TEST_OK: prefix causality, both directions, known levels, duplicate/open rejection')

import sqlite3,time,urllib.request,urllib.parse,datetime,hashlib
IRAN=datetime.timezone(datetime.timedelta(hours=3,minutes=30))
VERSION='BTC-V6-STOP100-EMA-SINGLE-SHADOW-1'
def source(path):
 c=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=10);c.row_factory=sqlite3.Row;c.execute('PRAGMA query_only=ON');return c
def fetch(start,end):
 bars={};cursor=end*1000-1
 while cursor>=start*1000:
  url='https://api.bybit.com/v5/market/kline?'+urllib.parse.urlencode(dict(category='linear',symbol='BTCUSDT',interval='5',end=cursor,limit=1000))
  with urllib.request.urlopen(url,timeout=20) as r:a=json.load(r)
  if a.get('retCode')!=0:raise RuntimeError('BYBIT '+str(a.get('retMsg')))
  batch=a['result']['list']
  if not batch:raise RuntimeError('EMPTY_HISTORY')
  earliest=min(int(r[0]) for r in batch)
  for r in batch:
   t=int(r[0])//1000
   if start<=t and t+300<=end:bars[t]=[t]+list(map(float,r[1:6]))
  if earliest>=cursor:raise RuntimeError('PAGINATION')
  cursor=earliest-1
 out=[bars[k] for k in sorted(bars)]
 if not out or out[0][0]!=start or out[-1][0]+300!=end:raise RuntimeError('MISSING_HISTORY')
 if any(z[0]-a[0]!=300 for a,z in zip(out,out[1:])):raise RuntimeError('GAP')
 return out
def classify(t,state,busy):
 f=json.loads(t['details']).get('filters',{})
 failures=[]
 if state['regime'] in ('RANGE','WARMUP'):failures.append(state['regime'])
 if abs(t['entry']-t['sl'])+1e-9<100:failures.append('STOP_LT_100')
 if f.get('ema_aligned') is not True:failures.append('EMA_NOT_CONFIRMED')
 if busy:failures.append('OVERLAP')
 return failures

def report(db,folder):
 rows=[dict(r) for r in db.execute('SELECT * FROM reviews ORDER BY id')];selected=[r for r in rows if r['accepted']]
 closed=[r for r in selected if r['status'] in ('TARGET','SL')];gross=sum(r['pnl'] or 0 for r in closed);fees=sum(r['fee'] or 0 for r in closed)
 data=dict(version=VERSION,updated_iran=datetime.datetime.now(IRAN).isoformat(),started=json.loads(db.execute("SELECT value FROM metadata WHERE key='start'").fetchone()[0]),evaluated=len(rows),selected=len(selected),targets=sum(r['status']=='TARGET' for r in selected),stops=sum(r['status']=='SL' for r in selected),open=sum(r['status']=='OPEN' for r in selected),unknown=sum(r['status']=='UNKNOWN' for r in selected),gross=gross,fees=fees,net=gross-fees,rows=rows,limitations='Shadow of issued signals only; fixed recorded executions; no new entries, funding or slippage; unknown remains blocking')
 tmp=folder/'report.tmp';tmp.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8');tmp.replace(folder/'report.json')
 return data

def watch(args):
 folder=Path(args.data_dir);folder.mkdir(parents=True,exist_ok=True)
 import fcntl
 lock=(folder/'watch.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 db=sqlite3.connect(folder/'shadow.sqlite3');db.row_factory=sqlite3.Row
 db.executescript('CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT);CREATE TABLE IF NOT EXISTS reviews(id INTEGER PRIMARY KEY,decision REAL,opened REAL,side TEXT,entry REAL,sl REAL,tp2 REAL,accepted INTEGER,failures TEXT,state TEXT,status TEXT,closed_at REAL,pnl REAL,fee REAL);')
 with source(args.source) as c:
  if not db.execute("SELECT 1 FROM metadata WHERE key='start'").fetchone():
   watermark=c.execute('SELECT coalesce(max(id),0) FROM trades').fetchone()[0];start=time.time()
   with db:
    db.execute('INSERT INTO metadata VALUES(?,?)',('start',json.dumps(dict(at=start,iran=datetime.datetime.fromtimestamp(start,IRAN).isoformat(),watermark=watermark,policy=POLICY))))
    db.execute('INSERT INTO metadata VALUES(?,?)',('checkpoint',str(watermark)))
 print('SHADOW_STARTED: existing trades skipped; original database read-only',flush=True)
 while True:
  try:
   with source(args.source) as c:
    # Update accepted positions before evaluating later signals; compare actual close time to entry time.
    for own in db.execute('SELECT * FROM reviews WHERE accepted=1').fetchall():
     t=c.execute('SELECT * FROM trades WHERE id=?',(own['id'],)).fetchone()
     if not t:raise RuntimeError('SOURCE_TRADE_DISAPPEARED')
     fee=None
     if t['status'] in ('TARGET','SL') and t['pnl'] is not None:
      exitprice=t['tp2'] if t['status']=='TARGET' else t['sl'];fee=1000*.00055*(1+exitprice/t['entry'])
     with db:db.execute('UPDATE reviews SET status=?,closed_at=?,pnl=?,fee=? WHERE id=?',(t['status'],t['closed_at'],t['pnl'],fee,t['id']))
    checkpoint=int(db.execute("SELECT value FROM metadata WHERE key='checkpoint'").fetchone()[0])
    pending=c.execute('SELECT * FROM trades WHERE id>? ORDER BY opened,id',(checkpoint,)).fetchall()
    for t in pending:
     if t['symbol']!='BTCUSDT' or t['legacy']:raise RuntimeError('UNEXPECTED_SOURCE_TRADE')
     end=int(t['decision'])//300*300
     q=Context();states=[q.on_bar(b,b[0]+300) for b in fetch(end-1000*300,end)];state=states[-1]
     busy=db.execute("SELECT 1 FROM reviews WHERE accepted=1 AND (closed_at IS NULL OR status NOT IN ('TARGET','SL') OR closed_at>?) LIMIT 1",(t['opened'],)).fetchone() is not None
     failures=classify(t,state,busy)
     with db:
      db.execute('INSERT INTO reviews VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(t['id'],t['decision'],t['opened'],t['side'],t['entry'],t['sl'],t['tp2'],int(not failures),json.dumps(failures),json.dumps(state),t['status'],t['closed_at'],t['pnl'],None))
      db.execute("UPDATE metadata SET value=? WHERE key='checkpoint'",(str(t['id']),))
     print(json.dumps(dict(id=t['id'],accepted=not failures,failures=failures,state_at=state['at'])),flush=True)
   report(db,folder)
   if args.once:return
  except Exception as e:
   print('ERROR: '+type(e).__name__+': '+str(e),flush=True)
   if args.once:raise
  time.sleep(15)

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--watch',action='store_true');ap.add_argument('--once',action='store_true');ap.add_argument('--report',action='store_true');ap.add_argument('--self-test',action='store_true');ap.add_argument('--source',default='/opt/btc_dual_h1_research/dual.sqlite3');ap.add_argument('--data-dir',default='/opt/btc_v6_forward_shadow');args=ap.parse_args()
 if args.self_test:
  self_test()
  t=dict(details=json.dumps({'filters':{'ema_aligned':True}}),entry=78000,sl=77900)
  s={'regime':'NON_RANGE'}
  assert classify(t,s,False)==[]
  assert classify(t,s,True)==['OVERLAP']
  assert classify(t,{'regime':'RANGE'},False)==['RANGE']
  t['sl']=77901;assert 'STOP_LT_100' in classify(t,s,False)
  t['details']='{}';assert 'EMA_NOT_CONFIRMED' in classify(t,s,False)
  print('SHADOW_POLICY_TEST_OK: stop boundary, EMA missing, range and overlap')
  return
 if args.report:
  db=sqlite3.connect(Path(args.data_dir)/'shadow.sqlite3');db.row_factory=sqlite3.Row;print(json.dumps(report(db,Path(args.data_dir)),ensure_ascii=False,indent=2));return
 if not(args.watch or args.once):ap.error('choose --watch, --once, --report or --self-test')
 watch(args)
if __name__=='__main__':main()
