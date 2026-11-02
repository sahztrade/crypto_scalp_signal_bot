#!/usr/bin/env python3
"""Read-only BTC V6 versus Ichimoku 5m/15m. No live changes."""
import argparse,bisect,collections,datetime,hashlib,importlib.util,json,pathlib,sqlite3,time,urllib.request,urllib.parse
TZ=datetime.timezone(datetime.timedelta(hours=3,minutes=30))
EXPECTED='9aa19e404cc9b444db348d67d9a90230dd4f9d8821951eacba546d5ce5fd600c'
def stamp(t):return datetime.datetime.fromtimestamp(t,TZ).isoformat() if t is not None else None
def aggregate(bars):
 groups=collections.defaultdict(list)
 for b in bars:groups[b[0]//900*900].append(b)
 out=[]
 for t,w in sorted(groups.items()):
  w.sort()
  if [b[0] for b in w]==[t,t+300,t+600]:out.append([t,w[0][1],max(b[2] for b in w),min(b[3] for b in w),w[-1][4],sum(b[5] for b in w)])
 return out
def clouds(bars,seconds):
 out=[]
 for i,b in enumerate(bars):
  j=i-26
  if j<51:continue
  def mid(n):
   w=bars[j-n+1:j+1];return (max(x[2] for x in w)+min(x[3] for x in w))/2
  a=(mid(9)+mid(26))/2;z=mid(52);lo=min(a,z);hi=max(a,z)
  out.append(dict(at=b[0]+seconds,close=b[4],low=lo,high=hi,direction='LONG' if b[4]>hi else 'SHORT' if b[4]<lo else 'INSIDE',calculation_end=bars[j][0]+seconds))
 return out
def fetch(start,end):
 data={};cursor=end*1000-1
 while cursor>=start*1000:
  url='https://api.bybit.com/v5/market/kline?'+urllib.parse.urlencode(dict(category='linear',symbol='BTCUSDT',interval='5',end=cursor,limit=1000))
  for attempt in range(4):
   try:
    with urllib.request.urlopen(url,timeout=30) as r:a=json.load(r)
    if a.get('retCode')!=0:raise RuntimeError(str(a.get('retMsg')))
    batch=a['result']['list']
    if not batch:raise RuntimeError('EMPTY_HISTORY')
    break
   except Exception:
    if attempt==3:raise
    time.sleep(attempt+1)
  earliest=min(int(r[0]) for r in batch)
  for r in batch:
   t=int(r[0])//1000
   if start<=t and t+300<=end:data[t]=[t]+list(map(float,r[1:6]))
  if earliest>=cursor:raise RuntimeError('PAGINATION_STALLED')
  cursor=earliest-1
 bars=[data[t] for t in sorted(data)]
 if not bars or bars[0][0]!=start or bars[-1][0]+300!=end:raise RuntimeError('HISTORY_INCOMPLETE')
 if any(z[0]-b[0]!=300 for b,z in zip(bars,bars[1:])):raise RuntimeError('HISTORY_GAP')
 print('HISTORY_OK',len(bars),flush=True);return bars
def summary(rows):
 closed=[t for t in rows if t['status'] in ('TARGET','SL') and t['pnl'] is not None]
 gross=sum(t['pnl'] for t in closed);fees=sum(1000*.00055*(1+(t['tp2'] if t['status']=='TARGET' else t['sl'])/t['entry']) for t in closed)
 return dict(count=len(rows),ids=[t['id'] for t in rows],targets=[t['id'] for t in rows if t['status']=='TARGET'],stops=[t['id'] for t in rows if t['status']=='SL'],open=[t['id'] for t in rows if t['status']=='OPEN'],unknown=[t['id'] for t in rows if t['status'] not in ('OPEN','SL','TARGET')],gross=gross,fees=fees,known_closed_net=gross-fees)
def choose(rows,variant,single):
 selected=[];removed=[]
 for t in rows:
  fail=list(t['base_failures'])
  if variant!='V6':
   cl=t['cloud5'] if variant=='V6_ICHI5' else t['cloud15']
   if cl is None:fail.append('MISSING_CLOUD')
   elif cl['direction']!=t['side']:fail.append('CLOUD_NOT_ALIGNED')
  if single and not fail:
   if any(a['closed_at'] is None or a['status'] not in ('TARGET','SL') or a['closed_at']>t['opened'] for a in selected):fail.append('OVERLAP')
  if fail:removed.append(dict(id=t['id'],failures=fail))
  else:selected.append(t)
 return selected,removed

def self_test():
 bars=[[i*300,100+i,101+i,99+i,100.5+i,1] for i in range(200)]
 cs=clouds(bars,300);assert cs[0]['at']==78*300
 assert cs[-1]['calculation_end']==cs[-1]['at']-26*300
 for n in (78,100,150,199):assert clouds(bars[:n],300)==[c for c in cs if c['at']<=n*300]
 changed=[list(b) for b in bars];changed[-1][2]+=10000
 assert clouds(changed,300)[-1]['high']==cs[-1]['high']
 assert aggregate(bars[:5])==aggregate(bars[:3])
 assert len(aggregate(bars[:6]))==2
 down=[[b[0],400-b[1],400-b[3],400-b[2],400-b[4],b[5]] for b in bars]
 assert clouds(down,300)[-1]['direction']=='SHORT'
 assert cs[-1]['direction']=='LONG'
 r=dict(id=1,base_failures=[],side='LONG',cloud5={'direction':'INSIDE'},cloud15={'direction':'LONG'},opened=100,closed_at=200,status='TARGET')
 assert not choose([r],'V6_ICHI5',False)[0]
 assert len(choose([r],'V6_ICHI15',False)[0])==1
 r2=dict(r,id=2,opened=150);assert len(choose([r,r2],'V6',True)[0])==1
 print('ICHI_TEST_OK: displacement26, prefix causality, complete UTC15m, both directions, inside-cloud rejection, independent overlap')
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--self-test',action='store_true');args=ap.parse_args()
 if args.self_test:self_test();return
 p=pathlib.Path('/opt/btc_v13_research/BTC_V6_Forward_Shadow.py')
 if hashlib.sha256(p.read_bytes()).hexdigest()!=EXPECTED:raise RuntimeError('V6_CODE_HASH_MISMATCH')
 s=importlib.util.spec_from_file_location('frozen_v6',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
 with m.source('/opt/btc_dual_h1_research/dual.sqlite3') as db:
  rows=[dict(r) for r in db.execute("SELECT * FROM trades WHERE symbol='BTCUSDT' AND legacy=0 AND id>=2 ORDER BY opened,id")]
 if not rows:raise RuntimeError('NO_TRADES')
 end=min(int(time.time())//300*300,max(int(t['decision'])//300*300 for t in rows))
 start=min(int(t['decision'])//300*300 for t in rows)-1000*300
 bars=fetch(start,end);times=[b[0] for b in bars]
 c5=clouds(bars,300);c15=clouds(aggregate(bars),900)
 def latest(cs,at,step):
  i=bisect.bisect_right([c['at'] for c in cs],at)-1
  if i<0 or at-cs[i]['at']>=step:return None
  return cs[i]
 evaluated=[]
 for t in rows:
  decision=int(t['decision']);through=decision//300*300;j=bisect.bisect_left(times,through)
  history=bars[j-1000:j]
  if len(history)!=1000 or history[-1][0]+300!=through:raise RuntimeError('V6_WARMUP_MISSING')
  q=m.Context()
  for b in history:state=q.on_bar(b,b[0]+300)
  f=json.loads(t['details'] or '{}').get('filters',{});fails=[]
  if state['regime'] in ('RANGE','WARMUP'):fails.append(state['regime'])
  if abs(t['entry']-t['sl'])+1e-9<100:fails.append('STOP_LT_100')
  if f.get('ema_aligned') is not True:fails.append('EMA_NOT_CONFIRMED')
  t.update(base_failures=fails,v6_state=state,cloud5=latest(c5,decision,300),cloud15=latest(c15,decision,900))
  if not t['cloud5'] or not t['cloud15']:raise RuntimeError('CLOUD_WARMUP_MISSING '+str(t['id']))
  evaluated.append(t)
 print('WINDOW',json.dumps(dict(first_id=rows[0]['id'],last_id=rows[-1]['id'],from_iran=stamp(rows[0]['opened']),last_entry_iran=stamp(rows[-1]['opened']),snapshot_iran=stamp(time.time()),source='BYBIT_LINEAR_BTCUSDT',cloud='9/26/52, conventional displacement 26 bars; no Chikou gate; strict outside cloud',V6_warmup=1000)))
 print('ORIGINAL',json.dumps(summary(rows)))
 for single in (False,True):
  base,_=choose(evaluated,'V6',single)
  for variant in ('V6','V6_ICHI5','V6_ICHI15'):
   kept,removed=choose(evaluated,variant,single)
   print('RESULT',variant,'SINGLE' if single else 'OVERLAP',json.dumps(summary(kept)))
   baseids={t['id'] for t in base};keptids={t['id'] for t in kept}
   print('DIFFERENCE',variant,json.dumps(dict(removed_from_baseline=[t['id'] for t in base if t['id'] not in keptids],newly_freed_by_overlap=[t['id'] for t in kept if t['id'] not in baseids])))
 for t in evaluated:
  if not t['base_failures']:
   print('CANDIDATE',json.dumps(dict(id=t['id'],time_iran=stamp(t['opened']),side=t['side'],entry=t['entry'],sl=t['sl'],target=t['tp2'],status=t['status'],cloud5=t['cloud5'],cloud15=t['cloud15'])))
 print('STATIC_RECONSTRUCTION_OF_ISSUED_SIGNALS; RECORDED_EXITS; NO_NEW_ENTRY_REPLAY')
 print('1000_USDT_EACH; FEE_0.055_PERCENT_EACH_SIDE; OPEN_UNKNOWN_EXCLUDED; NO_FUNDING_SLIPPAGE')
 print('SINGLE_MODE_STARTS_EMPTY; V6_RECONSTRUCTED_WITH_1000_BAR_WARMUP; MAY_DIFFER_FROM_OLD_VISIBLE_HISTORY_STUDY')
 print('CONVENTIONAL_26_BAR_SHIFT; EXACT_TRADINGVIEW_PLOT_OFFSET_PARITY_NOT_VERIFIED')
 print('READ_ONLY_COMPLETE; NO_BOT_SHADOW_TELEGRAM_OR_ORDER_CHANGES')
if __name__=='__main__':main()
