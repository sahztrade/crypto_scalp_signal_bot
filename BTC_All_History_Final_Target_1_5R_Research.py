#!/usr/bin/env python3
"""Full recorded BTC cohort: fixed stop, final 1.5R versus 2.5R; read-only research."""
import argparse,bisect,collections,datetime,hashlib,importlib.util,json,pathlib,time
BASE='/opt/btc_v13_research/'
EXPECTED={
 'BTC_All_History_Ichimoku15_Research.py':'57b7e276cfec36d1142a9433903fc59bd7f7de9e5effbed9f1ae37b8942fb668',
 'BTC_V6_Forward_Shadow.py':'9aa19e404cc9b444db348d67d9a90230dd4f9d8821951eacba546d5ce5fd600c',
 'BTC_V6_Ichimoku_Comparison.py':'8c2cf80b555782f728a8c7ef6438882aa767cce57d345e31efbb82f79499b268'}
def load(name,file):
 p=pathlib.Path(BASE)/file
 if hashlib.sha256(p.read_bytes()).hexdigest()!=EXPECTED[file]:raise RuntimeError('DEPENDENCY_HASH '+str(p))
 s=importlib.util.spec_from_file_location(name,p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def stats(rows):
 c=collections.Counter(t['outcome']['status'] for t in rows)
 return dict(count=len(rows),targets=c['TARGET'],stops=c['SL']+c['SL_GAP'],stop_gaps=c['SL_GAP'],open=c['OPEN'],other={k:v for k,v in c.items() if k not in ('TARGET','SL','SL_GAP','OPEN')},gross=sum(t['outcome']['gross'] or 0 for t in rows),fees=sum(t['outcome']['fees'] or 0 for t in rows),net=sum(t['outcome']['net'] or 0 for t in rows))
def differences(paths):
 ids=set(t['id'] for t in paths['OHLC'])|set(t['id'] for t in paths['OLHC']);out=[]
 maps={p:{t['id']:t['outcome'] for t in ts} for p,ts in paths.items()}
 for i in sorted(ids):
  if maps['OHLC'].get(i)!=maps['OLHC'].get(i):out.append(i)
 return out

def target_aware_replay(h):
 cache={}
 def replay(t,bars,times,through,trailing,path):
  key=(t['id'],t['side'],t['entry'],t['sl'],t['tp1'],t['tp2'],t['opened'],through,trailing,path)
  if key not in cache:cache[key]=h._raw_replay(t,bars,times,through,trailing,path)
  return cache[key]
 h.replay=replay

def self_test(h):
 target_aware_replay(h)
 p=dict(id='T',side='LONG',entry=100,sl=98,tp1=103,tp2=103,opened=0)
 bs=[[60,100,103.1,99,102,1]]
 for path in ('OHLC','OLHC'):assert h._raw_replay(p,bs,[60],120,False,path)['status']=='TARGET'
 q=dict(p,id='S',side='SHORT',sl=102,tp1=97,tp2=97)
 for path in ('OHLC','OLHC'):assert h._raw_replay(q,[[60,100,101,96.9,98,1]],[60],120,False,path)['status']=='TARGET'
 ambiguous=[[60,100,104,97,101,1]]
 assert h._raw_replay(p,ambiguous,[60],120,False,'OHLC')['status']=='TARGET'
 assert h._raw_replay(p,ambiguous,[60],120,False,'OLHC')['status']=='SL'
 assert h._raw_replay(p,[[0,100,105,95,100,1]],[0],60,False,'OHLC')['status']=='OPEN'
 x=h.run_strategy([p],bs,[60],120,False,'OHLC',False)
 y=h.run_strategy([dict(p,tp2=105)],bs,[60],120,False,'OHLC',False)
 assert x[0]['outcome']['status']=='TARGET' and y[0]['outcome']['status']=='OPEN','TARGET_CACHE_CONTAMINATION'
 print('TARGET15_TEST_OK: both directions, full exit at1.5R, fixed stop, dual-path ambiguity and entry-minute exclusion',flush=True)

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--self-test',action='store_true');args=ap.parse_args()
 h=load('all_btc_frozen','BTC_All_History_Ichimoku15_Research.py');self_test(h)
 if args.self_test:return
 v6=load('v6_frozen','BTC_V6_Forward_Shadow.py');ichi=load('ichi_frozen','BTC_V6_Ichimoku_Comparison.py')
 rows,counts,dedup=h.read_sources();END=int(time.time())//60*60
 if not rows:raise RuntimeError('NO_TRADES')
 START=min(t['decision'] for t in rows);warm=START//300*300-1000*300
 one=h.history1(warm,END);times=[b[0] for b in one];five=h.agg(one,300);fifteen=h.agg(one,900)
 ft=[b[0] for b in five];clouds={c['at']:c for c in ichi.clouds(fifteen,900)};states={};evaluated=[]
 for i,t in enumerate(rows):
  at=t['decision'];end=at//300*300;j=bisect.bisect_left(ft,end);bars=five[j-1000:j]
  if len(bars)!=1000 or bars[-1][0]+300!=end:raise RuntimeError('HISTORY_MISSING '+t['id'])
  cl=clouds.get(at//900*900)
  if cl is None:raise RuntimeError('CLOUD_MISSING '+t['id'])
  cs=[b[4] for b in bars[-100:]];e20=h.ema(cs,20,True);e50=h.ema(cs,50,True)
  aligned=e20>e50 and cs[-1]>e20 if t['side']=='LONG' else e20<e50 and cs[-1]<e20
  fails=[]
  if abs(t['entry']-t['sl'])+1e-9<100:fails.append('STOP_LT100')
  if not aligned:fails.append('EMA')
  if cl['direction']!=t['side']:fails.append('ICHI15')
  state=None
  if not fails:
   if end not in states:
    q=v6.Context()
    for b in bars:state=q.on_bar(b,b[0]+300)
    states[end]=state
   state=states[end]
   if state['regime'] in ('RANGE','WARMUP'):fails.append('V6_RANGE')
  t.update(filter_failures=fails,cloud15=cl,v6_state=state,ema_aligned=aligned)
  evaluated.append(t)
  if (i+1)%100==0:print('EVALUATED',i+1,flush=True)
 print('METADATA',json.dumps(dict(start=h.stamp(START),end=h.stamp(END),counts=counts,dedup=dedup,notional_usdt=1000,fee_each_side=.00055,management='Fixed initial stop; full final exit at 1.5R or 2.5R; no partials or trailing',new_filters='V6 non-RANGE + EMA aligned + stop>=100USD + ICHI15 aligned',cohort='All recorded filled entries; no newly generated TK opportunities')))
 outputs={};summaries=[];flags=[]
 for group in ('ALL','OLD_V4','DUAL'):
  own=evaluated if group=='ALL' else [t for t in evaluated if t['group']==group]
  for gate in ('NORMAL','NEW_FILTERS'):
   candidates=own if gate=='NORMAL' else [t for t in own if not t['filter_failures']]
   for multiple in (1.5,2.5):
    prepared=[dict(t,tp2=t['tp1'] if multiple==1.5 else t['tp2']) for t in candidates]
    for single in (False,True):
     paths={}
     for path in ('OHLC','OLHC'):
      kept=h.run_strategy(prepared,one,times,END,False,path,single)
      key='|'.join((group,gate,str(multiple)+'R','SINGLE' if single else 'OVERLAP',path));outputs[key]=kept;paths[path]=kept
      result=dict(key=key,**stats(kept));summaries.append(result);print('RESULT',json.dumps(result),flush=True)
     diff=differences(paths)
     if diff:
      item=dict(group=group,gate=gate,target=multiple,single=single,ids=diff);flags.append(item);print('PATH_DEPENDENT',json.dumps(item),flush=True)
 folder=pathlib.Path('/opt/btc_target15_research');folder.mkdir(exist_ok=True);out=folder/('run_'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S_%f'));out.mkdir()
 (out/'summary.json').write_text(json.dumps(dict(start=h.stamp(START),end=h.stamp(END),counts=counts,dedup=dedup,summaries=summaries,path_dependent=flags),indent=2),encoding='utf-8')
 (out/'results.json').write_text(json.dumps(dict(entries=evaluated,outputs=outputs),indent=2),encoding='utf-8')
 print('REPORT_DIRECTORY',out,flush=True)
 print('TWO_OHLC_PATH_SCENARIOS_NOT_EXHAUSTIVE_TICK_PROOF; ENTRY_MINUTE_EXCLUDED')
 print('SINGLE_SELECTION_REBUILT_FOR_EACH_TARGET; CLOSED_FEES_INCLUDED; OPEN_UNKNOWN_EXCLUDED; NO_FUNDING_SLIPPAGE')
 print('NORMAL_IS_NO_EXTRA_FILTER_NOT_ORIGINAL_HISTORICAL_TARGETS; NEW_FILTERS_DO_NOT_GENERATE_UNRECORDED_TK_SIGNALS')
 print('READ_ONLY_SOURCES; REPORT_FILES_ONLY; NO_BOT_SHADOW_TELEGRAM_OR_ORDER_CHANGES')
if __name__=='__main__':main()
