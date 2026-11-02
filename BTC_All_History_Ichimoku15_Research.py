#!/usr/bin/env python3
"""All available BTC paper entries: Ichimoku15 filter and independent TK15; research only."""
import argparse,bisect,collections,datetime,hashlib,importlib.util,json,math,pathlib,sqlite3,time,urllib.request,urllib.parse
from decimal import Decimal,ROUND_HALF_EVEN,ROUND_FLOOR,ROUND_CEILING
TZ=datetime.timezone(datetime.timedelta(hours=3,minutes=30))
COMPARE_SHA='8c2cf80b555782f728a8c7ef6438882aa767cce57d345e31efbb82f79499b268'
SHADOW_SHA='9aa19e404cc9b444db348d67d9a90230dd4f9d8821951eacba546d5ce5fd600c'
VERSION='BTC-ALL-ICHI15-TK15-RESEARCH-V1'
def stamp(t):return datetime.datetime.fromtimestamp(t,TZ).isoformat() if t is not None else None
def load(name,path,sha):
 p=pathlib.Path(path)
 if hashlib.sha256(p.read_bytes()).hexdigest()!=sha:raise RuntimeError('CODE_HASH '+str(p))
 s=importlib.util.spec_from_file_location(name,p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
def readonly(path):
 p=pathlib.Path(path)
 if not p.is_file():raise RuntimeError('SOURCE_MISSING '+str(p))
 c=sqlite3.connect(p.resolve().as_uri()+'?mode=ro',uri=True,timeout=20);c.row_factory=sqlite3.Row;c.execute('PRAGMA query_only=ON');return c

def normalize(identity,group,side,entry,stop,opened,decision,original_targets,details=None):
 vals=[float(v) for v in (entry,stop,opened,decision)]
 if not all(math.isfinite(v) for v in vals):raise RuntimeError('INVALID_NUMBERS '+identity)
 entry,stop,opened,decision=vals;sign=1 if side=='LONG' else -1
 if side not in ('LONG','SHORT') or min(entry,stop)<=0 or sign*(entry-stop)<=0:raise RuntimeError('INVALID_SIDE_STOP '+identity)
 if decision>opened:raise RuntimeError('FUTURE_DECISION '+identity)
 risk=sign*(entry-stop)
 tp1=quantize(entry+sign*1.5*risk,.1);tp2=quantize(entry+sign*2.5*risk,.1)
 if not 0<sign*(tp1-entry)<sign*(tp2-entry):raise RuntimeError('TARGET_COLLISION '+identity)
 return dict(id=identity,group=group,side=side,entry=entry,sl=stop,opened=opened,decision=int(decision),tp1=tp1,tp2=tp2,original_targets=original_targets,details=details or {},stop_pct=100*risk/entry)

def epoch(value):
 if value is None:return None
 if isinstance(value,(int,float)):return float(value)
 try:return float(value)
 except (ValueError,TypeError):
  parsed=datetime.datetime.fromisoformat(str(value).replace('Z','+00:00'))
  if parsed.tzinfo is None:raise RuntimeError('NAIVE_SOURCE_TIMESTAMP')
  return parsed.timestamp()
def read_sources(root='/opt'):
 rows=[];counts={};dedup=[];old_by_id={};excluded=collections.Counter()
 c=readonly(pathlib.Path(root)/'tmco_webhook_v4/data/v4_paper_live.db')
 try:
  rs=c.execute('SELECT t.*,c.decision_close AS confirmation_close FROM paper_trades_v4 t LEFT JOIN paper_confirmations c ON c.epoch=t.epoch ORDER BY t.signal_id').fetchall();counts['OLD_V4_RAW']=len(rs)
  for raw in rs:
   r=dict(raw)
   if r.get('entry_status')!='filled':excluded['NOT_FILLED']+=1;continue
   if r.get('entry_filled_at') is None:excluded['NO_RECORDED_FILL_TIME']+=1;continue
   opened=epoch(r['entry_filled_at']);decision=epoch(r.get('confirmation_close') if r.get('confirmation_close') is not None else r.get('decision_ts'))
   if decision is None:excluded['NO_DECISION_TIME']+=1;continue
   n=normalize('V4:'+str(r['signal_id']),'OLD_V4',r['direction'],r['entry'],r['sl'],opened,decision,[r.get('tp1'),r.get('tp2'),r.get('tp3')],dict(original_status=r.get('trade_status'),entry_minute_unverified=r.get('entry_minute_unverified')))
   rows.append(n);old_by_id[r['signal_id']]=n
  counts['OLD_V4_FILLED_INCLUDED']=len(rows);counts['OLD_V4_EXCLUDED']=dict(excluded)
 finally:c.close()
 c=readonly(pathlib.Path(root)/'btc_dual_h1_research/dual.sqlite3')
 try:
  rs=c.execute("SELECT * FROM trades WHERE symbol='BTCUSDT' ORDER BY opened,id").fetchall();counts['DUAL_RAW']=len(rs)
  for r in rs:
   details=json.loads(r['details'] or '{}');legacy_id=details.get('legacy_signal_id')
   if r['legacy']:
    previous=old_by_id.get(legacy_id)
    if previous is None:raise RuntimeError('LEGACY_ENTRY_NOT_FOUND '+str(legacy_id))
    if previous['side']!=r['side'] or abs(previous['entry']-r['entry'])>1e-6 or abs(previous['sl']-r['sl'])>1e-6 or abs(previous['opened']-r['opened'])>.01:raise RuntimeError('LEGACY_MISMATCH')
    dedup.append(dict(dual_id=r['id'],original=previous['id']));continue
   rows.append(normalize('DUAL:'+str(r['id']),'DUAL',r['side'],r['entry'],r['sl'],r['opened'],r['decision'],[r['tp1'],r['tp2']],details))
 finally:c.close()
 if len({r['id'] for r in rows})!=len(rows):raise RuntimeError('DUPLICATE_ID')
 counts['DUAL_UNIQUE']=counts['DUAL_RAW']-len(dedup);counts['TOTAL_UNIQUE']=len(rows)
 return sorted(rows,key=lambda r:(r['opened'],r['id'])),counts,dedup
def rsi(values, length):
    output = [None]*len(values)
    if len(values)<=length:
        return output
    changes = [b-a for a,b in zip(values,values[1:])]
    gain = sum(max(x,0) for x in changes[:length])/length
    loss = sum(max(-x,0) for x in changes[:length])/length
    def value():
        return 50. if gain==loss==0 else 100. if loss==0 else 100-100/(1+gain/loss)
    output[length] = value()
    for i in range(length,len(changes)):
        gain = (gain*(length-1)+max(changes[i],0))/length
        loss = (loss*(length-1)+max(-changes[i],0))/length
        output[i+1] = value()
    return output
def ema(values, length, seeded=False):
    out = []
    previous = None
    if seeded:
        previous = sum(values[:length])/length
        values = values[length:]
    for value in values:
        if value is None:
            out.append(None)
            continue
        previous = value if previous is None else previous+(value-previous)*2/(length+1)
        out.append(previous)
    return previous if seeded else out
def atr(bars):
    tr = [max(b[2]-b[3],abs(b[2]-a[4]),abs(b[3]-a[4])) for a,b in zip(bars,bars[1:])]
    if len(tr)<14:
        raise ValueError('INSUFFICIENT_ATR')
    result = sum(tr[:14])/14
    for value in tr[14:]:
        result = (result*13+value)/14
    return result
def filters(side, frames):
    one,three,five = frames['1m'][-240:],frames['3m'][-80:],frames['5m'][-100:]
    r1,r3,r5 = [rsi([b[4] for b in bars],14)[-1] for bars in (one,three,five)]
    closes = [b[4] for b in five]
    e20,e50 = ema(closes,20,True),ema(closes,50,True)
    aligned = e20>e50 and closes[-1]>e20 if side=='LONG' else e20<e50 and closes[-1]<e20
    rsi_ok = 45<=r3<=70 and r5>=45 if side=='LONG' else 30<=r3<=55 and r5<=55
    avg = sum(b[5] for b in one[-30:-1])/29
    volume = one[-1][5]/avg if avg>0 else 0
    b = one[-1]
    body = abs(b[4]-b[1])/(b[2]-b[3]) if b[2]>b[3] else 0
    body_ok = (b[4]>b[1] if side=='LONG' else b[4]<b[1]) and body>=.35
    peak,drop = five[-48][2],0
    for b in five[-47:]:
        drop = max(drop,(peak-b[3])/peak)
        peak = max(peak,b[2])
    lows = [five[i][3] for i in range(len(five)-46,len(five)-2)
            if all(five[i][3]<five[j][3] for j in (i-2,i-1,i+1,i+2))]
    return dict(score=2*int(aligned)+2*int(rsi_ok)+int(volume>=1)+int(body_ok),
                b3_reject=bool(side=='SHORT' and drop>=.005 and len(lows)>=2 and lows[-1]>lows[-2]),
                ema_aligned=bool(aligned),rsi_aligned=bool(rsi_ok),rsi_1m=r1,rsi_3m=r3,
                rsi_5m=r5,volume_ratio=volume)
def quantize(value, tick, rounding=ROUND_HALF_EVEN):
    q = Decimal(str(tick))
    return float((Decimal(str(value))/q).to_integral_value(rounding=rounding)*q)
def levels(side, entry, five, tick):
    if len(five)!=100:
        raise ValueError('EXPECTED_100_5M')
    sign = 1 if side=='LONG' else -1
    a = atr(five)
    stop = min(min(b[3] for b in five[-6:]),entry-1.2*a) if sign==1 else max(max(b[2] for b in five[-6:]),entry+1.2*a)
    stop = quantize(stop,tick,ROUND_FLOOR if sign==1 else ROUND_CEILING)
    risk = sign*(entry-stop)
    tp1,tp2 = [quantize(entry+sign*risk*k,tick) for k in (1.5,2.5)]
    if risk<=0 or not (0<sign*(tp1-entry)<sign*(tp2-entry)):
        raise ValueError('INVALID_LEVELS_AFTER_TICK_ROUNDING')
    return stop,tp1,tp2

def history1(start,end):
 data={};cursor=end*1000-1
 while cursor>=start*1000:
  url='https://api.bybit.com/v5/market/kline?'+urllib.parse.urlencode(dict(category='linear',symbol='BTCUSDT',interval='1',end=cursor,limit=1000))
  for attempt in range(4):
   try:
    with urllib.request.urlopen(url,timeout=30) as r:a=json.load(r)
    if a.get('retCode')!=0:raise RuntimeError(str(a.get('retMsg')))
    batch=a['result']['list']
    if not batch:raise RuntimeError('EMPTY')
    break
   except Exception:
    if attempt==3:raise
    time.sleep(attempt+1)
  earliest=min(int(r[0]) for r in batch)
  for r in batch:
   t=int(r[0])//1000
   if start<=t and t+60<=end:data[t]=[t]+list(map(float,r[1:6]))
  if earliest>=cursor:raise RuntimeError('PAGINATION')
  cursor=earliest-1
 bars=[data[t] for t in sorted(data)]
 if not bars or bars[0][0]!=start or bars[-1][0]+60!=end:raise RuntimeError('MISSING_HISTORY')
 if any(b[0]-a[0]!=60 for a,b in zip(bars,bars[1:])):raise RuntimeError('HISTORY_GAP')
 print('HISTORY_1M_OK',len(bars),flush=True);return bars
def agg(bars,seconds):
 groups=collections.defaultdict(list)
 for b in bars:groups[b[0]//seconds*seconds].append(b)
 out=[]
 for t,w in sorted(groups.items()):
  if [b[0] for b in w]!=list(range(t,t+seconds,60)):continue
  out.append([t,w[0][1],max(b[2] for b in w),min(b[3] for b in w),w[-1][4],sum(b[5] for b in w)])
 return out
def replay(t,bars,times,through,trailing,path):
 sign=1 if t['side']=='LONG' else -1;entry=t['entry'];stop=t['sl'];hit1=False
 start=int(t['opened'])//60*60+60
 def close(status,price,at):
  gross=1000*sign*(price-entry)/entry;fee=1000*.00055*(1+price/entry)
  return dict(status=status,exit=price,closed_at=at,gross=gross,fees=fee,net=gross-fee,hit1=hit1)
 for b in bars[bisect.bisect_left(times,start):]:
  if b[0]+60>through:break
  o,h,l,c=b[1:5];at=b[0]+60
  if sign*(o-stop)<=0:return close(('BE' if o==entry else 'BE_GAP') if stop==entry else ('SL' if o==stop else 'SL_GAP'),o,at)
  if sign*(o-t['tp2'])>=0:return close('TARGET',o,at)
  if sign*(o-t['tp1'])>=0:
   hit1=True
   if trailing:stop=entry
  vertices=[o,h,l,c] if path=='OHLC' else [o,l,h,c]
  for a,z in zip(vertices,vertices[1:]):
   # Traverse levels in actual assumed segment order; no future extreme used at entry.
   levels=[(stop,'STOP'),(t['tp2'],'FINAL')]
   if not hit1:levels.append((t['tp1'],'FIRST'))
   events=sorted([(abs(level-a),kind,level) for level,kind in levels if min(a,z)<=level<=max(a,z)],key=lambda e:e[0])
   for _,kind,level in events:
    if kind=='STOP':return close('BE' if stop==entry else 'SL',level,at)
    if kind=='FINAL':return close('TARGET',level,at)
    hit1=True
    if trailing:stop=entry
 return dict(status='OPEN',closed_at=None,gross=None,fees=None,net=None,hit1=hit1)
def summarize(xs):
 return dict(count=len(xs),ids=[t['id'] for t in xs],outcomes=dict(collections.Counter(t['outcome']['status'] for t in xs)),gross=sum(t['outcome']['gross'] or 0 for t in xs),fees=sum(t['outcome']['fees'] or 0 for t in xs),net=sum(t['outcome']['net'] or 0 for t in xs),open_ids=[t['id'] for t in xs if t['outcome']['status']=='OPEN'])
def run_strategy(candidates,one,times,end,trailing,path,single):
 kept=[];free=-math.inf
 for t in sorted(candidates,key=lambda x:(x['opened'],str(x['id']))):
  if single and t['opened']<free:continue
  outcome=replay(t,one,times,end,trailing,path);kept.append(dict(t,outcome=outcome))
  if single:free=outcome['closed_at'] if outcome['closed_at'] is not None else math.inf
 return kept
def ichi_entries(fifteen,clouds,start,end):
 cs={c['at']:c for c in clouds};result=[]
 def mid(i,n):
  if i<n-1:return None
  w=fifteen[i-n+1:i+1];return (max(b[2] for b in w)+min(b[3] for b in w))/2
 for i,b in enumerate(fifteen):
  at=b[0]+900
  if not start<=at<=end or i<1 or at not in cs or at-900 not in cs:continue
  current=cs[at];previous=cs[at-900];side=current['direction']
  if side not in ('LONG','SHORT'):continue
  # Rule A: first closed 15m candle emerging on that side of the displaced cloud.
  if previous['direction']!=side:result.append(dict(rule='CLOUD_EXIT15',at=at,side=side))
  ten,kij,pt,pk=mid(i,9),mid(i,26),mid(i-1,9),mid(i-1,26)
  if None not in (ten,kij,pt,pk):
   cross=(pt<=pk and ten>kij) if side=='LONG' else (pt>=pk and ten<kij)
   if cross:result.append(dict(rule='TK_CROSS15',at=at,side=side))
 return result

# Cache replay outcomes once. Different filters do not alter a retained entry's prices.
_raw_replay=replay
REPLAY_CACHE={}
def replay(t,bars,times,through,trailing,path):
 key=(t['id'],through,trailing,path)
 if key not in REPLAY_CACHE:REPLAY_CACHE[key]=_raw_replay(t,bars,times,through,trailing,path)
 return REPLAY_CACHE[key]

def self_test():
 n=normalize('T','OLD_V4','LONG',78000,77800,100,99,[])
 assert n['tp1']==78300 and n['tp2']==78500
 s=normalize('S','OLD_V4','SHORT',78000,78200,100,99,[])
 assert s['tp1']==77700 and s['tp2']==77500
 t=dict(n,opened=0,entry=100,sl=98,tp1=103,tp2=105)
 b=[[60,100,104,99,101,1]]
 assert _raw_replay(t,b,[60],120,True,'OHLC')['status']=='BE'
 assert _raw_replay(t,b,[60],120,True,'OLHC')['status']=='OPEN'
 assert _raw_replay(t,b,[60],120,False,'OHLC')['status']=='OPEN'
 assert epoch('2026-10-30T00:00:00+00:00')==1793318400
 assert agg([[i*60,100,101,99,100,1] for i in range(4)],300)==[]
 print('BTC_ALL_ICHI_TEST_OK: initial risk, both-side targets, timestamp parsing, trailing ambiguity, fixed exits, complete bars')
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--self-test',action='store_true');a=ap.parse_args()
 if a.self_test:self_test();return
 comp=load('ichi_math','/opt/btc_v13_research/BTC_V6_Ichimoku_Comparison.py',COMPARE_SHA)
 model=load('v6_math','/opt/btc_v13_research/BTC_V6_Forward_Shadow.py',SHADOW_SHA)
 rows,counts,dedup=read_sources();END=int(time.time())//60*60
 if not rows:raise RuntimeError('NO_ADA_ENTRIES')
 if any(t['opened']>END or t['decision']>END for t in rows):raise RuntimeError('RECENT_ENTRY_NOT_YET_CLOSED_MINUTE; RERUN_AFTER_ONE_MINUTE')
 START=min(t['decision'] for t in rows);history_start=START//300*300-1000*300
 one=history1(history_start,END);times=[b[0] for b in one];five=agg(one,300);three=agg(one,180);fifteen=agg(one,900)
 five_times=[b[0] for b in five];five_ends=[b[0]+300 for b in five];three_ends=[b[0]+180 for b in three]
 cloudlist=comp.clouds(fifteen,900);cloudmap={c['at']:c for c in cloudlist}
 v6cache={}
 def v6_at(at):
  end=at//300*300
  if end not in v6cache:
   j=bisect.bisect_left(five_times,end);bs=five[j-1000:j]
   if len(bs)!=1000 or bs[-1][0]+300!=end:raise RuntimeError('V6_WARMUP')
   q=model.Context()
   for b in bs:state=q.on_bar(b,b[0]+300)
   v6cache[end]=state
  return v6cache[end]
 def frames_at(at):
  i=bisect.bisect_left(times,at);j=bisect.bisect_right(three_ends,at);k=bisect.bisect_right(five_ends,at)
  frames={'1m':one[i-240:i],'3m':three[j-80:j],'5m':five[k-100:k]}
  if [len(frames[x]) for x in ('1m','3m','5m')]!=[240,80,100]:raise RuntimeError('FILTER_HISTORY')
  return frames
 print('WINDOW',json.dumps(dict(version=VERSION,source_counts=counts,transferred_duplicates=dedup,from_iran=stamp(START),end_iran=stamp(END),symbol='BTCUSDT',normalized_targets='1.5R and 2.5R from recorded initial stop',minimum_100_USD_stop='ONLY_IN_V6_EMA_STOP100_ARMS_AND_NEW_TK15',cloud='9/26/52 shifted26; conventional offset, exact TradingView parity not verified')),flush=True)
 evaluated=[]
 for idx,t in enumerate(rows):
  at=t['decision'];cl=cloudmap.get(at//900*900)
  if cl is None:raise RuntimeError('CLOUD_WARMUP_MISSING '+str(t['id']))
  f=filters(t['side'],frames_at(at));state=v6_at(at)
  t.update(cloud15=cl,v6_state=state,recomputed_filters=f)
  evaluated.append(t)
  if (idx+1)%100==0:print('EVALUATED',idx+1,flush=True)
 outputs={};summaries=[]
 for group in ('ALL','OLD_V4','DUAL'):
  own=evaluated if group=='ALL' else [t for t in evaluated if t['group']==group]
  for gate in ('BASE','ICHI15','V6_EMA_STOP100','V6_EMA_STOP100_ICHI15'):
   candidates=[t for t in own if (gate not in ('ICHI15','V6_EMA_STOP100_ICHI15') or t['cloud15']['direction']==t['side']) and (gate not in ('V6_EMA_STOP100','V6_EMA_STOP100_ICHI15') or (t['v6_state']['regime'] not in ('RANGE','WARMUP') and t['recomputed_filters']['ema_aligned'] and abs(t['entry']-t['sl'])+1e-9>=100))]
   for trailing in (False,True):
    for single in (False,True):
     pathout={}
     for path in ('OHLC','OLHC'):
      kept=run_strategy(candidates,one,times,END,trailing,path,single)
      key='|'.join((group,gate,'TRAIL_BE' if trailing else 'FIXED','SINGLE' if single else 'OVERLAP',path));outputs[key]=kept;result=summary_output(kept);summaries.append(dict(key=key,**result));pathout[path]=kept
      if path=='OHLC':print('RESULT',key,json.dumps(result),flush=True)
     diffs=path_differences(pathout)
     if diffs:print('PATH_DEPENDENT',group,gate,trailing,single,json.dumps(diffs),flush=True)
 generated=[e for e in ichi_entries(fifteen,cloudlist,START,END) if e['rule']=='TK_CROSS15'];new=[];reject=collections.Counter()
 for e in generated:
  at=e['at'];frames=frames_at(at);f=filters(e['side'],frames);state=v6_at(at);entry=frames['1m'][-1][4];sl,tp1,tp2=levels(e['side'],entry,frames['5m'],.1)
  fail=[]
  if state['regime'] in ('RANGE','WARMUP'):fail.append('V6_RANGE')
  if not f['ema_aligned']:fail.append('EMA')
  if abs(entry-sl)+1e-9<100:fail.append('STOP_LT100')
  if f['score']<4:fail.append('SCORE_LT4')
  if fail:reject.update(fail);continue
  nearby=[r['id'] for r in rows if r['side']==e['side'] and abs(r['decision']-at)<=900]
  new.append(dict(id='TK15:'+str(at),group='NEW_TK15',opened=at,decision=at,side=e['side'],entry=entry,sl=sl,tp1=tp1,tp2=tp2,nearby_recorded_ids_15min=nearby,filters=f))
 print('TK15_GENERATION',json.dumps(dict(before_filters=len(generated),after_filters=len(new),overlapping_rejections=dict(reject))),flush=True)
 for trailing in (False,True):
  for single in (False,True):
   pathout={}
   for path in ('OHLC','OLHC'):
    kept=run_strategy(new,one,times,END,trailing,path,single);key='|'.join(('NEW_TK15','V6_EMA_SCORE4','TRAIL_BE' if trailing else 'FIXED','SINGLE' if single else 'OVERLAP',path));outputs[key]=kept;result=summary_output(kept);summaries.append(dict(key=key,**result));pathout[path]=kept
    print('RESULT',key,json.dumps(result),flush=True)
   diff=path_differences(pathout)
   if diff:print('PATH_DEPENDENT_NEW',trailing,single,json.dumps(diff),flush=True)
 for t in new:print('NEW_TK15',json.dumps(dict(t,time_iran=stamp(t['opened']))),flush=True)
 folder=pathlib.Path('/opt/btc_all_ichimoku_research');folder.mkdir(exist_ok=True);out=folder/('all_btc_'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S_%f'));out.mkdir()
 data=dict(version=VERSION,start=START,end=END,counts=counts,dedup=dedup,entries=evaluated,new_tk15=new,outputs=outputs,summaries=summaries)
 (out/'results.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
 (out/'summary.json').write_text(json.dumps(dict(version=VERSION,start=stamp(START),end=stamp(END),counts=counts,dedup=dedup,summaries=summaries),indent=2),encoding='utf-8')
 print('REPORT_DIRECTORY',out)
 print('NORMALIZED_1.5R_2.5R_FULL_SIZE_EXITS; NOT_ORIGINAL_LEGACY_MANAGEMENT_OR_REALIZED_PNL')
 print('NEW_TK15_REPLACES_TMCO_H1_EARLY_GATES; KEEPS_V6_EMA_SCORE4_STOP100; B3_NOT_ADDED')
 print('ALL_RECORDED_FILLED_PAPER_ENTRIES; TELEGRAM_DELIVERY_NOT_VERIFIED; OLD_V4_AND_DUAL_SEPARATE')
 print('SCENARIOS_OHLC_OLHC_NOT_EXHAUSTIVE_TICK_PROOF; ENTRY_MINUTE_EXCLUDED; LIVE_QUOTE_CHASE_REENTRY_NOT_VERIFIED')
 print('1000_USDT_NOTIONAL_EACH; 0.055_PERCENT_FEE_EACH_SIDE; OPEN_EXCLUDED; NO_FUNDING_SLIPPAGE')
 print('READ_ONLY_SOURCES; RESEARCH_OUTPUT_FILES_ONLY; NO_BOT_SERVICE_SHADOW_OR_TELEGRAM_CHANGES')

def summary_output(kept):
 s=summarize(kept);s.pop('ids',None);s.pop('open_ids',None);return s

def path_differences(paths):
 byid={}
 for path,xs in paths.items():
  for t in xs:byid.setdefault(t['id'],{})[path]=t['outcome']
 return [k for k,v in byid.items() if len(v)!=2 or v['OHLC']!=v['OLHC']]
if __name__=='__main__':main()
