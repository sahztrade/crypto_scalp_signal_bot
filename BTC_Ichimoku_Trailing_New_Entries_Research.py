#!/usr/bin/env python3
"""Read-only Ichimoku15 trailing replay and additional independent entry hypotheses."""
import argparse,bisect,collections,datetime,hashlib,importlib.util,json,math,pathlib,sqlite3,time,urllib.request,urllib.parse
from decimal import Decimal,ROUND_HALF_EVEN,ROUND_FLOOR,ROUND_CEILING
TZ=datetime.timezone(datetime.timedelta(hours=3,minutes=30))
COMPARE_SHA='8c2cf80b555782f728a8c7ef6438882aa767cce57d345e31efbb82f79499b268'
SHADOW_SHA='9aa19e404cc9b444db348d67d9a90230dd4f9d8821951eacba546d5ce5fd600c'
def load(name,path,sha):
 p=pathlib.Path(path)
 if hashlib.sha256(p.read_bytes()).hexdigest()!=sha:raise RuntimeError('CODE_HASH '+str(p))
 s=importlib.util.spec_from_file_location(name,p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
def stamp(t):return datetime.datetime.fromtimestamp(t,TZ).isoformat() if t is not None else None
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

def self_test():
 t=dict(id=1,side='LONG',opened=0,entry=100,sl=98,tp1=103,tp2=105)
 b=[[60,100,104,99,101,1]];a=replay(t,b,[60],120,True,'OHLC');z=replay(t,b,[60],120,True,'OLHC')
 assert a['status']=='BE' and z['status']=='OPEN'
 assert replay(t,b,[60],120,False,'OHLC')['status']=='OPEN'
 t2=dict(t,side='SHORT',entry=100,sl=102,tp1=97,tp2=95)
 b2=[[60,100,101,96,99,1]]
 assert replay(t2,b2,[60],120,True,'OLHC')['status']=='BE'
 assert replay(t2,b2,[60],120,True,'OHLC')['status']=='OPEN'
 assert replay(t,[[0,100,110,90,100,1]],[0],60,True,'OHLC')['status']=='OPEN'
 assert replay(t,[[60,97,99,96,98,1]],[60],120,False,'OHLC')['exit']==97
 assert levels('LONG',100,[[i*300,100,101,99,100,1] for i in range(100)],.1)[0]<100
 sample=[[i*900,100,101,99,100,1] for i in range(100)]
 fake=[dict(at=b[0]+900,direction='SHORT' if i<80 else 'LONG') for i,b in enumerate(sample)]
 full=ichi_entries(sample,fake,0,100*900)
 for n in (78,81,90):assert ichi_entries(sample[:n],fake[:n],0,n*900)==[e for e in full if e['at']<=n*900]
 assert any(e['rule']=='CLOUD_EXIT15' for e in full)
 print('ADVANCED_TEST_OK: TP1-to-BE both sides, intraminute ambiguity, entry-minute exclusion, gap fills, fixed stop')

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--self-test',action='store_true');a=ap.parse_args()
 if a.self_test:self_test();return
 comp=load('ichi_compare','/opt/btc_v13_research/BTC_V6_Ichimoku_Comparison.py',COMPARE_SHA)
 model=load('v6_frozen','/opt/btc_v13_research/BTC_V6_Forward_Shadow.py',SHADOW_SHA)
 with model.source('/opt/btc_dual_h1_research/dual.sqlite3') as db:
  rows=[dict(r) for r in db.execute("SELECT * FROM trades WHERE symbol='BTCUSDT' AND legacy=0 AND id>=2 ORDER BY opened,id")]
 if not rows:raise RuntimeError('NO_TRADES')
 END=int(time.time())//60*60;START=min(int(t['decision']) for t in rows)
 history_start=START//300*300-1000*300
 one=history1(history_start,END);times=[b[0] for b in one];five=agg(one,300);three=agg(one,180);fifteen=agg(one,900)
 five_times=[b[0] for b in five];three_ends=[b[0]+180 for b in three];five_ends=[b[0]+300 for b in five]
 c15=comp.clouds(fifteen,900);cmap={c['at']:c for c in c15}
 def v6_at(at):
  end=at//300*300;j=bisect.bisect_left(five_times,end);bs=five[j-1000:j]
  if len(bs)!=1000 or bs[-1][0]+300!=end:raise RuntimeError('V6_HISTORY_MISSING')
  q=model.Context()
  for b in bs:state=q.on_bar(b,b[0]+300)
  return state
 eligible=[]
 for t in rows:
  state=v6_at(int(t['decision']));cloud=cmap.get(int(t['decision'])//900*900);f=json.loads(t['details'] or '{}').get('filters',{})
  if not cloud:raise RuntimeError('CLOUD_MISSING')
  if state['regime'] not in ('RANGE','WARMUP') and abs(t['entry']-t['sl'])+1e-9>=100 and f.get('ema_aligned') is True and cloud['direction']==t['side']:eligible.append(t)
 generated=ichi_entries(fifteen,c15,START,END);new={'CLOUD_EXIT15':[],'TK_CROSS15':[]};rejections=collections.Counter()
 for e in generated:
  at=e['at'];idx1=bisect.bisect_left(times,at);idx3=bisect.bisect_right(three_ends,at);idx5=bisect.bisect_right(five_ends,at)
  frames={'1m':one[max(0,idx1-240):idx1],'3m':three[max(0,idx3-80):idx3],'5m':five[max(0,idx5-100):idx5]}
  if [len(frames[k]) for k in ('1m','3m','5m')]!=[240,80,100]:raise RuntimeError('FILTER_HISTORY_MISSING')
  f=filters(e['side'],frames);entry=frames['1m'][-1][4];sl,tp1,tp2=levels(e['side'],entry,frames['5m'],.1);state=v6_at(at)
  failures=[]
  if state['regime'] in ('RANGE','WARMUP'):failures.append('RANGE')
  if abs(entry-sl)+1e-9<100:failures.append('STOP_LT100')
  if f['ema_aligned'] is not True:failures.append('EMA')
  if f['score']<4:failures.append('SCORE_LT4')
  if failures:
   rejections.update(e['rule']+':'+x for x in failures);continue
  nearby=[r['id'] for r in rows if r['side']==e['side'] and abs(r['decision']-at)<=900]
  new[e['rule']].append(dict(id=e['rule']+':'+str(at),opened=at,decision=at,side=e['side'],entry=entry,sl=sl,tp1=tp1,tp2=tp2,nearby_recorded_ids_15min=nearby,filters=f))
 print('WINDOW',json.dumps(dict(from_iran=stamp(START),end_iran=stamp(END),last_recorded_id=rows[-1]['id'],eligible_ichi15_ids=[t['id'] for t in eligible],generated_before_filters=dict(collections.Counter(e['rule'] for e in generated)),rejections=dict(rejections))))
 all_outputs={}
 for name,candidates in [('RECORDED_ICHI15',eligible)]+list(new.items()):
  for trailing in (False,True):
   for single in (False,True):
    outs={}
    for path in ('OHLC','OLHC'):
     kept=run_strategy(candidates,one,times,END,trailing,path,single)
     key='|'.join((name,'TRAIL_BE' if trailing else 'FIXED','SINGLE' if single else 'OVERLAP',path));all_outputs[key]=kept;outs[path]=kept
     print('RESULT',key,json.dumps(summarize(kept)))
    perid={}
    for path,xs in outs.items():
     for t in xs:perid.setdefault(str(t['id']),{})[path]=t['outcome']
    differs=[i for i,x in perid.items() if len(x)!=2 or x.get('OHLC')!=x.get('OLHC')]
    print('PATH_DEPENDENT',name,'TRAIL_BE' if trailing else 'FIXED','SINGLE' if single else 'OVERLAP',json.dumps(differs))
    if name=='RECORDED_ICHI15' and trailing and single:
     for path,xs in outs.items():
      for t in xs:print('TRAIL_TRADE',json.dumps(dict(id=t['id'],path=path,time_iran=stamp(t['opened']),side=t['side'],entry=t['entry'],sl=t['sl'],tp1=t['tp1'],tp2=t['tp2'],outcome=t['outcome'])))
 for name,xs in new.items():
  for t in xs:print('NEW_CANDIDATE',name,json.dumps(dict(t,time_iran=stamp(t['opened']))))
 folder=pathlib.Path('/opt/btc_ichimoku_research');folder.mkdir(exist_ok=True)
 output=folder/('advanced_'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S_%f')+'.json')
 output.write_text(json.dumps(dict(end=END,recorded_ids=[t['id'] for t in rows],new_candidates=new,outputs=all_outputs),indent=2),encoding='utf-8');print('REPORT',output)
 print('TWO_OHLC_PATH_ASSUMPTIONS_ARE_SCENARIOS_NOT_EXHAUSTIVE_TICK_PROOF; PATH_DIFFERENCES_NOT_KNOWN_PROFIT')
 print('TP1_1.5R_MOVES_FULL_STOP_TO_ENTRY; FULL_EXIT_2.5R; NO_PARTIALS; ENTRY_MINUTE_EXCLUDED')
 print('NEW_RULES_REPLACE_TMCO_AND_H1_EARLY_GATES_WITH_ICHI15; KEEP_V6_EMA_SCORE4_STOP100; B3_OFF')
 print('NEW_ENTRIES_LAST_CLOSED_1M_CLOSE_NOT_ACTUAL_QUOTES; CHASE_REENTRY_NOT_VERIFIED; NEARBY_IDS_NOT_DUPLICATE_PROOF')
 print('1000_USDT_NOTIONAL; 0.055_PERCENT_FEE_PER_SIDE; OPEN_EXCLUDED; NO_FUNDING_SLIPPAGE')
 print('READ_ONLY_SOURCE; REPORT_FILES_ONLY; NO_BOT_SHADOW_TELEGRAM_OR_ORDER_CHANGES')
if __name__=='__main__':main()
