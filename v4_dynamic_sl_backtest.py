import sqlite3, math, time, requests
from datetime import datetime,timezone,timedelta
DB='data/v4_paper_live.db'; URL='https://api.bybit.com/v5/market/kline'; S=requests.Session()
def fetch(tf,start,end):
 step=int(tf)*60; start=start//step*step; end=end//step*step; out={}
 for a in range(start,end,step*1000):
  b=min(a+step*1000,end)
  for attempt in range(3):
   try:
    r=S.get(URL,params={'category':'linear','symbol':'BTCUSDT','interval':tf,'start':a*1000,'end':(b-step)*1000,'limit':1000},timeout=30);r.raise_for_status();j=r.json();assert j['retCode']==0,j;break
   except Exception:
    if attempt==2:raise
    time.sleep(2)
  for x in j['result']['list']:
   t=int(x[0])//1000
   if a<=t<b:out[t]=(float(x[2]),float(x[3]))
  if sum(a<=t<b for t in out)!=(b-a)//step:raise RuntimeError(f'MISSING {tf}m {a}..{b}')
 return out
with sqlite3.connect('file:'+DB+'?mode=ro',uri=True) as db:
 rows=db.execute('''SELECT t.signal_id,t.direction,t.entry,t.sl,t.tp1,t.tp2,t.tp3,t.monitor_start_ts,c.decision_close FROM paper_trades_v4 t LEFT JOIN paper_confirmations c ON c.epoch=t.epoch WHERE t.management_version='structural_3tp_v1' ORDER BY t.signal_id''').fetchall()
assert rows and all(r[7] is not None and r[8] is not None for r in rows)
end=int(time.time())//60*60
five=fetch('5',min(int(r[8]) for r in rows)//300*300-101*300,end)
one=fetch('1',min(math.ceil(r[7]/60)*60 for r in rows),end)
print('READ ONLY | SIGNALS:',len(rows),'| 1m candles:',len(one),'| 5m candles:',len(five))
def sim(r,dynamic,pivot):
 sid,side,entry,sl,tp1,tp2,tp3,monitor,decision=r
 entry,sl,tp1,tp2,tp3=map(float,(entry,sl,tp1,tp2,tp3));risk=abs(entry-sl)
 if not risk:return ('BAD_RISK',None,None)
 stop=sl;hit1=hit2=False;newsl=None
 for t in range(math.ceil(monitor/60)*60,end,60):
  high,low=one[t]; islong=side=='LONG'
  stophit=low<=stop if islong else high>=stop
  h1=high>=tp1 if islong else low<=tp1
  h2=high>=tp2 if islong else low<=tp2
  h3=high>=tp3 if islong else low<=tp3
  if stophit:
   if h3 or (h2 and not hit2) or (h1 and not hit1):return ('UNKNOWN',None,newsl)
   return ('SL',round(((stop-entry) if islong else (entry-stop))/risk,4),newsl)
  if h3:return ('TP3',round(((tp3-entry) if islong else (entry-tp3))/risk,4),newsl)
  if h2 and not hit2:
   hit1=hit2=True;stop=max(stop,tp1) if islong else min(stop,tp1)
  elif h1 and not hit1:
   hit1=True;candidate=entry
   if dynamic and pivot is not None:
    # Exclude original stop candle and any 5m candle not fully closed BEFORE TP1 1m candle.
    last_full=t//300*300
    levels=[five[k][1 if islong else 0] for k in range(pivot+300,last_full,300)]
    if levels:
     candidate=min(levels) if islong else max(levels);newsl=candidate
    else:candidate=sl
   stop=max(stop,candidate) if islong else min(stop,candidate)
 return ('ACTIVE',None,newsl)
results=[]
print('ID | SIDE | ORIGINAL SL CANDLE (IRAN) | NEW SL AFTER TP1 | CURRENT | NEW RULE')
for r in rows:
 sid,side,entry,sl,tp1,tp2,tp3,monitor,decision=r
 dec=int(decision)//300*300
 matches=[k for k in range(dec-100*300,dec,300) if k in five and abs(five[k][1 if side=='LONG' else 0]-sl)<.011]
 pivot=max(matches) if matches else None
 current=sim(r,False,pivot);dynamic=sim(r,True,pivot)
 results.append((sid,current,dynamic,pivot))
 clock=datetime.fromtimestamp(pivot,timezone(timedelta(hours=3,minutes=30))).strftime('%m-%d %H:%M') if pivot else 'NOT FOUND'
 print(f'{sid} | {side} | {clock} | {dynamic[2]} | {current[0]} {current[1]}R | {dynamic[0]} {dynamic[1]}R')
for i,name in [(1,'CURRENT'),(2,'NEW RULE')]:
 vals=[x[i][1] for x in results if x[i][1] is not None]
 print(name,'RESOLVED:',len(vals),'TOTAL RESOLVED R:',round(sum(vals),3),'ACTIVE:',sum(x[i][0]=='ACTIVE' for x in results),'UNKNOWN:',sum(x[i][0]=='UNKNOWN' for x in results))
print('DIFFERENT OUTCOMES:',[sid for sid,a,b,_ in results if a[:2]!=b[:2]])
print('WARNING: Open/unknown trades prevent a fair final profit comparison. Fees excluded.')
print('NO DATABASE WRITES; NO BOT CHANGES')
