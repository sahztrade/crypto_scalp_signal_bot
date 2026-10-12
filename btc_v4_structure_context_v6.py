#!/usr/bin/env python3
"""Read-only V6 structural context audit; Bybit BTCUSDT, closed 5m candles."""
import csv,json,sqlite3,urllib.parse,urllib.request,time
from pathlib import Path
DB='/opt/tmco_webhook_v4/data/v4_paper_live.db'
OUT=Path('/tmp/btc_v4_structure_context_v6')
def candles(epoch):
    end=int(epoch//300)*300; start=end-320*300; data={}; cursor=end*1000-1
    for _ in range(4):
        q=urllib.parse.urlencode(dict(category='linear',symbol='BTCUSDT',interval='5',end=cursor,limit=200))
        req=urllib.request.Request('https://api.bybit.com/v5/market/kline?'+q,headers={'User-Agent':'structure-v6-audit'})
        with urllib.request.urlopen(req,timeout=30) as r: obj=json.load(r)
        if obj['retCode']!=0: raise RuntimeError(str(obj))
        batch=obj['result']['list']
        if not batch: break
        for a in batch:
            t=int(a[0])//1000
            if start<=t and t+300<=epoch:data[t]=(t,*map(float,a[1:5]))
        oldest=min(int(a[0]) for a in batch)
        if oldest<=start*1000:break
        cursor=oldest-1;time.sleep(.1)
    b=sorted(data.values())[-288:]
    if len(b)!=288 or any(b[i][0]-b[i-1][0]!=300 for i in range(1,288)):raise RuntimeError('Incomplete 288 closed bars')
    return b
def atr(b,k):
    if k<14:return None
    return sum(max(b[i][2]-b[i][3],abs(b[i][2]-b[i-1][4]),abs(b[i][3]-b[i-1][4])) for i in range(k-13,k+1))/14
def pivots(b):
    p=[]
    for i in range(16,len(b)-2):
        a=atr(b,i+2)
        h=b[i][2];l=b[i][3];region=b[i-2:i+3]
        if h>max(b[j][2] for j in (i-2,i-1)) and h>=max(b[j][2] for j in (i+1,i+2)) and h-min(x[3] for x in region)>=.7*a:p.append((i+2,i,'H',h))
        if l<min(b[j][3] for j in (i-2,i-1)) and l<=min(b[j][3] for j in (i+1,i+2)) and max(x[2] for x in region)-l>=.7*a:p.append((i+2,i,'L',l))
    return sorted(p)
def merge(p,a):
    w=[]
    for x in p:
        if not w:w.append(x);continue
        y=w[-1]
        if x[2]==y[2]:
            if (x[2]=='H' and x[3]>y[3]) or (x[2]=='L' and x[3]<y[3]):w[-1]=x
        elif abs(x[3]-y[3])>=1.8*a:w.append(x)
    return w
def trend(w,a):
    h=[x[3] for x in w if x[2]=='H'][-3:];l=[x[3] for x in w if x[2]=='L'][-3:]
    if len(h)<3 or len(l)<3:return 'INSUFFICIENT'
    if all(h[i+1]>h[i]+.35*a and l[i+1]>l[i]+.35*a for i in (0,1)):return 'UP'
    if all(h[i+1]<h[i]-.35*a and l[i+1]<l[i]-.35*a for i in (0,1)):return 'DOWN'
    if max(h)-min(h)<.7*a and max(l)-min(l)<.7*a:return 'RANGE'
    return 'TRANSITION'
def breaks(b,p,start):
    up=dn=0
    for k in range(max(16,start),len(b)):
        for kind in ('H','L'):
            q=[x for x in p if x[2]==kind and x[0]<k]
            if not q:continue
            v=q[-1][3]
            if kind=='H' and b[k-1][4]<=v<b[k][4]:up+=1
            if kind=='L' and b[k-1][4]>=v>b[k][4]:dn+=1
    return up,dn
def analyze(b,side,entry):
    a=atr(b,287);p=pivots(b);w=merge(p,a)
    candidates=[x for x in w if (x[2]=='H' and side=='LONG' and x[3]>entry) or (x[2]=='L' and side=='SHORT' and x[3]<entry)]
    near=min(candidates,key=lambda x:abs(x[3]-entry)) if candidates else None
    dist=abs(near[3]-entry)/a if near else None
    lastpair=None
    for i in range(len(w)-1,0,-1):
        x,y=w[i-1],w[i]
        if (side=='LONG' and x[2]=='H' and y[2]=='L') or (side=='SHORT' and x[2]=='L' and y[2]=='H'):
            lastpair=x,y;break
    recovery=None
    if lastpair:
        x,y=lastpair
        recovery=100*((entry-y[3])/(x[3]-y[3]) if side=='LONG' else (y[3]-entry)/(y[3]-x[3]))
    mu,md=breaks(b,w,252);iu,idn=breaks(b,p,252)
    phase='UNMAPPED_BREAKOUT' if dist is None else 'NEAR_LEVEL' if dist<.75 else 'APPROACH_LEVEL' if dist<1.5 else 'OPEN_SPACE'
    return dict(TREND=trend(w,a),ATR=round(a,2),WAVES=len(w),NEAREST_LEVEL=near[3] if near else '',LEVEL_DISTANCE_ATR=round(dist,2) if dist is not None else '',RECOVERY_PCT=round(recovery,1) if recovery is not None else '',MAJOR_BREAK_UP_3H=mu,MAJOR_BREAK_DOWN_3H=md,INTERNAL_BREAK_UP_3H=iu,INTERNAL_BREAK_DOWN_3H=idn,PHASE=phase)
def main():
    con=sqlite3.connect('file:'+DB+'?mode=ro',uri=True)
    try: rows=con.execute('SELECT signal_id,epoch,direction,entry FROM paper_trades_v4 WHERE signal_id BETWEEN 159 AND 179 ORDER BY signal_id').fetchall()
    finally:con.close()
    if not rows:raise RuntimeError('No matching paper trades')
    OUT.mkdir(exist_ok=True,parents=True);records=[]
    print('V6 READ ONLY | Bybit BTCUSDT | 288 completed 5m candles | no future data')
    print('ID SIDE TREND PHASE LEVEL_ATR RECOVERY% MAJOR_UP/DOWN INTERNAL_UP/DOWN')
    for sid,epoch,side,entry in rows:
        try:
            r=analyze(candles(float(epoch)),str(side).upper(),float(entry))
            records.append(dict(ID=sid,SIDE=side,ENTRY=entry,**r))
            print(sid,side,r['TREND'],r['PHASE'],r['LEVEL_DISTANCE_ATR'],r['RECOVERY_PCT'],str(r['MAJOR_BREAK_UP_3H'])+'/'+str(r['MAJOR_BREAK_DOWN_3H']),str(r['INTERNAL_BREAK_UP_3H'])+'/'+str(r['INTERNAL_BREAK_DOWN_3H']))
        except Exception as ex:
            print('ERROR',sid,ex);records.append(dict(ID=sid,SIDE=side,ERROR=str(ex)))
    path=OUT/'structure_context_report.csv'
    fields=list(dict.fromkeys(k for r in records for k in r))
    with path.open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(records)
    print('CSV:',path);print('FINISHED | no database writes, no orders, no service changes')
if __name__=='__main__':main()
