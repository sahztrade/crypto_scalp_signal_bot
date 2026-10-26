#!/usr/bin/env python3
"""Independent historical BTC research; public APIs and read-only signal DB.

Streams daily trade archives into minute aggregates; never submits orders,
modifies a bot, or writes to its database. No third-party dependencies.
"""
import argparse,csv,datetime as dt,gzip,io,json,math,os,sqlite3,time
from pathlib import Path
import urllib.request,urllib.parse,urllib.error

SCHEMA=1
UTC=dt.timezone.utc
START=int(dt.datetime(2026,10,21,tzinfo=UTC).timestamp())
END=int(dt.datetime(2026,10,26,23,21,tzinfo=dt.timezone(dt.timedelta(hours=3,minutes=30))).timestamp())
HEADERS={'User-Agent':'BTC-Orderflow-OI-Research/1.0'}

def atomic_json(path,data):
    temporary=path.with_suffix(path.suffix+'.tmp')
    temporary.write_text(json.dumps(data,allow_nan=False,separators=(',',':')),encoding='utf-8')
    os.replace(temporary,path)

def request(url):
    for attempt in range(3):
        try:return urllib.request.urlopen(urllib.request.Request(url,headers=HEADERS),timeout=45)
        except urllib.error.HTTPError as exc:
            if exc.code not in (429,500,502,503,504) or attempt==2:raise
        except (urllib.error.URLError,TimeoutError):
            if attempt==2:raise
        time.sleep(attempt+1)

def add_trade(minutes,at,price,size,buy):
    if not all(math.isfinite(v) for v in (at,price,size)) or price<=0 or size<=0:
        raise ValueError('INVALID_TRADE')
    minute=int(at)//60*60
    b=minutes.setdefault(minute,dict(at=minute,buy_btc=0.,sell_btc=0.,buy_usdt=0.,sell_usdt=0.,
                                   trades=0,first_at=at,last_at=at,open=price,close=price))
    b['buy_btc' if buy else 'sell_btc']+=size
    b['buy_usdt' if buy else 'sell_usdt']+=price*size
    b['trades']+=1
    if at<b['first_at']:b['first_at']=at;b['open']=price
    if at>=b['last_at']:b['last_at']=at;b['close']=price

def trade_day(folder,day):
    date=dt.datetime.fromtimestamp(day,UTC).strftime('%Y-%m-%d')
    target=folder/('trades_'+date+'.json')
    if target.exists():
        saved=json.loads(target.read_text(encoding='utf-8'))
        if saved.get('schema')!=SCHEMA or saved.get('date')!=date or not saved.get('complete'):
            raise ValueError('INCOMPATIBLE_TRADE_CACHE')
        print('TRADE_CACHE_OK',date,flush=True)
        return saved
    url='https://public.bybit.com/trading/BTCUSDT/BTCUSDT'+date+'.csv.gz'
    print('TRADE_STREAM_START',date,flush=True)
    minutes={};count=0
    try:
        with request(url) as response:
            length=response.headers.get('Content-Length')
            with gzip.GzipFile(fileobj=response) as compressed:
                with io.TextIOWrapper(compressed,encoding='utf-8') as stream:
                    reader=csv.DictReader(stream)
                    if not {'timestamp','symbol','side','size','price'}<=set(reader.fieldnames or []):
                        raise ValueError('UNEXPECTED_ARCHIVE_COLUMNS')
                    for row in reader:
                        at=float(row['timestamp'])
                        if row['symbol']!='BTCUSDT' or not day<=at<day+86400:
                            raise ValueError('ARCHIVE_DATE_OR_SYMBOL_MISMATCH')
                        if row['side'] not in ('Buy','Sell'):raise ValueError('INVALID_SIDE')
                        add_trade(minutes,at,float(row['price']),float(row['size']),row['side']=='Buy')
                        count+=1
                        if count%500000==0:print('TRADE_PROGRESS',date,count,flush=True)
    except urllib.error.HTTPError as exc:
        if exc.code==404:
            print('TRADE_ARCHIVE_UNAVAILABLE',date,flush=True)
            return dict(date=date,complete=False,minutes=[],http_error=404)
        raise
    saved=dict(schema=SCHEMA,date=date,complete=True,source=url,compressed_bytes=length,
               trade_records=count,minutes=[minutes[t] for t in sorted(minutes)])
    atomic_json(target,saved)
    print('TRADE_AGGREGATION_OK',date,count,len(minutes),flush=True)
    return saved

def open_interest(folder):
    target=folder/'oi_5m.json'
    if target.exists():
        saved=json.loads(target.read_text(encoding='utf-8'))
        if saved.get('schema')==SCHEMA and saved.get('start')==START and saved.get('end')==END:
            print('OI_CACHE_OK',len(saved['rows']),flush=True)
            return saved['rows']
        raise ValueError('INCOMPATIBLE_OI_CACHE')
    rows={};cursor=None;seen=set()
    for page in range(100):
        params=dict(category='linear',symbol='BTCUSDT',intervalTime='5min',
                    startTime=START*1000,endTime=END*1000,limit=200)
        if cursor:params['cursor']=cursor
        url='https://api.bybit.com/v5/market/open-interest?'+urllib.parse.urlencode(params)
        with request(url) as response:data=json.load(response)
        if data.get('retCode')!=0:raise RuntimeError('OI_API_ERROR:'+str(data.get('retMsg')))
        for r in data.get('result',{}).get('list',[]):
            at=int(r['timestamp'])//1000;value=float(r['openInterest'])
            if not START<=at<=END or at%300 or not math.isfinite(value) or value<=0:
                raise ValueError('INVALID_OI_POINT')
            if at in rows and rows[at]!=value:raise ValueError('CONFLICTING_OI_POINT')
            rows[at]=value
        print('OI_PROGRESS',len(rows),flush=True)
        cursor=data.get('result',{}).get('nextPageCursor')
        if not cursor:break
        if cursor in seen:raise ValueError('OI_CURSOR_NOT_ADVANCING')
        seen.add(cursor)
    else:raise RuntimeError('OI_PAGE_LIMIT')
    result=[dict(at=t,value=rows[t]) for t in sorted(rows)]
    atomic_json(target,dict(schema=SCHEMA,start=START,end=END,rows=result))
    return result

def window(minutes,end,length):
    buckets=[minutes.get(t) for t in range(end-length,end,60)]
    if not buckets or any(b is None for b in buckets):return None
    buy=sum(b['buy_usdt'] for b in buckets);sell=sum(b['sell_usdt'] for b in buckets)
    buy_btc=sum(b['buy_btc'] for b in buckets);sell_btc=sum(b['sell_btc'] for b in buckets)
    return dict(buy_usdt=buy,sell_usdt=sell,delta_usdt=buy-sell,
                delta_btc=buy_btc-sell_btc,imbalance=(buy-sell)/(buy+sell) if buy+sell else None,
                price_change_pct=(buckets[-1]['close']/buckets[0]['open']-1)*100,
                first_price=buckets[0]['open'],last_price=buckets[-1]['close'],
                trades=sum(b['trades'] for b in buckets))

def feature(r,minutes,oi):
    end=int(r['decision_close'])
    if end%60:raise ValueError('UNALIGNED_DECISION')
    sign=1 if r['direction']=='LONG' else -1
    flow={str(n)+'m':window(minutes,end,n*60) for n in (1,3,5,15)}
    five=flow['5m']
    # Price failing to follow aggressive flow is a candidate warning, not
    # proof of passive absorption or an identified institutional order block.
    absorption=(sign*five['imbalance']>=.10 and sign*five['price_change_pct']<=0) if five and five['imbalance'] is not None else None
    day=end//86400*86400
    daily=[minutes.get(t) for t in range(day,end,60)]
    cvd=sum(b['buy_btc']-b['sell_btc'] for b in daily) if daily and all(b is not None for b in daily) else None
    # One full 5m lag: source timestamps do not establish exact publication
    # time. This avoids pretending a just-stamped historical point was live.
    point=(end-300)//300*300
    value=oi.get(point)
    change={}
    for n in (5,15):
        before=oi.get(point-n*60)
        change[str(n)+'m']=(value/before-1)*100 if value is not None and before is not None else None
    return dict(signal_id=r['signal_id'],direction=r['direction'],decision_close=end,
                flow=flow,cvd_btc_utc_day=cvd,candidate_absorption_warning=absorption,
                candidate_flow_aligned=(sign*five['imbalance']>=.10) if five and five['imbalance'] is not None else None,
                oi_timestamp=point if value is not None else None,oi_btc=value,
                oi_change_pct=change,oi_lag_seconds=end-point if value is not None else None)

def self_test():
    minutes={}
    for t in range(0,1200,60):
        add_trade(minutes,t+1,100,2,True)
        add_trade(minutes,t+2,99,1,False)
    r=dict(signal_id=1,direction='LONG',decision_close=1200)
    oi={900:110,600:100,0:100}
    a=feature(r,minutes,oi)
    assert a['candidate_absorption_warning'] is True
    assert a['flow']['5m']['delta_btc']==5
    assert abs(a['oi_change_pct']['5m']-10)<1e-10
    assert a['oi_timestamp']==900 and a['oi_change_pct']['15m'] is not None
    add_trade(minutes,1201,10000,100,True)
    oi[1200]=1000000
    assert feature(r,minutes,oi)==a
    assert window({},1200,300) is None
    assert feature(r,{},oi)['candidate_absorption_warning'] is None
    b={};add_trade(b,10,100,1,True);add_trade(b,5,99,1,False)
    assert b[0]['open']==99 and b[0]['close']==100
    print('SELF_TEST_OK: trade aggregation, closed windows, missing data, OI lag, no future leakage')

def main():
    p=argparse.ArgumentParser(description=__doc__)
    modes=p.add_mutually_exclusive_group(required=True)
    modes.add_argument('--self-test',action='store_true');modes.add_argument('--run',action='store_true')
    p.add_argument('--data-dir',default='/opt/btc_v13_research/orderflow_oi_research')
    args=p.parse_args()
    if args.self_test:self_test();return
    folder=Path(args.data_dir);folder.mkdir(parents=True,exist_ok=True)
    import fcntl
    with (folder/'research.lock').open('a') as lock:
        fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        db=sqlite3.connect('file:/opt/tmco_webhook_v4/data/v4_paper_live.db?mode=ro',uri=True,timeout=15)
        db.row_factory=sqlite3.Row
        trades=[dict(r) for r in db.execute('''SELECT t.signal_id,t.direction,c.decision_close
            FROM paper_trades_v4 t JOIN paper_confirmations c ON c.epoch=t.epoch
            WHERE t.signal_id BETWEEN 558 AND 747 ORDER BY t.signal_id''')]
        db.close()
        if len(trades)!=190:raise ValueError('EXPECTED_190_SIGNALS')
        if any(not START<=r['decision_close']<=END for r in trades):raise ValueError('UNEXPECTED_SIGNAL_TIME')
        oi_rows=open_interest(folder);oi={r['at']:r['value'] for r in oi_rows}
        minutes={};availability=[]
        for day in range(START,END//86400*86400+1,86400):
            saved=trade_day(folder,day)
            availability.append({k:v for k,v in saved.items() if k!='minutes'})
            for b in saved['minutes']:minutes[b['at']]=b
        features=[feature(r,minutes,oi) for r in trades]
        result=dict(study='BTC_ORDERFLOW_OI_190',schema=SCHEMA,
                    definitions=dict(flow='Executed trade size and price; aggressor side as published in Bybit archive',
                                     cvd='Cumulative Buy BTC minus Sell BTC since UTC midnight; minute-complete windows only',
                                     absorption='Proxy warning: aligned aggressive notional imbalance >=0.10 but signed 5m price change <=0',
                                     oi='Historical openInterest; 5m minimum lag; no claim of exact historical publication timing',
                                     missing='Null means unavailable, never zero or an automatic rejection'),
                    archives=availability,oi_points=len(oi_rows),features=features)
        output=folder/'BTC_Orderflow_OI_Features_190.json'
        atomic_json(output,result)
        print('RESULT_SAVED',output,flush=True)
        print(json.dumps(result,ensure_ascii=False,indent=2),flush=True)

if __name__=='__main__':main()
