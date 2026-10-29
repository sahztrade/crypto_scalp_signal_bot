#!/usr/bin/env python3
"""Independent paper signals: original fixed SL, 2.5R, causal hourly trend.

No exchange credentials, order endpoints, leverage changes or actual orders.
Telegram: fresh entries and one per-symbol midnight report only.
"""
import argparse
import collections
import concurrent.futures
import datetime as dt
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN
import hashlib
import json
import math
from pathlib import Path
import signal
import sqlite3
import threading
import time
import urllib.parse
import urllib.request

VERSION = 'DUAL-H1-ORIGINAL-2.5R-V1'
API = 'https://api.bybit.com'
IRAN = dt.timezone(dt.timedelta(hours=3, minutes=30))
SECONDS = {'1m':60,'3m':180,'5m':300,'15m':900,'1h':3600}
MODES = ('M1','M2')

def stamp(t):
    return dt.datetime.fromtimestamp(t, IRAN).strftime('%Y/%m/%d %H:%M:%S')

def api(path, **params):
    url = API + path + '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={'User-Agent':VERSION})
    with urllib.request.urlopen(req, timeout=8) as response:
        data = json.load(response)
    if data.get('retCode') != 0:
        raise RuntimeError('BYBIT_RESPONSE_' + str(data.get('retCode')))
    if abs(float(data.get('time',0))/1000-time.time()) > 10:
        raise RuntimeError('CLOCK_DRIFT')
    return data['result']

def candles(symbol, tf, end, count):
    seconds = SECONDS[tf]
    finish = int(end)//seconds*seconds
    rows = {}
    cursor = finish*1000-1
    while len(rows) < count:
        result = api('/v5/market/kline', category='linear', symbol=symbol,
                     interval=str(seconds//60), end=cursor,
                     limit=min(1000,count-len(rows)+3))
        batch = result.get('list',[])
        if not batch:
            raise RuntimeError('EMPTY_KLINES')
        earliest = min(int(r[0]) for r in batch)
        for r in batch:
            ts = int(r[0])//1000
            if ts+seconds <= finish:
                rows[ts] = [ts]+[float(x) for x in r[1:6]]
        if earliest >= cursor:
            raise RuntimeError('KLINE_PAGINATION_STALLED')
        cursor = earliest-1
    bars = [rows[t] for t in sorted(rows)][-count:]
    if len(bars)!=count or bars[-1][0]+seconds!=finish:
        raise RuntimeError('INCOMPLETE_KLINES_'+tf)
    for i,b in enumerate(bars):
        if (len(b)!=6 or not all(math.isfinite(x) for x in b) or b[0]%seconds
            or b[3]<=0 or b[5]<0 or b[3]>min(b[1],b[4])
            or b[2]<max(b[1],b[4]) or b[3]>b[2]
            or (i and b[0]-bars[i-1][0]!=seconds)):
            raise RuntimeError('INVALID_OR_GAPPED_KLINES')
    return bars

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

def crossings(bars, tf):
    source = [sum(b[1:5])/4 for b in bars]
    line,wave = ema(rsi(source,2),2),ema(rsi(source,4),4)
    result = []
    for i in range(1,len(bars)):
        if None in (line[i-1],wave[i-1],line[i],wave[i]):
            continue
        before,after = wave[i-1]-line[i-1],wave[i]-line[i]
        side = 'LONG' if before>=0 and after<0 else 'SHORT' if before<=0 and after>0 else None
        if side:
            result.append(dict(at=bars[i][0]+SECONDS[tf],tf=tf,side=side))
    return result

class CrossGate:
    def __init__(self, mode, saved=None):
        self.mode = mode
        self.previous = None
        self.latest = {}
        self.last_close = 0
        self.ident = 0
        self.cutoff = 0
        if saved:
            self.__dict__.update(saved)

    def process(self, event, received):
        at,tf,side = event['at'],event['tf'],event['side']
        allowed = ('1m','3m','5m') if self.mode=='M1' else ('15m','1h')
        if tf not in allowed or side not in ('LONG','SHORT'):
            return None
        if not 0<=received-at<=15 or at<self.last_close or at<=self.latest.get(tf,-1):
            return None
        previous = self.previous
        self.ident += 1
        self.latest[tf] = at
        self.last_close = at
        self.previous = dict(event,id=self.ident)
        window = 600 if self.mode=='M1' else 3600
        if (previous and previous['side']==side and previous['id']>self.cutoff
            and 0<=at-previous['at']<=window
            and (self.mode=='M1' or previous['tf']!=tf)):
            self.cutoff = self.ident
            return dict(at=at,side=side,first=previous,second=self.previous)
        return None

class HourTrend:
    """Exact causal local-trend rule used in the research, on own hourly closes."""
    def __init__(self, saved=None):
        self.bars = []
        self.highs = []
        self.lows = []
        self.local = None
        self.stop = None
        self.invalid_count = 0
        self.seen = 0
        if saved:
            self.__dict__.update(saved)

    def step(self, b):
        if self.bars and b[0]<=self.bars[-1][0]:
            return
        if self.bars and b[0]!=self.bars[-1][0]+3600:
            raise ValueError('HOURLY_TREND_GAP')
        self.bars.append(b)
        self.bars = self.bars[-70:]
        self.seen += 1
        if self.seen<65:
            return
        bars = self.bars
        a = sum(max(x[2]-x[3],abs(x[2]-previous[4]),abs(x[3]-previous[4]))
                for previous,x in zip(bars[-15:-1],bars[-14:]))/14
        a = max(a,1e-12)
        at = b[0]+3600
        pivot = bars[-3]
        neighbors = bars[-5:-3]+bars[-2:]
        if all(pivot[2]>x[2] for x in neighbors):
            self.highs = (self.highs+[dict(price=pivot[2],open=pivot[0],known=at)])[-2:]
        if all(pivot[3]<x[3] for x in neighbors):
            self.lows = (self.lows+[dict(price=pivot[3],open=pivot[0],known=at)])[-2:]
        if self.local and self.stop is not None:
            if self.local=='LONG' and self.lows:
                self.stop = max(self.stop,self.lows[-1]['price']-.15*a)
            elif self.local=='SHORT' and self.highs:
                self.stop = min(self.stop,self.highs[-1]['price']+.15*a)
            broken = b[4]<self.stop if self.local=='LONG' else b[4]>self.stop
            self.invalid_count = self.invalid_count+1 if broken else 0
            if self.invalid_count>=2:
                self.local,self.stop,self.invalid_count = None,None,0
        if len(self.highs)==len(self.lows)==2:
            h,l = self.highs[-1],self.lows[-1]
            recent = bars[-2:]
            up = (h['price']>self.highs[-2]['price'] and l['price']>self.lows[-2]['price']
                  and l['open']>h['open']
                  and all(x[0]>=h['known'] and x[4]>h['price']+.10*a for x in recent))
            down = (h['price']<self.highs[-2]['price'] and l['price']<self.lows[-2]['price']
                    and h['open']>l['open']
                    and all(x[0]>=l['known'] and x[4]<l['price']-.10*a for x in recent))
            candidate = 'LONG' if up else 'SHORT' if down else None
            if candidate and candidate!=self.local:
                self.local = candidate
                self.stop = l['price']-.15*a if candidate=='LONG' else h['price']+.15*a
                self.invalid_count = 0

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

def observe(p,b):
    sign = 1 if p['side']=='LONG' else -1
    stopped = b[3]<=p['sl'] if sign==1 else b[2]>=p['sl']
    hit1 = b[2]>=p['tp1'] if sign==1 else b[3]<=p['tp1']
    hit2 = b[2]>=p['tp2'] if sign==1 else b[3]<=p['tp2']
    # Unordered stop/final-target touches must never be counted as known PnL.
    if stopped and hit2:
        return 'UNKNOWN',None,p['hit1']
    if stopped:
        return 'SL',1000*sign*(p['sl']-p['entry'])/p['entry'],p['hit1']
    if hit2:
        return 'TARGET',1000*sign*(p['tp2']-p['entry'])/p['entry'],1
    return 'OPEN',None,int(bool(p['hit1'] or hit1))

def credentials(path):
    values = {}
    for line in Path(path).read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if line.startswith('export '):
            line = line[7:]
        if not line or line.startswith('#') or '=' not in line:
            continue
        key,value = line.split('=',1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    if not values.get('BOT_TOKEN') or not values.get('CHAT_ID'):
        raise ValueError('TELEGRAM_CONFIGURATION_MISSING')
    return values['BOT_TOKEN'],values['CHAT_ID']

def entry_text(symbol, mode, sid, p):
    fmt = lambda x: format(x,'.10f').rstrip('0').rstrip('.')
    return '\n'.join([symbol+' | حالت '+('اول' if mode=='M1' else 'دوم'),
        'سیگنال پژوهشی شماره '+str(sid),'جهت: '+p['side'],
        'زمان ایران: '+stamp(p['opened']),'',
        'ورود مرجع: '+fmt(p['entry']),'استاپ ثابت: '+fmt(p['sl']),
        'هدف اول 1.5R: '+fmt(p['tp1']),'هدف نهایی 2.5R: '+fmt(p['tp2']),
        'امتیاز فیلتر: '+str(p['score'])+' از ۶','روند یک‌ساعته: '+p['side'],
        'تقاطع: '+('قواعد فعلی ۱، ۳ و ۵ دقیقه' if mode=='M1' else '۱۵ دقیقه و یک ساعت'),
        '','خروج کامل در هدف نهایی یا استاپ؛ بدون جابه‌جایی استاپ',
        'ارزیابی با کندل‌های کامل پس از دقیقه ورود؛ بدون هزینه‌ها',
        'پژوهشی؛ بدون سفارش واقعی'])

class Store:
    def __init__(self, folder):
        Path(folder).mkdir(parents=True,exist_ok=True)
        self.path = Path(folder)/'dual.sqlite3'
        self.db = sqlite3.connect(self.path,timeout=20)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS trades(
            id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT,mode TEXT,side TEXT,
            entry REAL,sl REAL,tp1 REAL,tp2 REAL,opened REAL,decision INTEGER,
            monitor_next INTEGER,status TEXT,hit1 INTEGER DEFAULT 0,
            closed_at REAL,pnl REAL,source_key TEXT UNIQUE,details TEXT,
            legacy INTEGER DEFAULT 0);
          CREATE TABLE IF NOT EXISTS decisions(
            key TEXT PRIMARY KEY,at REAL,mode TEXT,result TEXT);
          CREATE TABLE IF NOT EXISTS outbox(
            key TEXT PRIMARY KEY,kind TEXT,at REAL,text TEXT,status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,message_id INTEGER);
        ''')
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO meta VALUES(?,?)',('version',json.dumps(VERSION)))
        if self.get('version')!=VERSION:
            raise ValueError('DATABASE_VERSION_MISMATCH')

    def get(self,key,default=None):
        r = self.db.execute('SELECT value FROM meta WHERE key=?',(key,)).fetchone()
        return json.loads(r[0]) if r else default

    def put(self,key,value):
        self.db.execute('INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                        (key,json.dumps(value,allow_nan=False)))

    def queue(self,key,kind,at,text):
        self.db.execute('INSERT OR IGNORE INTO outbox(key,kind,at,text) VALUES(?,?,?,?)',
                        (key,kind,at,text))

    def add(self,symbol,mode,p,key,legacy=False):
        active = self.db.execute("SELECT id,entry,sl FROM trades WHERE symbol=? AND (mode=? OR mode='LEGACY') AND side=? AND status='OPEN'",
                                 (symbol,mode,p['side'])).fetchall()
        for r in active:
            same = abs(Decimal(str(r['sl']))-Decimal(str(p['sl'])))<Decimal(str(p['tick']))/2
            worse = p['entry']>r['entry'] if p['side']=='LONG' else p['entry']<r['entry']
            if same and worse and not legacy:
                return None,'WORSE_REENTRY_SAME_ACTIVE_STOP'
        cur = self.db.execute('''INSERT OR IGNORE INTO trades
          (symbol,mode,side,entry,sl,tp1,tp2,opened,decision,monitor_next,status,hit1,source_key,details,legacy)
          VALUES(?,?,?,?,?,?,?,?,?,?,'OPEN',?,?,?,?)''',
          (symbol,mode,p['side'],p['entry'],p['sl'],p['tp1'],p['tp2'],p['opened'],p['decision'],
           p.get('monitor_next',int(p['opened'])//60*60+60),p.get('hit1',0),key,
           json.dumps(p,allow_nan=False),int(legacy)))
        if cur.rowcount!=1:
            return None,'DUPLICATE'
        sid = cur.lastrowid
        if not legacy:
            self.queue('entry:'+str(sid),'ENTRY',p['opened'],entry_text(symbol,mode,sid,p))
        return sid,'ACCEPTED'

    def monitor(self,symbol,through):
        positions = self.db.execute("SELECT * FROM trades WHERE status='OPEN' ORDER BY monitor_next,id").fetchall()
        if not positions:
            return
        start = min(r['monitor_next'] for r in positions)
        if start>=through:
            return
        # Process all missed closed minutes. No time expiry or skipped recovery.
        count = (through-start)//60
        bars = candles(symbol,'1m',through,count)
        if bars[0][0]!=start:
            raise ValueError('MISSING_RECOVERY_CANDLES')
        for row in positions:
            p = dict(row)
            for b in bars:
                if b[0]<p['monitor_next']:
                    continue
                status,pnl,hit1 = observe(p,b)
                p['monitor_next'],p['hit1'] = b[0]+60,hit1
                if status!='OPEN':
                    p.update(status=status,pnl=pnl,closed_at=b[0]+60)
                    break
            with self.db:
                self.db.execute('UPDATE trades SET monitor_next=?,hit1=?,status=?,pnl=?,closed_at=? WHERE id=?',
                    (p['monitor_next'],p['hit1'],p['status'],p.get('pnl'),p.get('closed_at'),p['id']))

    def report(self,symbol,day):
        start = dt.datetime.combine(day,dt.time(),IRAN).timestamp()
        end = start+86400
        rows = [dict(r) for r in self.db.execute('SELECT * FROM trades WHERE opened<?',(end,))]
        lines = [symbol+' | گزارش روزانه','روز معاملاتی: '+day.strftime('%Y/%m/%d'),'زمان گزارش ایران: '+stamp(end),
                 'حجم هر معامله: ۱۰۰۰ دلار؛ بدون هزینه‌ها','']
        lines += ['حالت | ورود روز | TP1 قطعی | هدف نهایی | استاپ | نامشخص | باز']
        for mode,label in (('M1','اول'),('M2','دوم'),('LEGACY','قبلی')):
            own = [r for r in rows if r['mode']==mode]
            if mode=='LEGACY' and not own:
                continue
            cohort = [r for r in own if start<=r['opened']<end]
            c = collections.Counter(r['status'] for r in cohort)
            lines.append(f"{label} | {len(cohort)} | {sum(r['hit1'] for r in cohort)} | {c['TARGET']} | {c['SL']} | {c['UNKNOWN']} | {c['OPEN']}")
            exits = [r for r in own if r['closed_at'] is not None and start<=r['closed_at']<end]
            profit = sum(max(0,r['pnl']) for r in exits if r['pnl'] is not None)
            loss = sum(min(0,r['pnl']) for r in exits if r['pnl'] is not None)
            carry = sum(r['status']=='OPEN' and r['opened']<start for r in own)
            lines += [f'بسته‌شده‌های روز، حالت {label}: سود {profit:+.2f} | زیان {loss:+.2f} | خالص {profit+loss:+.2f} دلار',
                      f'بازِ روزهای قبل، حالت {label}: {carry}']
        lines += ['','جدول: وضعیت سیگنال‌های صادرشده در روز گزارش.',
                  'سود و زیان: تمام خروج‌های روز، شامل سیگنال‌های روزهای قبل.',
                  'نامشخص‌ها از سود و زیان کنار گذاشته شده‌اند؛ دقیقه ورود بررسی نمی‌شود.',
                  'پژوهشی؛ بدون سفارش واقعی']
        self.queue('report:'+day.isoformat(),'REPORT',end,'\n'.join(lines))
        self.put('last_report_day',day.isoformat())

class Sender(threading.Thread):
    def __init__(self,path,env,stop):
        super().__init__(daemon=True)
        self.path,self.env,self.stop = path,env,stop

    def run(self):
        token,chat = credentials(self.env)
        db = sqlite3.connect(self.path,timeout=20)
        while not self.stop.wait(1):
            row = db.execute("SELECT key,kind,at,text FROM outbox WHERE status='pending' ORDER BY at,key LIMIT 1").fetchone()
            if not row:
                continue
            key,kind,at,text = row
            if kind=='ENTRY' and time.time()-at>30:
                with db:
                    db.execute("UPDATE outbox SET status='expired' WHERE key=?",(key,))
                continue
            try:
                body = urllib.parse.urlencode(dict(chat_id=chat,text=text)).encode()
                req = urllib.request.Request('https://api.telegram.org/bot'+token+'/sendMessage',data=body)
                with urllib.request.urlopen(req,timeout=8) as response:
                    result = json.load(response)
                if not result.get('ok'):
                    raise RuntimeError('TELEGRAM_REJECTED')
                with db:
                    db.execute("UPDATE outbox SET status='sent',message_id=?,attempts=attempts+1 WHERE key=?",
                               (result['result']['message_id'],key))
            except Exception:
                with db:
                    db.execute('UPDATE outbox SET attempts=attempts+1 WHERE key=?',(key,))
                print('TELEGRAM_RETRY_PENDING',flush=True)
                self.stop.wait(10)
        db.close()

def btc_source(after=None,since=None):
    db = sqlite3.connect('file:/opt/tmco_webhook_v4/data/signals.db?mode=ro',uri=True,timeout=10)
    try:
        maximum = db.execute('SELECT COALESCE(MAX(id),0) FROM event_queue').fetchone()[0]
        query = ('SELECT q.id,c.direction,c.timeframe,c.received_at,c.bar_time FROM event_queue q '
                 "JOIN crosses c ON c.id=q.cross_id WHERE q.id<=? AND c.symbol IN ('BTCUSDT','BTCUSDT.P','BYBIT:BTCUSDT.P')")
        params = [maximum]
        if after is not None:
            query += ' AND q.id>?'
            params.append(after)
        if since is not None:
            query += ' AND q.id>MAX(0,?-10000)'
            params.append(maximum)
        rows = db.execute(query+' ORDER BY q.id',params).fetchall()
    finally:
        db.close()
    out = []
    for qid,side,tf,received,opened in rows:
        if tf not in ('1m','3m','5m') or not opened:
            continue
        at = dt.datetime.fromisoformat(opened).timestamp()+SECONDS[tf]
        received = dt.datetime.fromisoformat(received).timestamp()
        if since is None or at>=since:
            out.append((qid,dict(at=int(at),tf=tf,side=side),received))
    return maximum,out

def plan(symbol,mode,pair,frames,trend,tick,received):
    side = pair['side']
    if trend.local!=side:
        return None,'H1_TREND_NOT_ALIGNED'
    if not trend.bars or trend.bars[-1][0]+3600!=pair['at']//3600*3600:
        return None,'H1_TREND_STALE'
    if any(frames[tf][-1][0]+SECONDS[tf]!=pair['at']//SECONDS[tf]*SECONDS[tf]
           for tf in ('1m','3m','5m')):
        return None,'FILTER_CANDLES_STALE'
    f = filters(side,frames)
    if f['score']<4 or f['b3_reject']:
        return None,dict(reject='BTC_FILTERS',filters=f)
    ticker = api('/v5/market/tickers',category='linear',symbol=symbol)['list']
    if len(ticker)!=1 or ticker[0]['symbol']!=symbol:
        raise ValueError('INVALID_TICKER_SYMBOL')
    entry = float(ticker[0]['lastPrice'])
    now = time.time()
    if not 0<=now-received<=30 or now-pair['at']>45:
        return None,'STALE_DECISION'
    reference = frames['1m'][-1][4]
    if symbol=='BTCUSDT':
        reference = round(reference,2)
    sign = 1 if side=='LONG' else -1
    if not math.isfinite(entry) or entry<=0:
        raise ValueError('INVALID_ENTRY_PRICE')
    if sign*(entry-reference)/reference*100>.10:
        return None,'PRICE_CHASE'
    sl,tp1,tp2 = levels(side,entry,frames['5m'][-100:],tick)
    return dict(side=side,entry=entry,sl=sl,tp1=tp1,tp2=tp2,opened=now,
                decision=pair['at'],tick=tick,score=f['score'],filters=f,
                cross_pair=pair,policy=VERSION), 'ACCEPTED'

def fetch_frames(symbol,cache,cutoff):
    needed = [tf for tf in SECONDS if tf not in cache or cache[tf][-1][0]+SECONDS[tf]!=cutoff//SECONDS[tf]*SECONDS[tf]]
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        jobs = {tf:pool.submit(candles,symbol,tf,cutoff,300) for tf in needed}
        fresh = {tf:job.result() for tf,job in jobs.items()}
    cache.update(fresh)
    return cache

def run(args):
    import fcntl
    folder = Path(args.data_dir)
    folder.mkdir(parents=True,exist_ok=True)
    lock = (folder/'runner.lock').open('a')
    fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    store = Store(folder)
    stop = threading.Event()
    signal.signal(signal.SIGTERM,lambda *_:stop.set())
    signal.signal(signal.SIGINT,lambda *_:stop.set())
    inst = api('/v5/market/instruments-info',category='linear',symbol=args.symbol)['list']
    if len(inst)!=1 or inst[0]['symbol']!=args.symbol or inst[0]['status']!='Trading':
        raise ValueError('INVALID_INSTRUMENT')
    tick = float(inst[0]['priceFilter']['tickSize'])
    if not math.isfinite(tick) or tick<=0:
        raise ValueError('INVALID_TICK')
    trend = HourTrend(store.get('hour_trend'))
    cutoff = int(time.time())//60*60
    # Rebuild from at least 28 days once; persist the causal state thereafter.
    hours = candles(args.symbol,'1h',cutoff,672 if not trend.bars else 300)
    if trend.bars and hours[0][0]>trend.bars[-1][0]+3600:
        hours = candles(args.symbol,'1h',cutoff,(cutoff//3600*3600-trend.bars[-1][0])//3600)
    for b in hours:
        trend.step(b)
    cache = fetch_frames(args.symbol,{},cutoff)
    if args.check:
        print(json.dumps(dict(check_passed=True,symbol=args.symbol,tick=tick,
            source='BYBIT_LINEAR',hour_trend=trend.local,
            frames={k:v[-1][0]+SECONDS[k] for k,v in cache.items()},policy=VERSION)),flush=True)
        return
    gates = {m:CrossGate(m,store.get('gate_'+m)) for m in MODES}
    with store.db:
        if not store.get('last_report_day'):
            store.put('last_report_day',(dt.datetime.now(IRAN).date()-dt.timedelta(days=1)).isoformat())
        store.put('symbol',args.symbol)
    cursor = cutoff
    # Restore only sequence state; no historical entry after a restart.
    for mode,tfs in (('M1',('1m','3m','5m')),('M2',('15m','1h'))):
        if mode=='M1' and args.symbol=='BTCUSDT':
            queue_cursor,events = btc_source(since=cutoff-3600)
            for _,event,received in events:
                gates[mode].process(event,received)
        else:
            events = sorted([e for tf in tfs for e in crossings(cache[tf],tf)
                             if cutoff-7200<e['at']<=cutoff],key=lambda e:(e['at'],SECONDS[e['tf']]))
            for event in events:
                gates[mode].process(event,event['at']+1)
    if args.telegram:
        credentials(args.telegram_env)
        Sender(store.path,args.telegram_env,stop).start()
    print(json.dumps(dict(started=VERSION,symbol=args.symbol,hour_trend=trend.local)),flush=True)
    last_attempt = 0
    last_logged = 0
    try:
        while not stop.is_set():
            now = time.time()
            minute = int(now)//60*60
            saved_gates = {m:json.loads(json.dumps(gates[m].__dict__)) for m in MODES}
            saved_cursor = cursor
            try:
                # Stop recovery at each midnight, create its report, then continue.
                day = dt.date.fromisoformat(store.get('last_report_day'))+dt.timedelta(days=1)
                midnight = dt.datetime.combine(day+dt.timedelta(days=1),dt.time(),IRAN).timestamp()
                through = min(minute,int(midnight))
                store.monitor(args.symbol,through)
                if minute>=midnight:
                    with store.db:
                        store.report(args.symbol,day)
                    continue
                pairs = []
                source_next = None
                if args.symbol=='BTCUSDT':
                    source_next,events = btc_source(after=queue_cursor)
                    for _,event,received in events:
                        pair = gates['M1'].process(event,received)
                        if pair:
                            pairs.append(('M1',pair,received))
                if minute>cursor and now-last_attempt>=1:
                    last_attempt = now
                    fetch_frames(args.symbol,cache,minute)
                    for b in cache['1h']:
                        trend.step(b)
                    received = time.time()
                    for mode,tfs in (('M1',('1m','3m','5m')),('M2',('15m','1h'))):
                        if mode=='M1' and args.symbol=='BTCUSDT':
                            continue
                        events = sorted([e for tf in tfs for e in crossings(cache[tf],tf)
                                         if cursor<e['at']<=minute],key=lambda e:(e['at'],SECONDS[e['tf']]))
                        for event in events:
                            pair = gates[mode].process(event,received)
                            if pair:
                                pairs.append((mode,pair,received))
                    cursor = minute
                if pairs:
                    fetch_frames(args.symbol,cache,minute)
                    for b in cache['1h']:
                        trend.step(b)
                with store.db:
                    for mode,pair,received in pairs:
                        if store.get('activation_pending',False):
                            continue
                        key = mode+':'+str(pair['at'])+':'+pair['side']
                        if store.db.execute('SELECT 1 FROM decisions WHERE key=?',(key,)).fetchone():
                            continue
                        p,reason = plan(args.symbol,mode,pair,cache,trend,tick,received)
                        if p:
                            _,reason = store.add(args.symbol,mode,p,key)
                        store.db.execute('INSERT INTO decisions VALUES(?,?,?,?)',
                                         (key,pair['at'],mode,json.dumps(reason)))
                    for mode in MODES:
                        store.put('gate_'+mode,gates[mode].__dict__)
                    store.put('hour_trend',trend.__dict__)
                    store.put('heartbeat',dict(at=time.time(),minute=cursor,trend=trend.local))
                if source_next is not None:
                    queue_cursor = source_next
            except Exception as exc:
                gates = {m:CrossGate(m,saved_gates[m]) for m in MODES}
                cursor = saved_cursor
                if time.time()-last_logged>30:
                    print(json.dumps(dict(error=type(exc).__name__,detail=str(exc)[:120])),flush=True)
                    last_logged = time.time()
            stop.wait(.5)
    finally:
        store.db.close()
        lock.close()

def self_test():
    import tempfile
    from unittest.mock import patch
    event = lambda at,tf,side:dict(at=at,tf=tf,side=side)
    g = CrossGate('M1')
    assert g.process(event(600,'1m','LONG'),601) is None
    assert g.process(event(660,'3m','LONG'),661)
    assert g.process(event(720,'5m','LONG'),721) is None
    assert g.process(event(780,'1m','LONG'),781)
    assert g.process(event(780,'1m','LONG'),781) is None
    assert g.process(event(840,'3m','SHORT'),856) is None
    g = CrossGate('M2')
    assert g.process(event(900,'15m','LONG'),901) is None
    assert g.process(event(1800,'15m','LONG'),1801) is None
    assert g.process(event(3600,'1h','LONG'),3601)
    assert g.process(event(4500,'15m','SHORT'),4501) is None
    assert g.process(event(7200,'1h','LONG'),7201) is None
    restored = CrossGate('M2',json.loads(json.dumps(g.__dict__)))
    assert restored.__dict__==g.__dict__
    p = dict(side='LONG',entry=100,sl=90,tp1=115,tp2=125,hit1=0)
    assert observe(p,[0,100,116,99,115,1])==('OPEN',None,1)
    assert observe(p,[60,115,126,100,125,1])==('TARGET',250.,1)
    assert observe(p,[60,115,126,89,100,1])[0]=='UNKNOWN'
    p = dict(side='SHORT',entry=100,sl=110,tp1=85,tp2=75,hit1=0)
    assert observe(p,[0,100,101,74,75,1])==('TARGET',250.,1)
    bars = [[i*300,100,101,99,100,10] for i in range(100)]
    sl,a,b = levels('LONG',100,bars,.1)
    assert sl==97.6 and a==103.6 and b==106.
    for side in ('LONG','SHORT'):
        assert filters(side,{'1m':bars*3,'3m':bars,'5m':bars})['score']>=0
    # Crosses have prefix causality; future candles cannot change old events.
    bars = [[i*300,100,102,98,100+math.sin(i),10] for i in range(100)]
    all_events = crossings(bars,'5m')
    for n in range(20,100):
        assert crossings(bars[:n],'5m')==[e for e in all_events if e['at']<=n*300]
    h = HourTrend()
    hourly = [[i*3600,100,102,98,100+math.sin(i/3),10] for i in range(150)]
    for b in hourly[:100]:
        h.step(b)
    h2 = HourTrend(json.loads(json.dumps(h.__dict__)))
    for b in hourly[100:]:
        h.step(b);h2.step(b)
    assert h.__dict__==h2.__dict__
    with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
        s = Store(folder)
        p = dict(side='LONG',entry=100,sl=90,tp1=115,tp2=125,tick=.1,
                 opened=1000,decision=960,score=6)
        with s.db:
            assert s.add('ADAUSDT','M1',p,'a')[0]
            assert s.add('ADAUSDT','M1',dict(p,entry=101),'b')[1].startswith('WORSE')
            assert s.add('ADAUSDT','M2',dict(p,entry=101),'c')[0]
        assert s.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0]==2
        with s.db:
            s.report('ADAUSDT',dt.date(2026,10,29))
            s.report('ADAUSDT',dt.date(2026,10,29))
        assert s.db.execute("SELECT COUNT(*) FROM outbox WHERE kind='REPORT'").fetchone()[0]==1
        s.db.close()
    print('SELF_TEST_OK: cross pairs, timing, prefix causality, fixed exits, ticks, state restore, independent modes, daily-report idempotency')

def install():
    """Install two isolated paper services and migrate open legacy positions."""
    import os
    import shutil
    import subprocess
    import sys
    if os.geteuid()!=0:
        raise SystemExit('INSTALL_REQUIRES_ROOT')
    script=Path(__file__).resolve()
    interpreter=Path('/opt/btc_v13_research/venv_orderflow/bin/python')
    if script.parent!=Path('/opt/btc_v13_research') or not interpreter.exists():
        raise SystemExit('EXPECTED_RESEARCH_PATH_AND_VENV')
    old=['tmco-v4-paper.service','ada-v1-1-scalp-research.service']
    new=['btc-dual-h1-research.service','ada-dual-h1-research.service']
    folders=[Path('/opt/btc_dual_h1_research'),Path('/opt/ada_dual_h1_research')]
    sources=[Path('/opt/tmco_webhook_v4/data/v4_paper_live.db'),
             Path('/opt/ada_scalp_5m_research/ada_v1.sqlite3')]
    def command(args):
        return subprocess.run(args,check=True,capture_output=True,text=True,timeout=90).stdout
    def readonly(path):
        db=sqlite3.connect('file:'+str(path)+'?mode=ro',uri=True,timeout=20)
        db.row_factory=sqlite3.Row
        return db
    def inspect_legacy():
        result=[]
        with readonly(sources[0]) as db:
            for r in db.execute("SELECT * FROM paper_trades_v4 WHERE trade_status='active' ORDER BY signal_id"):
                if r['entry_status']!='filled' or r['entry_filled_at'] is None:
                    raise RuntimeError('UNSUPPORTED_UNFILLED_LEGACY_BTC')
                result.append(('BTCUSDT',dict(side=r['direction'],entry=float(r['entry']),
                    sl=float(r['sl']),tp1=float(r['tp1']),tp2=float(r['tp3']),
                    opened=float(r['entry_filled_at']),decision=int(r['decision_ts']),
                    tick=.1,score=0,hit1=int(r['tp1_hit'] or 0),
                    legacy_signal_id=int(r['signal_id']),policy='LEGACY_PRESERVE_LEVELS')))
        with readonly(sources[1]) as db:
            row=db.execute('SELECT payload FROM state WHERE id=1').fetchone()
            if not row:
                raise RuntimeError('ADA_STATE_MISSING')
            state=json.loads(row[0])
            if state.get('position'):
                raise RuntimeError('UNSUPPORTED_LEGACY_PARTIAL_ADA_POSITION; no changes made')
            for p in state.get('positions',[]):
                if p.get('remaining',1)<=0:
                    continue
                if p.get('management')!='BTC_FIXED_SL_FULL_POSITION' or len(p['targets'])!=3:
                    raise RuntimeError('UNSUPPORTED_LEGACY_ADA_MANAGEMENT')
                result.append(('ADAUSDT',dict(side=p['side'],entry=float(p['entry']),
                    sl=float(p.get('initial_stop',p['stop'])),tp1=float(p['targets'][0]),
                    tp2=float(p['targets'][2]),opened=float(p['opened']),decision=int(p['decision']),
                    tick=.0001,score=0,hit1=int(p.get('hit',0)>0),
                    legacy_signal_id=p.get('signal_id'),policy='LEGACY_PRESERVE_LEVELS')))
        return result
    def unit(symbol,folder):
        return ('[Unit]\nDescription='+symbol+' Independent Dual H1 Paper Research\n'
                'Wants=network-online.target\nAfter=network-online.target\n\n'
                '[Service]\nType=simple\nUser=root\nWorkingDirectory=/opt/btc_v13_research\n'
                'ExecStart='+str(interpreter)+' -B '+str(script)+' --watch --telegram --symbol '+symbol+
                ' --data-dir '+str(folder)+'\nRestart=always\nRestartSec=5\n'
                'Environment=PYTHONUNBUFFERED=1\nNoNewPrivileges=true\nUMask=0077\n\n'
                '[Install]\nWantedBy=multi-user.target\n')
    if any((Path('/etc/systemd/system')/name).exists() for name in new):
        raise SystemExit('NEW_SERVICE_ALREADY_EXISTS; inspect existing installation before retry')
    if any((folder/'dual.sqlite3').exists() for folder in folders):
        raise SystemExit('NEW_DATABASE_ALREADY_EXISTS; history preserved, installation not repeated')
    credentials('/opt/tmco_webhook_v4/telegram.env')
    self_test()
    # Validate the exact original entry points before stopping anything.
    expected=[('/usr/bin/python3','/opt/tmco_webhook_v4/v4_paper_live.py'),
              (str(interpreter),'/opt/btc_v13_research/ADA_V1_3_BTC_Rules_Scalp_Research.py')]
    original_state=[]
    for service,(exe,target) in zip(old,expected):
        description=command(['systemctl','show',service,'--property=ExecStart','--value'])
        if exe not in description or target not in description:
            raise RuntimeError('UNEXPECTED_OLD_EXECSTART_'+service)
        active=command(['systemctl','is-active',service]).strip()
        enabled=subprocess.run(['systemctl','is-enabled',service],capture_output=True,text=True).stdout.strip()
        if active!='active':
            raise RuntimeError('OLD_SERVICE_NOT_ACTIVE_'+service)
        original_state.append(dict(service=service,enabled=enabled))
    inspect_legacy()
    for symbol in ('BTCUSDT','ADAUSDT'):
        # A temporary, independent directory; --check never starts Telegram.
        import tempfile
        with tempfile.TemporaryDirectory(prefix='dual_preflight_') as temporary:
            print(command([str(interpreter),'-B',str(script),'--check','--symbol',symbol,
                           '--data-dir',temporary]).strip(),flush=True)
    btc_source(since=time.time()-3600)
    tag=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
    backup=Path('/opt/btc_v13_research')/('before_dual_h1_'+tag)
    backup.mkdir(mode=0o700)
    manifest=dict(version=VERSION,script_sha256=hashlib.sha256(script.read_bytes()).hexdigest(),
                  previous_services=original_state,at=time.time())
    (backup/'manifest.json').write_text(json.dumps(manifest,indent=2))
    for service in old:
        (backup/(service+'.txt')).write_text(command(['systemctl','cat',service]))
    stopped=False
    try:
        command(['systemctl','stop']+old)
        stopped=True
        for src in sources:
            target=backup/src.name
            source_db=readonly(src)
            destination=sqlite3.connect(target)
            try:
                source_db.backup(destination)
            finally:
                destination.close();source_db.close()
            print('DATABASE_BACKUP:',str(target),flush=True)
        positions=inspect_legacy()
        for symbol,folder in zip(('BTCUSDT','ADAUSDT'),folders):
            store=Store(folder)
            try:
                with store.db:
                    store.put('last_report_day',(dt.datetime.now(IRAN).date()-dt.timedelta(days=1)).isoformat())
                    store.put('symbol',symbol)
                    store.put('migration_backup',str(backup))
                    store.put('activation_pending',True)
                    for owner,p in positions:
                        if owner!=symbol:
                            continue
                        key='legacy:'+symbol+':'+str(p['legacy_signal_id'])
                        sid,reason=store.add(symbol,'LEGACY',p,key,legacy=True)
                        if sid is None:
                            raise RuntimeError('LEGACY_IMPORT_FAILED_'+reason)
            finally:
                store.db.close()
        for symbol,folder,name in zip(('BTCUSDT','ADAUSDT'),folders,new):
            (Path('/etc/systemd/system')/name).write_text(unit(symbol,folder))
        command(['systemctl','daemon-reload'])
        command(['systemctl','enable','--now']+new)
        # Wait for real heartbeats, not merely a running bootstrap process.
        deadline=time.monotonic()+80
        while time.monotonic()<deadline:
            ready=[]
            for folder in folders:
                with readonly(folder/'dual.sqlite3') as db:
                    row=db.execute("SELECT value FROM meta WHERE key='heartbeat'").fetchone()
                    ready.append(bool(row and time.time()-json.loads(row[0])['at']<20))
            if all(ready):
                break
            time.sleep(2)
        else:
            raise RuntimeError('NEW_SERVICES_DID_NOT_REACH_HEALTHY_HEARTBEAT')
        command(['systemctl','disable']+old)
        for folder in folders:
            ready_store=Store(folder)
            try:
                with ready_store.db:
                    ready_store.put('activation_pending',False)
            finally:
                ready_store.db.close()
        for service in new:
            print(service,command(['systemctl','show',service,
                '--property=ActiveState,SubState,MainPID,NRestarts,UnitFileState']).strip(),flush=True)
        print('OPEN_LEGACY_POSITIONS_PRESERVED:',len(positions))
        print('ROLLBACK_BACKUP:',str(backup))
        print('BTC_AND_ADA_DUAL_MODES_ACTIVE; ENTRY_MESSAGES_AND_IRAN_MIDNIGHT_REPORTS_ONLY')
    except Exception as exc:
        subprocess.run(['systemctl','disable','--now']+new,capture_output=True,text=True)
        # The new ledger remains for audit; no databases or history are deleted.
        for service in old:
            subprocess.run(['systemctl','start',service],capture_output=True,text=True)
        for state in original_state:
            if state['enabled']=='enabled':
                subprocess.run(['systemctl','enable',state['service']],capture_output=True,text=True)
        raise SystemExit('INSTALL_FAILED; OLD_SERVICES_RESTARTED; NEW_HISTORY_PRESERVED: '+type(exc).__name__+': '+str(exc))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--install',action='store_true')
    parser.add_argument('--self-test',action='store_true')
    parser.add_argument('--check',action='store_true')
    parser.add_argument('--watch',action='store_true')
    parser.add_argument('--symbol',choices=('BTCUSDT','ADAUSDT'))
    parser.add_argument('--data-dir')
    parser.add_argument('--telegram',action='store_true')
    parser.add_argument('--telegram-env',default='/opt/tmco_webhook_v4/telegram.env')
    args = parser.parse_args()
    if args.install:
        install()
        return
    if args.self_test:
        self_test()
        return
    if not args.symbol or not args.data_dir or args.check==args.watch:
        parser.error('choose --check or --watch, with --symbol and --data-dir')
    if args.telegram and not args.watch:
        parser.error('--telegram requires --watch')
    run(args)

if __name__=='__main__':
    main()
