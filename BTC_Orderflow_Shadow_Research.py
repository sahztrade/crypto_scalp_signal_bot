#!/usr/bin/env python3
"""BTC order-flow shadow recorder: public Bybit trades, no signal/order/Telegram writes."""
import argparse, collections, datetime as dt, hashlib, importlib.util, json, math
import os, signal, sqlite3, subprocess, sys, tempfile, threading, time, uuid
from pathlib import Path
from contextlib import closing

VERSION = 'BTC-ORDERFLOW-SHADOW-V1'
SYMBOL = 'BTCUSDT'
WS_URL = 'wss://stream.bybit.com/v5/public/linear'
SOURCE_DEFAULT = Path('/opt/btc_dual_h1_research/dual.sqlite3')
DATA_DEFAULT = Path('/opt/btc_orderflow_shadow')
PYTHON = Path('/opt/btc_v13_research/venv_orderflow/bin/python')
UNIT = 'btc-orderflow-shadow.service'
IRAN = dt.timezone(dt.timedelta(hours=3, minutes=30))
WINDOWS = (60,180,300)


def encoded(value):
    return json.dumps(value,ensure_ascii=False,allow_nan=False,separators=(',',':'))


def iran(at):
    return dt.datetime.fromtimestamp(float(at),IRAN).isoformat() if at is not None else None


def readonly(path):
    db = sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=5)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    return db


class Flow:
    def __init__(self):
        self.lock = threading.RLock()
        self.session = uuid.uuid4().hex
        self.generation = 0
        self.segments = []
        self.connected = False
        self.buckets = {}
        self.seen = {}
        self.dirty = set()
        self.events = []
        self.last_recv = None
        self.last_event = None
        self.last_message = None
        self.last_error = None
        self.duplicates = 0
        self.block_ignored = 0
        self.total_trades = 0
        self.order = 0
        self.last_trim = 0

    def begin(self,at):
        with self.lock:
            self.generation += 1
            self.connected = True
            self.last_recv = None
            self.last_event = None
            self.last_error = None
            self.segments.append(dict(start=float(at),end=None,generation=self.generation))
            self.events.append((at,'CONNECTED',dict(generation=self.generation)))

    def end(self,at,reason):
        with self.lock:
            if self.connected and self.segments:
                # Conservatively stop coverage at the last received valid trade.
                self.segments[-1]['end'] = self.last_recv or self.segments[-1]['start']
            self.connected = False
            self.last_error = str(reason)[:160]
            self.events.append((at,'DISCONNECTED',dict(reason=self.last_error)))

    def ingest(self,data,received):
        if not isinstance(data,list):
            raise ValueError('INVALID_TRADE_BATCH')
        parsed = []
        for d in data:
            if d.get('s')!=SYMBOL or d.get('S') not in ('Buy','Sell'):
                raise ValueError('INVALID_TRADE_SYMBOL_OR_SIDE')
            at = int(d['T'])
            price,size = float(d['p']),float(d['v'])
            identity = str(d['i'])
            if not identity or not all(math.isfinite(x) and x>0 for x in (price,size,price*size)):
                raise ValueError('INVALID_TRADE_VALUES')
            if at/1000>received+2 or received-at/1000>10:
                raise ValueError('TRADE_CLOCK_OR_LATENCY_ERROR')
            parsed.append((d,at,price,size,identity))
        with self.lock:
            if not self.connected:
                return
            self.last_message = received
            for d,at,price,size,identity in parsed:
                if identity in self.seen:
                    self.duplicates += 1
                    continue
                self.seen[identity] = received
                if d.get('BT',False):
                    self.block_ignored += 1
                    continue
                self.last_recv = received
                self.last_event = max(self.last_event or 0,at/1000)
                self.total_trades += 1
                self.order += 1
                event_sec,recv_sec = at//1000,int(received)
                key = (self.generation,event_sec,recv_sec)
                b = self.buckets.get(key)
                point = [at,self.order]
                if b is None:
                    b = dict(event_sec=event_sec,recv_sec=recv_sec,generation=self.generation,
                        buy_usdt=0.,sell_usdt=0.,buy_btc=0.,sell_btc=0.,trades=0,rpi_trades=0,
                        first_key=point,last_key=point,first_price=price,last_price=price,
                        high=price,low=price,first_received=received,last_received=received)
                    self.buckets[key] = b
                prefix = 'buy' if d['S']=='Buy' else 'sell'
                b[prefix+'_usdt'] += price*size
                b[prefix+'_btc'] += size
                b['trades'] += 1
                b['rpi_trades'] += int(bool(d.get('RPI',False)))
                if point<b['first_key']:
                    b['first_key'],b['first_price'] = point,price
                if point>b['last_key']:
                    b['last_key'],b['last_price'] = point,price
                b['high'],b['low'] = max(b['high'],price),min(b['low'],price)
                b['first_received'] = min(b['first_received'],received)
                b['last_received'] = max(b['last_received'],received)
                self.dirty.add(key)

    def features(self,cutoff,side=None):
        cutoff = float(cutoff)
        if cutoff!=int(cutoff):
            raise ValueError('INTEGER_CUTOFF_REQUIRED')
        with self.lock:
            answer = {}
            for seconds in WINDOWS:
                start = cutoff-seconds
                covered = any(s['start']<=start and (s['end'] is None or s['end']>=cutoff)
                              for s in self.segments)
                # Both exchange event time and local reception must precede the decision.
                own = [b for b in self.buckets.values()
                       if start<=b['event_sec']<cutoff and b['recv_sec']<cutoff]
                total_trades = sum(b['trades'] for b in own)
                if not covered:
                    quality = 'INCOMPLETE_CONNECTION_WINDOW'
                elif not own:
                    quality = 'NO_TRADES'
                elif cutoff-max(b['last_key'][0] for b in own)/1000>10:
                    quality = 'NO_RECENT_TRADES_AT_CUTOFF'
                else:
                    quality = 'OBSERVED_TRANSPORT_WINDOW'
                usable = quality=='OBSERVED_TRANSPORT_WINDOW'
                if not usable:
                    answer[str(seconds)] = dict(seconds=seconds,usable=False,quality=quality,
                        connection_window_covered=covered,observed_trades=total_trades,
                        buy_usdt=None,sell_usdt=None,delta_usdt=None,imbalance=None,
                        cvd_change_btc=None,price_change_pct=None)
                    continue
                buy,sell = [sum(b[k] for b in own) for k in ('buy_usdt','sell_usdt')]
                first = min(own,key=lambda b:b['first_key'])
                last = max(own,key=lambda b:b['last_key'])
                delta = buy-sell
                imbalance = delta/(buy+sell) if buy+sell>0 else None
                change = 100*(last['last_price']/first['first_price']-1)
                cvd = sum(b['buy_btc']-b['sell_btc'] for b in own)
                sign = 1 if side=='LONG' else -1 if side=='SHORT' else None
                answer[str(seconds)] = dict(seconds=seconds,usable=True,quality=quality,
                    connection_window_covered=True,trades=total_trades,
                    buy_usdt=buy,sell_usdt=sell,delta_usdt=delta,imbalance=imbalance,
                    cvd_change_btc=cvd,first_price=first['first_price'],last_price=last['last_price'],
                    high=max(b['high'] for b in own),low=min(b['low'] for b in own),
                    price_change_pct=change,
                    signed_imbalance=sign*imbalance if sign is not None and imbalance is not None else None,
                    signed_price_change_pct=sign*change if sign is not None else None,
                    possible_sell_absorption=imbalance<=-.10 and change>=0,
                    possible_buy_absorption=imbalance>=.10 and change<=0,
                    first_event_ms=first['first_key'][0],last_event_ms=last['last_key'][0],
                    max_received_at=max(b['last_received'] for b in own),
                    rpi_trades=sum(b['rpi_trades'] for b in own))
            return dict(cutoff=cutoff,cutoff_iran=iran(cutoff),side=side,windows=answer,
                        session=self.session,method='EVENT_AND_RECEIPT_BEFORE_CUTOFF')

    def health(self,now):
        with self.lock:
            age = now-self.last_recv if self.last_recv is not None else None
            return dict(connected=self.connected,healthy=bool(self.connected and age is not None and 0<=age<=10),
                trade_age_seconds=age,last_error=self.last_error,generation=self.generation,
                total_trades=self.total_trades,duplicates_ignored=self.duplicates,
                block_trades_ignored=self.block_ignored,session=self.session)

    def drain(self,now):
        with self.lock:
            values = [(self.session+':'+':'.join(map(str,k)),self.buckets[k]['event_sec'],
                       encoded(self.buckets[k])) for k in self.dirty]
            self.dirty.clear()
            events,self.events = self.events,[]
            if now-self.last_trim>=10:
                self.buckets = {k:b for k,b in self.buckets.items() if b['event_sec']>=now-900}
                self.seen = {k:t for k,t in self.seen.items() if t>=now-900}
                self.segments = [s for s in self.segments if s['end'] is None or s['end']>=now-900]
                self.last_trim = now
            return values,events


class Receiver(threading.Thread):
    def __init__(self,flow,stop):
        super().__init__(daemon=True)
        self.flow,self.stop = flow,stop

    def run(self):
        import websocket
        while not self.stop.is_set():
            ws = None
            try:
                ws = websocket.create_connection(WS_URL,timeout=10,enable_multithread=True)
                ws.send(encoded(dict(op='subscribe',args=['publicTrade.'+SYMBOL])))
                subscribed = False
                connected_at = last_ping = time.monotonic()
                last_trade = time.monotonic()
                while not self.stop.is_set():
                    if time.monotonic()-last_ping>=20:
                        ws.send(encoded(dict(op='ping')))
                        last_ping = time.monotonic()
                    if not subscribed and time.monotonic()-connected_at>10:
                        raise RuntimeError('SUBSCRIPTION_TIMEOUT')
                    if subscribed and time.monotonic()-last_trade>15:
                        raise RuntimeError('TRADE_STREAM_STALE')
                    ws.settimeout(1)
                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue
                    received = time.time()
                    if not raw:
                        raise RuntimeError('WEBSOCKET_CLOSED')
                    msg = json.loads(raw)
                    if msg.get('op')=='subscribe':
                        if not msg.get('success'):
                            raise RuntimeError('SUBSCRIPTION_REJECTED')
                        subscribed = True
                        self.flow.begin(received)
                    elif msg.get('topic')=='publicTrade.'+SYMBOL and subscribed:
                        self.flow.ingest(msg.get('data'),received)
                        last_trade = time.monotonic()
            except Exception as exc:
                self.flow.end(time.time(),type(exc).__name__+': '+str(exc)[:100])
            finally:
                if ws:
                    try:
                        ws.close()
                    except Exception:
                        pass
            self.stop.wait(2)


class ShadowStore:
    def __init__(self,folder):
        folder = Path(folder)
        folder.mkdir(parents=True,exist_ok=True)
        self.path = folder/'orderflow.sqlite3'
        self.pending_buckets = {}
        self.pending_events = []
        self.db = sqlite3.connect(self.path,timeout=10)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS buckets(key TEXT PRIMARY KEY,event_sec INTEGER,payload TEXT);
            CREATE INDEX IF NOT EXISTS bucket_time ON buckets(event_sec);
            CREATE TABLE IF NOT EXISTS samples(at INTEGER PRIMARY KEY,payload TEXT);
            CREATE TABLE IF NOT EXISTS decisions(source_rowid INTEGER PRIMARY KEY,key TEXT UNIQUE,
                at REAL,mode TEXT,side TEXT,result TEXT,seen_at REAL,features TEXT);
            CREATE TABLE IF NOT EXISTS trades(id INTEGER PRIMARY KEY,payload TEXT);
            CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY,at REAL,kind TEXT,payload TEXT);
        ''')
        old = self.get('version')
        if old not in (None,VERSION):
            raise ValueError('SHADOW_DATABASE_VERSION_MISMATCH')
        with self.db:
            self.put('version',VERSION)

    def get(self,key,default=None):
        r = self.db.execute('SELECT value FROM meta WHERE key=?',(key,)).fetchone()
        return json.loads(r[0]) if r else default

    def put(self,key,value):
        self.db.execute('INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                        (key,encoded(value)))

    def poll(self,source,flow,now):
        with closing(readonly(source)) as src:
            src.execute('BEGIN')
            if self.get('source_checkpoint') is None:
                checkpoint = src.execute('SELECT COALESCE(MAX(rowid),0) FROM decisions').fetchone()[0]
                with self.db:
                    self.put('source_checkpoint',checkpoint)
                    self.put('first_started',now)
                    self.put('source_path',str(Path(source).resolve()))
            else:
                if self.get('source_path')!=str(Path(source).resolve()):
                    raise ValueError('SOURCE_DATABASE_PATH_CHANGED')
                checkpoint = self.get('source_checkpoint')
            rows = src.execute('SELECT rowid AS source_rowid,* FROM decisions WHERE rowid>? ORDER BY rowid',
                               (checkpoint,)).fetchall()
            trades = [dict(r) for r in src.execute("SELECT * FROM trades WHERE symbol='BTCUSDT'")]
            source_heartbeat = src.execute("SELECT value FROM meta WHERE key='heartbeat'").fetchone()
        with self.db:
            for r in rows:
                side = r['key'].split(':')[-1]
                if side not in ('LONG','SHORT'):
                    raise ValueError('UNKNOWN_SOURCE_DECISION_KEY')
                features = flow.features(r['at'],side)
                self.db.execute('INSERT OR IGNORE INTO decisions VALUES(?,?,?,?,?,?,?,?)',
                    (r['source_rowid'],r['key'],r['at'],r['mode'],side,r['result'],now,encoded(features)))
                checkpoint = r['source_rowid']
            self.put('source_checkpoint',checkpoint)
            self.put('source_heartbeat',json.loads(source_heartbeat[0]) if source_heartbeat else None)
            for r in trades:
                # Keep only market/trade fields; no credentials or Telegram data are copied.
                details = json.loads(r['details']) if r.get('details') else {}
                own = {k:r.get(k) for k in ('id','symbol','mode','side','entry','sl','tp1','tp2','opened',
                    'decision','status','hit1','closed_at','pnl','source_key','legacy')}
                own['entry_path'] = details.get('entry_path')
                payload = encoded(own)
                self.db.execute('INSERT INTO trades VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload '
                                'WHERE trades.payload!=excluded.payload',(r['id'],payload))
            self.put('last_source_poll',now)

    def persist(self,flow,now):
        buckets,events = flow.drain(now)
        self.pending_buckets.update((r[0],r) for r in buckets)
        self.pending_events.extend((at,kind,encoded(data)) for at,kind,data in events)
        with self.db:
            self.db.executemany('INSERT INTO buckets VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload',
                                list(self.pending_buckets.values()))
            self.db.executemany('INSERT INTO events(at,kind,payload) VALUES(?,?,?)',self.pending_events)
            self.put('heartbeat',dict(at=now,health=flow.health(now),version=VERSION))
        self.pending_buckets.clear()
        self.pending_events.clear()

    def prune(self,now,hours):
        with self.db:
            for table,column in (('buckets','event_sec'),('samples','at'),('events','at')):
                self.db.execute('DELETE FROM '+table+' WHERE rowid IN (SELECT rowid FROM '+table+
                    ' WHERE '+column+'<? LIMIT 20000)',(now-hours*3600,))
        self.db.execute('PRAGMA wal_checkpoint(PASSIVE)')


def require_websocket():
    if importlib.util.find_spec('websocket') is None:
        raise RuntimeError('DEPENDENCY_MISSING: websocket-client in orderflow venv')
    import websocket
    if not hasattr(websocket,'create_connection'):
        raise RuntimeError('INCOMPATIBLE_WEBSOCKET_PACKAGE')


def live_check(seconds):
    require_websocket()
    flow,stop = Flow(),threading.Event()
    worker = Receiver(flow,stop)
    worker.start()
    started = time.monotonic()
    try:
        while time.monotonic()-started<seconds:
            stop.wait(.5)
        now = time.time()
        output = dict(check_passed=flow.health(now)['healthy'],health=flow.health(now),
                      features=flow.features(int(now)),read_only=True)
        print(encoded(output),flush=True)
        if not output['check_passed']:
            raise RuntimeError('LIVE_TRADE_CHECK_FAILED')
    finally:
        stop.set()
        worker.join(timeout=3)


def watch(args):
    import fcntl
    require_websocket()
    folder = Path(args.data_dir)
    folder.mkdir(parents=True,exist_ok=True)
    lock = (folder/'recorder.lock').open('a')
    fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    store,flow,stop = ShadowStore(folder),Flow(),threading.Event()
    signal.signal(signal.SIGTERM,lambda *_:stop.set())
    signal.signal(signal.SIGINT,lambda *_:stop.set())
    # Establish the source baseline before opening the feed. Older decisions are never fabricated.
    store.poll(args.source_db,flow,time.time())
    worker = Receiver(flow,stop)
    worker.start()
    last_poll = last_sample = last_prune = 0
    last_error = 0
    print(encoded(dict(started=VERSION,source_read_only=str(args.source_db),data_dir=str(folder))),flush=True)
    try:
        while not stop.is_set():
            now = time.time()
            try:
                store.persist(flow,now)
                if now-last_poll>=1:
                    store.poll(args.source_db,flow,now)
                    last_poll = now
                sample_at = int(now)//30*30
                if sample_at>last_sample:
                    with store.db:
                        store.db.execute('INSERT OR IGNORE INTO samples VALUES(?,?)',
                                         (sample_at,encoded(flow.features(sample_at))))
                    last_sample = sample_at
                if now-last_prune>=60:
                    store.prune(now,args.retention_hours)
                    last_prune = now
            except Exception as exc:
                store.db.rollback()
                if now-last_error>=30:
                    print(encoded(dict(error=type(exc).__name__,detail=str(exc)[:160])),flush=True)
                    last_error = now
            stop.wait(.5)
    finally:
        stop.set()
        worker.join(timeout=3)
        store.persist(flow,time.time())
        store.db.close()
        lock.close()


def report(args):
    path = Path(args.data_dir)/'orderflow.sqlite3'
    now,start = time.time(),time.time()-args.report_hours*3600
    with closing(readonly(path)) as db:
        heartbeat = db.execute("SELECT value FROM meta WHERE key='heartbeat'").fetchone()
        poll = db.execute("SELECT value FROM meta WHERE key='last_source_poll'").fetchone()
        source = db.execute("SELECT value FROM meta WHERE key='source_path'").fetchone()
        heartbeat = json.loads(heartbeat[0]) if heartbeat else None
        last_poll = json.loads(poll[0]) if poll else None
        decisions = [dict(r) for r in db.execute('SELECT * FROM decisions WHERE at>=? ORDER BY at',(start,))]
        trades = [json.loads(r[0]) for r in db.execute('SELECT payload FROM trades')]
        latest_sample = db.execute('SELECT payload FROM samples ORDER BY at DESC LIMIT 1').fetchone()
    # Refresh paper outcomes from the live ledger in read-only mode, never change it.
    refresh_error = None
    try:
        if source:
            with closing(readonly(json.loads(source[0]))) as db:
                outcomes = {r['id']:dict(r) for r in db.execute("SELECT * FROM trades WHERE symbol='BTCUSDT'")}
            for t in trades:
                if t['id'] in outcomes:
                    for k in ('status','hit1','closed_at','pnl'):
                        t[k] = outcomes[t['id']][k]
    except Exception as exc:
        refresh_error = type(exc).__name__+': '+str(exc)[:100]
    by_key = {r['key']:r for r in decisions}
    counts = collections.Counter()
    for r in decisions:
        f = json.loads(r['features'])
        for seconds in WINDOWS:
            quality = f['windows'][str(seconds)]['quality']
            counts[(r['mode'],seconds,quality)] += 1
    own = []
    for t in sorted(trades,key=lambda x:x['opened']):
        if t['opened']<start:
            continue
        r = by_key.get(t['source_key'])
        features = json.loads(r['features']) if r else None
        own.append(dict(id=t['id'],mode=t['mode'],side=t['side'],time_iran=iran(t['opened']),
            entry=t['entry'],sl=t['sl'],tp1=t['tp1'],final_target=t['tp2'],
            result=t['status'],pnl_1000_usdt=t['pnl'],entry_path=t.get('entry_path'),
            flow=features if features else 'NOT_CAPTURED_AT_THIS_DECISION'))
    print('REPORT:',encoded(dict(version=VERSION,from_iran=iran(start),to_iran=iran(now),
        heartbeat=heartbeat,heartbeat_age_seconds=now-heartbeat['at'] if heartbeat else None,
        last_source_poll_iran=iran(last_poll),source_poll_age_seconds=now-last_poll if last_poll else None,
        outcome_refresh_error=refresh_error,latest_sample=json.loads(latest_sample[0]) if latest_sample else None)))
    print('COVERAGE:',encoded([dict(mode=mode,window_seconds=seconds,quality=quality,count=count)
        for (mode,seconds,quality),count in sorted(counts.items())]))
    print('TRADES:',json.dumps(own,ensure_ascii=False,indent=2))
    print('LATEST_DECISIONS:',json.dumps([dict(key=r['key'],time_iran=iran(r['at']),mode=r['mode'],
        result=json.loads(r['result']),features=json.loads(r['features']))
        for r in decisions[-10:]],ensure_ascii=False,indent=2))
    print('SHADOW_ONLY; NO ENTRY FILTERS OR TELEGRAM CHANGES; NO HISTORICAL FLOW FABRICATED')


def self_test():
    def trade(at,side,price,size,identity,**extra):
        return dict(T=int(at*1000),S=side,p=str(price),v=str(size),i=identity,s=SYMBOL,**extra)
    f = Flow()
    f.begin(0)
    f.ingest([trade(58,'Buy',100,1,'a')],58.1)
    f.ingest([trade(59,'Sell',99,2,'b')],59.5)
    f.ingest([trade(59,'Sell',99,2,'b')],59.6)
    f.ingest([trade(59.5,'Buy',1000,100,'late')],60.2)
    feature = f.features(60,'SHORT')['windows']['60']
    assert feature['usable'] and feature['trades']==2
    assert feature['buy_usdt']==100 and feature['sell_usdt']==198
    assert feature['delta_usdt']==-98 and feature['cvd_change_btc']==-1
    assert math.isclose(feature['price_change_pct'],-1)
    assert feature['signed_imbalance']>0 and f.duplicates==1
    frozen = encoded(f.features(60,'SHORT'))
    f.ingest([trade(65,'Buy',200,3,'future')],65.1)
    assert encoded(f.features(60,'SHORT'))==frozen, 'FUTURE_DATA_CHANGED_PAST_FEATURE'
    assert feature['max_received_at']<60
    assert not f.features(60)['windows']['180']['usable']
    f.end(66,'test_disconnect')
    f.begin(80)
    f.ingest([trade(90,'Buy',100,1,'new')],90.1)
    assert not f.features(91)['windows']['60']['usable'], 'RECONNECT_WARMUP_FAILED'
    f.ingest([trade(140,'Sell',100,1,'warm')],140.1)
    assert f.features(141)['windows']['60']['usable']
    f.ingest([trade(141,'Buy',100,100,'block',BT=True)],141.1)
    assert f.block_ignored==1
    assert f.features(142)['windows']['60']['buy_usdt']==100
    try:
        f.ingest([trade(200,'Buy',100,1,'bad_future')],150)
        raise AssertionError('CLOCK_CHECK_MISSING')
    except ValueError:
        pass
    g = Flow()
    g.begin(0)
    tape = []
    for i in range(1,302):
        side = 'Buy' if i%3 else 'Sell'
        price = 100+(i%7)/10
        g.ingest([trade(i,side,price,1,str(i))],i+.1)
        tape.append((i,side,price))
    for seconds in WINDOWS:
        result = g.features(301,'LONG')['windows'][str(seconds)]
        selected = [t for t in tape if 301-seconds<=t[0]<301]
        assert result['trades']==len(selected)==seconds
        buy = sum(p for _,s,p in selected if s=='Buy')
        sell = sum(p for _,s,p in selected if s=='Sell')
        assert math.isclose(result['delta_usdt'],buy-sell)
        assert math.isclose(result['cvd_change_btc'],sum(1 if s=='Buy' else -1 for _,s,_ in selected))
    with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
        source = Path(folder)/'source.sqlite3'
        with closing(sqlite3.connect(source)) as db, db:
            db.executescript('''
                CREATE TABLE decisions(key TEXT PRIMARY KEY,at REAL,mode TEXT,result TEXT);
                CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT);
                CREATE TABLE trades(id INTEGER PRIMARY KEY,symbol TEXT,mode TEXT,side TEXT,entry REAL,
                    sl REAL,tp1 REAL,tp2 REAL,opened REAL,decision REAL,status TEXT,hit1 INTEGER,
                    closed_at REAL,pnl REAL,source_key TEXT,legacy INTEGER,details TEXT);
            ''')
            db.execute('INSERT INTO decisions VALUES(?,?,?,?)',('M1:30:LONG',30,'M1',encoded('ACCEPTED')))
            db.execute('INSERT INTO meta VALUES(?,?)',('heartbeat',encoded(dict(at=30))))
        store = ShadowStore(Path(folder)/'shadow')
        store.poll(source,f,50)
        assert store.get('source_checkpoint')==1
        assert store.db.execute('SELECT COUNT(*) FROM decisions').fetchone()[0]==0
        with closing(sqlite3.connect(source)) as db, db:
            db.execute('INSERT INTO decisions VALUES(?,?,?,?)',('M1:60:SHORT',60,'M1',encoded('ACCEPTED')))
        store.poll(source,f,61)
        row = store.db.execute('SELECT features FROM decisions').fetchone()[0]
        store.poll(source,f,62)
        assert store.db.execute('SELECT COUNT(*) FROM decisions').fetchone()[0]==1
        assert store.db.execute('SELECT features FROM decisions').fetchone()[0]==row
        with closing(readonly(source)) as db:
            try:
                db.execute('DELETE FROM decisions')
                raise AssertionError('SOURCE_WRITE_WAS_ALLOWED')
            except sqlite3.OperationalError:
                pass
        store.persist(g,302)
        assert store.db.execute('SELECT COUNT(*) FROM buckets').fetchone()[0]>0
        store.db.close()
    print('SELF_TEST_OK: taker side, notional delta, CVD windows, receipt causality, future exclusion, dedup, reconnect warmup, block exclusion, source read-only, immutable decision snapshots')


def service_value(unit,key):
    return subprocess.check_output(['systemctl','show',unit,'--property='+key,'--value'],text=True).strip()


def install():
    if not hasattr(os,'geteuid') or os.geteuid()!=0:
        raise SystemExit('ROOT_REQUIRED')
    target = Path('/opt/btc_v13_research/BTC_Orderflow_Shadow_Research.py')
    if Path(__file__).resolve()!=target or Path(sys.executable).resolve()!=PYTHON.resolve():
        raise SystemExit('EXPECTED_SCRIPT_PATH_AND_ORDERFLOW_VENV_REQUIRED')
    self_test()
    require_websocket()
    with closing(readonly(SOURCE_DEFAULT)) as src:
        for table in ('decisions','trades','meta'):
            src.execute('SELECT 1 FROM '+table+' LIMIT 1').fetchall()
    live_check(15)
    unit_path = Path('/etc/systemd/system')/UNIT
    text = ('[Unit]\nDescription=BTC Bybit Orderflow Shadow Research - No Signal Changes\n'
        'Wants=network-online.target\nAfter=network-online.target\n\n[Service]\nType=simple\nUser=root\n'
        'WorkingDirectory=/opt/btc_v13_research\n'
        'ExecStart='+str(PYTHON)+' -B '+str(target)+' --watch\n'
        'Restart=always\nRestartSec=5\nNoNewPrivileges=true\nUMask=0077\n\n'
        '[Install]\nWantedBy=multi-user.target\n')
    if unit_path.exists() and unit_path.read_text()!=text:
        raise SystemExit('EXISTING_SHADOW_SERVICE_DIFFERS; NO CHANGES')
    others = {u:service_value(u,'MainPID') for u in
        ('btc-dual-h1-research.service','ada-dual-h1-research.service')}
    created = not unit_path.exists()
    if not created and service_value(UNIT,'ActiveState')=='active':
        print('SHADOW_SERVICE_ALREADY_ACTIVE; NO RESTART PERFORMED')
        return
    try:
        if created:
            with unit_path.open('x') as stream:
                stream.write(text)
            os.chmod(unit_path,0o644)
        subprocess.run(['systemctl','daemon-reload'],check=True)
        began = time.time()
        subprocess.run(['systemctl','enable','--now',UNIT],check=True)
        deadline = time.monotonic()+100
        verified = None
        while time.monotonic()<deadline:
            try:
                with closing(readonly(DATA_DEFAULT/'orderflow.sqlite3')) as db:
                    row = db.execute("SELECT value FROM meta WHERE key='heartbeat'").fetchone()
                    poll = db.execute("SELECT value FROM meta WHERE key='last_source_poll'").fetchone()
                    sample = db.execute('SELECT payload FROM samples ORDER BY at DESC LIMIT 1').fetchone()
                h = json.loads(row[0]) if row else {}
                sample = json.loads(sample[0]) if sample else {}
                poll_at = json.loads(poll[0]) if poll else 0
                if (h.get('at',0)>=began and time.time()-h['at']<5
                    and h.get('health',{}).get('healthy') and time.time()-poll_at<5):
                    verified = h
                    break
            except (sqlite3.Error,FileNotFoundError,ValueError):
                pass
            time.sleep(1)
        if verified is None:
            raise RuntimeError('SHADOW_FEED_OR_SOURCE_POLL_NOT_VERIFIED')
        for unit,pid in others.items():
            if service_value(unit,'MainPID')!=pid:
                raise RuntimeError('SIGNAL_SERVICE_PID_CHANGED_DURING_INSTALL: '+unit)
        print(subprocess.check_output(['systemctl','show',UNIT,
            '--property=MainPID,NRestarts,ActiveState,SubState,UnitFileState'],text=True),flush=True)
        print('SHADOW_HEARTBEAT:',encoded(verified),flush=True)
        print('BTC_ORDERFLOW_SHADOW_ACTIVE; 1M_3M_5M_WINDOWS_WARM_UP_AFTER_CONNECTION')
        print('BTC_AND_ADA_SIGNAL_SERVICE_PIDS_UNCHANGED; SOURCE_DATABASE_READ_ONLY')
        print('NO SIGNAL FILTER, STOP, TARGET, ORDER OR TELEGRAM CHANGES')
    except BaseException:
        if created:
            subprocess.run(['systemctl','disable','--now',UNIT],check=False)
            if unit_path.exists() and unit_path.read_text()==text:
                unit_path.unlink()
            subprocess.run(['systemctl','daemon-reload'],check=False)
        raise


def main():
    p = argparse.ArgumentParser(description=__doc__)
    action = p.add_mutually_exclusive_group(required=True)
    action.add_argument('--self-test',action='store_true')
    action.add_argument('--check',type=int,metavar='SECONDS')
    action.add_argument('--watch',action='store_true')
    action.add_argument('--report',action='store_true')
    action.add_argument('--install',action='store_true')
    p.add_argument('--source-db',type=Path,default=SOURCE_DEFAULT)
    p.add_argument('--data-dir',type=Path,default=DATA_DEFAULT)
    p.add_argument('--report-hours',type=float,default=24)
    p.add_argument('--retention-hours',type=int,default=48)
    args = p.parse_args()
    if args.retention_hours<1 or args.report_hours<=0 or (args.check is not None and not 1<=args.check<=600):
        p.error('invalid duration')
    if args.self_test:
        self_test()
    elif args.check is not None:
        live_check(args.check)
    elif args.report:
        report(args)
    elif args.install:
        install()
    else:
        watch(args)


if __name__=='__main__':
    main()
