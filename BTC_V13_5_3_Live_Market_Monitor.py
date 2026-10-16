#!/usr/bin/env python3
# coding: utf-8
"""V13.5 independent Bybit live research monitor. No order placement endpoints.
Public market data only; Telegram is explicit --watch --telegram opt-in.
"""
import argparse
from collections import deque
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import statistics
import threading
import time
import urllib.parse
import urllib.request

VERSION = 'V13.5.3'
STORE_VERSION = 'V13.5.0'  # Compatible storage schema; preserve existing state/outbox.
IRAN = dt.timezone(dt.timedelta(hours=3, minutes=30))
API = 'https://api.bybit.com'
WS = 'wss://stream.bybit.com/v5/public/linear'
FRAMES = {'3m':180, '5m':300, '15m':900}


def stamp(epoch):
    return dt.datetime.fromtimestamp(epoch, IRAN).isoformat()


def validate(bars, seconds):
    if not bars:
        raise ValueError('EMPTY_CANDLES')
    for i, b in enumerate(bars):
        if len(b) < 5 or not all(math.isfinite(float(v)) for v in b[:5]):
            raise ValueError('INVALID_CANDLE')
        if b[0] != int(b[0]) or int(b[0]) % seconds or b[3] <= 0 or b[3] > min(b[1],b[4]) or b[2] < max(b[1],b[4]):
            raise ValueError('INVALID_OHLC_OR_ALIGNMENT')
        if i and b[0]-bars[i-1][0] != seconds:
            raise ValueError('CANDLE_GAP')


def aggregate15(bars):
    groups = {}
    for b in bars:
        groups.setdefault(int(b[0])//900*900, []).append(b)
    return [(s,g[0][1],max(b[2] for b in g),min(b[3] for b in g),g[-1][4])
            for s,g in sorted(groups.items()) if len(g)==3 and
            all(b[0]==s+i*300 for i,b in enumerate(g))]


def indicators(bars):
    closes = [b[4] for b in bars]
    ema = closes[0]
    values = []
    for close in closes:
        ema += 2/21*(close-ema)
        values.append(ema)
    tr = [max(b[2]-b[3],abs(b[2]-bars[i-1][4]),abs(b[3]-bars[i-1][4]))
          if i else b[2]-b[3] for i,b in enumerate(bars)]
    atr = sum(tr[-14:])/14
    if atr <= 0:
        atr = 1e-12  # Flat candles remain neutral; avoid division by zero.
    travel = sum(abs(closes[i]-closes[i-1]) for i in range(len(closes)-20,len(closes)))
    er = abs(closes[-1]-closes[-21])/travel if travel else 0.0
    return {'ema':ema, 'atr':atr, 'slope':(values[-1]-values[-6])/atr, 'er':er}


def quantile(values, q):
    values = sorted(values)
    return values[int((len(values)-1)*q)]


class Market:
    def __init__(self, tf, saved=None):
        self.tf = tf
        self.bars = []
        self.regime = 'UNCERTAIN'
        self.direction = 'FLAT'
        self.box = None
        self.proposal = None
        self.stop = None
        self.counter = 0
        self.counter_side = None
        self.last_transition = 0
        self.last_range_exit = 0
        self.range_candidate = None
        self.ind = None
        self.last3 = 0
        self.recent_three = []
        self.swing_high = None
        self.swing_low = None
        if saved:
            self.__dict__.update(saved)

    def switch(self, target, at, reason):
        old = self.regime
        if old=='RANGE' and target in ('UP','DOWN'):
            self.last_range_exit = at
        self.range_candidate = None
        self.regime = target
        self.last_transition = at
        self.counter = 0
        self.counter_side = None
        self.proposal = None
        return {'kind':'REGIME_CHANGED','tf':self.tf,'from':old,'to':target,'reason':reason,'at':at}

    def on_bar(self, bar):
        seconds = FRAMES[self.tf]
        if self.bars and bar[0] <= self.bars[-1][0]:
            return []
        if self.bars and bar[0] != self.bars[-1][0]+seconds:
            raise ValueError('FRAME_GAP_'+self.tf)
        self.bars.append(list(bar))
        self.bars = self.bars[-300:]
        if len(self.bars) < 65:
            return []
        self.ind = indicators(self.bars)
        atr, ema, slope, er = [self.ind[k] for k in ('atr','ema','slope','er')]
        at = int(bar[0])+seconds
        move = (bar[4]-self.bars[-4][4])/atr
        new_direction = 'UP' if move>0.25 else 'DOWN' if move < -0.25 else 'FLAT'
        events = []
        if new_direction != self.direction:
            events.append({'kind':'DIRECTION_CHANGED','tf':self.tf,'from':self.direction,'to':new_direction,'at':at})
            self.direction = new_direction
        pivot = self.bars[-3]
        neighbors = self.bars[-5:-3]+self.bars[-2:]
        if all(pivot[2]>x[2] for x in neighbors):
            self.swing_high = {'price':pivot[2],'open':pivot[0],'known_at':at}
        if all(pivot[3]<x[3] for x in neighbors):
            self.swing_low = {'price':pivot[3],'open':pivot[0],'known_at':at}
        # Cooldown follows a range EXIT, not every trend update; cap it at 30m.
        # Strong broad sideways structure may contain a directional short leg.
        can_range = self.box is None and (not self.last_range_exit or at-self.last_range_exit >= min(6*seconds,1800))
        range_matched = False
        if can_range:
            windows = {'3m':(72,48,36,24),'5m':(72,48,36,24),'15m':(40,32,24)}[self.tf]
            recent = self.bars[-6:]
            recent_travel = sum(abs(recent[i][4]-recent[i-1][4]) for i in range(1,6))
            recent_er = abs(recent[-1][4]-recent[0][4])/recent_travel if recent_travel else 0
            for n in windows:
                h = self.bars[-n-1:-1]
                if len(h)!=n:
                    continue
                low = quantile([b[3] for b in h],.10)
                high = quantile([b[2] for b in h],.90)
                width = high-low
                travel = sum(abs(h[i][4]-h[i-1][4]) for i in range(1,n))
                long_er = abs(h[-1][4]-h[0][4])/travel if travel else 0
                touches = all(any(b[3]<=low+.2*width for b in part) and
                              any(b[2]>=high-.2*width for b in part)
                              for part in (h[:n//2],h[n//2:]))
                if 0<width<=8*atr and long_er<=.30 and (recent_er<=.40 or long_er<=.20) and touches and low<=bar[4]<=high:
                    range_matched = True
                    candidate = self.range_candidate
                    overlap = max(0,min(high,candidate['resistance'])-max(low,candidate['support'])) if candidate else 0
                    union = max(high,candidate['resistance'])-min(low,candidate['support']) if candidate else width
                    if (candidate and at==candidate['last_seen']+seconds and overlap/union>=.70 and
                            candidate['support']<=bar[4]<=candidate['resistance']):
                        candidate['count'] += 1
                        candidate['last_seen'] = at
                    else:
                        candidate = {'support':low,'resistance':high,'buffer':.25*atr,'window':n,
                                     'first_seen':at,'last_seen':at,'count':1}
                    self.range_candidate = candidate
                    if candidate['count']>=2:
                        self.box = {'support':candidate['support'],'resistance':candidate['resistance'],
                                    'buffer':candidate['buffer'],'formed_at':at,'window':candidate['window']}
                        events.append(self.switch('RANGE',at,'TWO_OWN_TF_CLOSES_CONFIRM_RANGE'))
                        self.stop = None
                    break
        if self.box is None and not range_matched:
            self.range_candidate = None
        # Trail only after a pivot has two closed candles on each side.
        if self.regime in ('UP','DOWN') and self.stop is not None:
            pivot = self.bars[-3]
            neighbors = self.bars[-5:-3]+self.bars[-2:]
            candidate_stop = None
            if self.regime=='UP' and all(pivot[3]<x[3] for x in neighbors):
                candidate_stop = max(self.stop,pivot[3]-.15*atr)
            elif self.regime=='DOWN' and all(pivot[2]>x[2] for x in neighbors):
                candidate_stop = min(self.stop,pivot[2]+.15*atr)
            if candidate_stop is not None and candidate_stop!=self.stop:
                events.append({'kind':'INVALIDATION_MOVED','tf':self.tf,'from':self.stop,'to':candidate_stop,'at':at})
                self.stop = candidate_stop
                self.counter = 0
                self.counter_side = None
        history = self.bars[-7:-1]
        if self.regime == 'UNCERTAIN':
            side = 'UP' if slope>=.25 and er>=.20 and bar[4]>ema else 'DOWN' if slope<=-.25 and er>=.20 and bar[4]<ema else None
            if self.proposal and (self.proposal['side']!=side or at-self.proposal['created']>6*seconds):
                self.proposal = None
                self.counter = 0
                self.counter_side = None
            if side and self.proposal is None:
                self.proposal = {'side':side, 'created':at, 'buffer':.10*atr,
                                 'level':max(b[2] for b in history) if side=='UP' else min(b[3] for b in history),
                                 'stop':min(b[3] for b in history) if side=='UP' else max(b[2] for b in history)}
        return events

    def on_three(self, bar):
        at = int(bar[0])+180
        if at<=self.last3:
            return []
        self.last3 = at
        self.recent_three = (self.recent_three+[list(bar)])[-2:]
        if self.ind is None:
            return []
        if self.regime!='RANGE' and len(self.recent_three)==2 and self.swing_high and self.swing_low:
            high,low = self.swing_high,self.swing_low
            buffer = .10*self.ind['atr']
            up = low['open']>high['open'] and all(x[0]>=high['known_at'] and x[4]>high['price']+buffer for x in self.recent_three)
            down = high['open']>low['open'] and all(x[0]>=low['known_at'] and x[4]<low['price']-buffer for x in self.recent_three)
            side = 'UP' if up else 'DOWN' if down else None
            if side and side!=self.regime:
                event = self.switch(side,at,'TWO_3M_CLOSES_BEYOND_CONFIRMED_OWN_TF_PIVOT')
                self.stop = low['price']-.15*self.ind['atr'] if side=='UP' else high['price']+.15*self.ind['atr']
                return [event]
        target = None
        reason = None
        next_stop = None
        if self.regime=='RANGE' and self.box:
            b = self.box
            if bar[0] < b['formed_at']:
                return []
            target = 'UP' if bar[4]>b['resistance']+b['buffer'] else 'DOWN' if bar[4]<b['support']-b['buffer'] else None
            reason = 'TWO_3M_CLOSES_OUTSIDE_FROZEN_RANGE'
            next_stop = b['support']-b['buffer'] if target=='UP' else b['resistance']+b['buffer']
        elif self.regime in ('UP','DOWN') and self.stop is not None:
            crossed = bar[4]<self.stop if self.regime=='UP' else bar[4]>self.stop
            target = 'UNCERTAIN' if crossed else None
            reason = 'TWO_3M_CLOSES_BEYOND_TREND_INVALIDATION'
        elif self.regime=='UNCERTAIN' and self.proposal:
            p = self.proposal
            if bar[0] < p['created']:
                return []
            crossed = bar[4]>p['level']+p['buffer'] if p['side']=='UP' else bar[4]<p['level']-p['buffer']
            target = p['side'] if crossed else None
            reason = 'TWO_3M_CLOSES_BEYOND_OWN_TF_STRUCTURE'
            next_stop = p['stop']
        self.counter = self.counter+1 if target and target==self.counter_side else 1 if target else 0
        self.counter_side = target
        if self.counter<2:
            return []
        event = self.switch(target,at,reason)
        self.box = None
        self.stop = next_stop
        return [event]

    def view(self):
        last = self.bars[-1] if self.bars else None
        return {'regime':self.regime,'direction':self.direction,'close':last[4] if last else None,
                'candle_end':int(last[0])+FRAMES[self.tf] if last else None,
                'support':self.box['support'] if self.box else None,
                'resistance':self.box['resistance'] if self.box else None,
                'invalidation':self.stop,'pending':self.counter_side,'confirmation_closes':self.counter,
                'indicators':self.ind}


class Reversal:
    def __init__(self, saved=None):
        self.pending = {}
        self.active = []
        if saved:
            self.__dict__.update(saved)

    def step(self, bars):
        if len(bars)<11:
            return []
        b = bars[-1]
        at = int(b[0])+180
        events = []
        for a in list(self.active):
            invalid = b[4]<a['extreme'] if a['side']=='UP' else b[4]>a['extreme']
            zone = a.get('zone')
            zone_invalid = zone and (b[4]<zone['low'] if a['side']=='UP' else b[4]>zone['high'])
            if invalid or zone_invalid or at-a['confirmed_at']>3600:
                events.append({'kind':'EARLY_INVALIDATED' if invalid or zone_invalid else 'EARLY_EXPIRED',
                               'side':a['side'],'at':at,'origin':a['origin'],'zone':zone})
                self.active.remove(a)
        for side, c in list(self.pending.items()):
            invalid = b[4]<c['extreme'] if side=='UP' else b[4]>c['extreme']
            if invalid or at-c['origin']>6*180:
                if c['local_announced']:
                    events.append({'kind':'EARLY_INVALIDATED' if invalid else 'EARLY_EXPIRED','side':side,'at':at,'origin':c['origin'],'zone':c.get('zone')})
                del self.pending[side]
                continue
            outside = b[4]>c['trigger'] if side=='UP' else b[4]<c['trigger']
            context = b[4]>c['context'] if side=='UP' else b[4]<c['context']
            c['local_count'] = c['local_count']+1 if outside else 0
            c['context_count'] = c['context_count']+1 if context else 0
            if outside and c['local_count']==1 and not c['local_announced']:
                zone = None
                for x in reversed([x for x in bars if c['origin']-180<=x[0]<b[0]]):
                    if (x[4]<x[1] if side=='UP' else x[4]>x[1]):
                        zone = {'low':x[3],'high':x[2],'candle_open_ts':x[0]}
                        break
                c['zone'] = zone
            if c['local_count']>=2 and not c['local_announced']:
                c['local_announced'] = True
                if c.get('zone'):
                    c['zone']['known_at_close_ts'] = at
                events.append({'kind':'EARLY_LOCAL','side':side,'at':at,'origin':c['origin'],
                               'level':c['trigger'],'zone':c.get('zone')})
            if c['local_count']>=2 and c['context_count']>=2:
                events.append({'kind':'EARLY_CONTEXT_CONFIRMED','side':side,'at':at,'origin':c['origin'],
                               'level':c['context'],'zone':c.get('zone')})
                self.active.append(dict(c, confirmed_at=at))
                del self.pending[side]
        h = bars[-11:-1]
        low, high = min(x[3] for x in h), max(x[2] for x in h)
        sides = []
        if b[3]<low and b[4]>low and b[4]>b[1]:
            sides.append(('UP',b[3],b[2],high))
        if b[2]>high and b[4]<high and b[4]<b[1]:
            sides.append(('DOWN',b[2],b[3],low))
        for side, extreme, trigger, context in sides:
            previous = self.pending.get(side)
            if previous and previous['local_announced']:
                events.append({'kind':'EARLY_REPLACED','side':side,'at':at,'origin':previous['origin']})
            self.pending[side] = dict(side=side,extreme=extreme,trigger=trigger,context=context,
                                      origin=at,local_count=0,context_count=0,local_announced=False)
        return events


class Engine:
    def __init__(self, saved=None):
        self.frames = {tf:Market(tf, saved['frames'][tf] if saved else None) for tf in FRAMES}
        self.reversal = Reversal(saved.get('reversal') if saved else None)
        self.last_decision = saved.get('last_decision',0) if saved else 0

    def revalidate_ranges(self):
        # Upgrade existing state without inventing bars or changing active range bounds.
        # Two already-closed own-TF bars are rechecked; only non-range states are eligible.
        changes = []
        for tf, machine in self.frames.items():
            if machine.regime=='RANGE' or len(machine.bars)<67:
                continue
            bars = list(machine.bars)
            before = machine.regime
            machine.bars = bars[:-2]
            machine.range_candidate = None
            for bar in bars[-2:]:
                machine.on_bar(bar)
            if before!=machine.regime:
                changes.append({'tf':tf,'from':before,'to':machine.regime})
        return changes

    def dump(self):
        return {'version':VERSION,'frames':{k:v.__dict__ for k,v in self.frames.items()},
                'reversal':self.reversal.__dict__,'last_decision':self.last_decision}

    def view(self):
        return {k:v.view() for k,v in self.frames.items()}

    def ingest(self, five, three):
        # Merge by close time, own 15m/5m structures BEFORE 3m confirmations on ties.
        fifteen = aggregate15(five)
        timeline = [(int(b[0])+300,1,'5m',b) for b in five]
        timeline += [(int(b[0])+900,0,'15m',b) for b in fifteen]
        timeline += [(int(b[0])+180,2,'3m',b) for b in three]
        events = []
        for at, _, tf, b in sorted(timeline):
            machine = self.frames[tf]
            if machine.bars and b[0]<=machine.bars[-1][0]:
                continue
            # Never consume a 3m decision until required 5m/15m closes are available.
            if tf=='3m' and at>self.last_decision:
                f = self.frames['5m'].bars
                q = self.frames['15m'].bars
                if f and int(f[-1][0])+300 < at//300*300:
                    break
                if q and int(q[-1][0])+900 < at//900*900:
                    break
            current_events = machine.on_bar(b)
            if tf=='3m':
                if machine.ind is not None:
                    for m in self.frames.values():
                        current_events += m.on_three(b)
                    current_events += self.reversal.step(machine.bars)
                self.last_decision = at
            for event in current_events:
                event['frames'] = self.view()
            events += current_events
        return events


def fetch_bars(seconds, cutoff, count):
    last = int(cutoff)//seconds*seconds-seconds
    remaining, end, all_rows = count, (last+seconds)*1000-1, {}
    while remaining>0:
        params = urllib.parse.urlencode(dict(category='linear',symbol='BTCUSDT',interval=str(seconds//60),
                                             end=end,limit=min(1000,remaining)))
        req = urllib.request.Request(API+'/v5/market/kline?'+params,headers={'User-Agent':'BTC-V13.5-research'})
        with urllib.request.urlopen(req,timeout=15) as response:
            answer = json.load(response)
        if answer.get('time') and abs(float(answer['time'])/1000-time.time())>15:
            raise RuntimeError('SERVER_CLOCK_DIFFERS_FROM_BYBIT')
        if answer.get('retCode')!=0:
            raise RuntimeError('BYBIT_KLINE_REJECTED')
        page = answer['result']['list']
        if not page:
            raise RuntimeError('BYBIT_EMPTY_PAGE')
        before = len(all_rows)
        for row in page:
            b = [int(row[0])//1000]+[float(v) for v in row[1:5]]
            if b[0]+seconds<=cutoff:
                all_rows[b[0]] = b
        if len(all_rows)==before:
            raise RuntimeError('BYBIT_PAGE_DID_NOT_ADVANCE')
        remaining = count-len(all_rows)
        end = min(int(r[0]) for r in page)-1
    bars = sorted(all_rows.values())[-count:]
    validate(bars,seconds)
    if bars[-1][0]!=last:
        raise RuntimeError('LATEST_CANDLE_MISMATCH')
    return bars


class Store:
    def __init__(self, directory):
        directory = Path(directory)
        directory.mkdir(parents=True,exist_ok=True)
        self.path = directory/'v13_5_monitor.sqlite3'
        self.db = sqlite3.connect(str(self.path),timeout=15)
        existing = self.db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        if existing:
            try:
                owner = self.db.execute('SELECT value FROM meta WHERE key=?',('owner',)).fetchone()
            except sqlite3.Error:
                self.db.close()
                raise ValueError('REFUSE_FOREIGN_DATABASE')
            if not owner or owner[0]!=STORE_VERSION:
                self.db.close()
                raise ValueError('DATABASE_VERSION_MISMATCH')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA busy_timeout=15000')
        self.db.executescript("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);"
                             "CREATE TABLE IF NOT EXISTS engine(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL);"
                             "CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY,at REAL NOT NULL,payload TEXT NOT NULL,delivered INTEGER NOT NULL,attempts INTEGER NOT NULL DEFAULT 0);"
                             "CREATE TABLE IF NOT EXISTS samples(at INTEGER PRIMARY KEY,payload TEXT NOT NULL);")
        self.db.execute('INSERT OR IGNORE INTO meta VALUES(?,?)',('owner',STORE_VERSION))
        self.db.commit()

    def load(self):
        row = self.db.execute('SELECT payload FROM engine WHERE id=1').fetchone()
        return json.loads(row[0]) if row else None

    def archive_before_upgrade(self, saved):
        key = 'state_before_'+VERSION
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO meta VALUES(?,?)',(key,json.dumps(saved)))
            # Keep every row for audit; retire pending messages based on the old policy.
            self.db.execute('UPDATE events SET delivered=2 WHERE delivered=0')

    def save(self, engine, events, telegram, context):
        important = {'STARTED','CATCHUP','FEED_CHANGED'}
        grouped = {}
        for event in events:
            if event['kind'] in ('REGIME_CHANGED','EARLY_CONTEXT_CONFIRMED'):
                grouped.setdefault(event['at'],[]).append(event)
        notices = []
        for at,group in grouped.items():
            changes = [{k:v for k,v in event.items() if k!='frames'} for event in group]
            notices.append({'kind':'MARKET_UPDATE','at':at,'changes':changes,
                            'frames':group[-1].get('frames',engine.view())})
        with self.db:
            self.db.execute('INSERT INTO engine VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                            (json.dumps(engine.dump()),))
            for event in events+notices:
                payload = dict(event,live=context)
                payload.setdefault('frames',engine.view())
                identity = {k:event.get(k) for k in ('kind','at','tf','side','origin','from','to')}
                event_id = hashlib.sha256(json.dumps(identity,sort_keys=True).encode()).hexdigest()[:24]
                send = telegram and (event['kind'] in important or event['kind']=='MARKET_UPDATE')
                self.db.execute('INSERT OR IGNORE INTO events(id,at,payload,delivered) VALUES(?,?,?,?)',
                                (event_id,event['at'],json.dumps(payload,ensure_ascii=False),0 if send else 1))

    def sample(self, context, now):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO samples VALUES(?,?)',(int(now)//10*10,json.dumps(context)))
            self.db.execute('DELETE FROM samples WHERE at<?',(now-7*86400,))


def credentials(path):
    values = {}
    for line in Path(path).read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key,value = line.removeprefix('export ').split('=',1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    if not values.get('BOT_TOKEN') or not values.get('CHAT_ID'):
        raise ValueError('BOT_TOKEN_AND_CHAT_ID_REQUIRED')
    return values['BOT_TOKEN'],values['CHAT_ID']


def telegram_text(payload):
    names = {'UP':'صعودی 🟢','DOWN':'نزولی 🔴','RANGE':'رنج 🟠','UNCERTAIN':'نامشخص ⚪','FLAT':'خنثی'}
    labels = {'MARKET_UPDATE':'گزارش تغییر ساختار و هشدار مهم','INVALIDATION_MOVED':'جابجایی سطح ابطال ساختار','REGIME_CHANGED':'تغییر ساختار بازار','DIRECTION_CHANGED':'تغییر حرکت کوتاه‌مدت',
              'EARLY_LOCAL':'هشدار برگشت محلی؛ هنوز روند تأیید نشده',
              'EARLY_CONTEXT_CONFIRMED':'شکست تأییدشدهٔ سقف/کف زمینه',
              'EARLY_INVALIDATED':'ابطال هشدار برگشت','EARLY_EXPIRED':'پایان اعتبار هشدار',
              'EARLY_REPLACED':'جایگزینی هشدار برگشت','FEED_CHANGED':'تغییر سلامت داده',
              'FLOW_CHANGED':'تغییر فشار معاملات','ZONES_CHANGED':'تغییر نواحی نقدینگی',
              'STARTED':'شروع مانیتور مستقل','CATCHUP':'بازیابی کندل‌های زمان قطعی'}
    lines = ['BTCUSDT | V13.5.3 پژوهشی',labels.get(payload['kind'],payload['kind']),stamp(payload['at'])]
    for change in payload.get('changes',[]):
        if change['kind']=='REGIME_CHANGED':
            lines.append(change['tf']+': '+names[change['from']]+' → '+names[change['to']])
        elif change['kind']=='EARLY_CONTEXT_CONFIRMED':
            lines.append('شکست زمینهٔ ۳ دقیقه: '+names[change['side']])
            if change.get('zone'):
                z = change['zone']
                lines.append('نامزد اردربلاک: '+format(z['low'],',.1f')+' تا '+format(z['high'],',.1f'))
    if payload.get('tf'):
        lines.append(payload['tf']+': '+names.get(payload.get('from'),str(payload.get('from')))+' → '+names.get(payload.get('to'),str(payload.get('to'))))
    if payload.get('side'):
        lines.append('سمت هشدار: '+names[payload['side']])
    for tf,x in payload.get('frames',{}).items():
        lines.append(tf+': '+names[x['regime']]+' | حرکت: '+names[x['direction']])
        if x.get('support') is not None:
            lines.append('  حمایت '+format(x['support'],',.1f')+' | مقاومت '+format(x['resistance'],',.1f'))
    if payload.get('zone'):
        z = payload['zone']
        lines.append('نامزد اردربلاک: '+format(z['low'],',.1f')+' تا '+format(z['high'],',.1f'))
    live = payload.get('live',{})
    health_names = {'OK':'سالم','WARMING_UP':'در حال تکمیل داده','DEGRADED':'داده زنده ناقص','CANDLES_DEGRADED':'کندل ناقص/قدیمی','HISTORICAL_NO_ORDERFLOW':'بازیابی تاریخی بدون جریان سفارش'}
    flow_names = {'BUY_PRESSURE':'فشار خرید','SELL_PRESSURE':'فشار فروش','BALANCED':'متعادل','UNAVAILABLE':'داده کافی نیست'}
    lines.append('سلامت: '+health_names.get(live.get('health'),'نامشخص')+' | فشار: '+flow_names.get(live.get('flow'),'داده کافی نیست'))
    for side in ('bid_zone','ask_zone'):
        z = live.get(side)
        if z:
            lines.append(('ناحیه خرید: ' if side=='bid_zone' else 'ناحیه فروش: ')+str(z['low'])+' تا '+str(z['high'])+' | '+format(z['notional'],',.0f')+' USDT')
    if payload.get('reason'):
        reasons = {'OWN_TF_RANGE_ESTABLISHED':'تشکیل رنج از کندل‌های همان تایم‌فریم',
                   'TWO_OWN_TF_CLOSES_CONFIRM_RANGE':'دو کندل بسته‌شدهٔ همان تایم‌فریم، رنج را تأیید کردند',
                   'TWO_3M_CLOSES_OUTSIDE_FROZEN_RANGE':'دو کندل ۳ دقیقه بیرون مرز ثابت رنج',
                   'TWO_3M_CLOSES_BEYOND_TREND_INVALIDATION':'دو کندل ۳ دقیقه آن‌سوی سطح ابطال روند',
                   'TWO_3M_CLOSES_BEYOND_OWN_TF_STRUCTURE':'دو کندل ۳ دقیقه آن‌سوی ساختار تایم‌فریم',
                   'TWO_3M_CLOSES_BEYOND_CONFIRMED_OWN_TF_PIVOT':'دو کندل ۳ دقیقه آن‌سوی سقف/کف تأییدشدهٔ تایم‌فریم',
                   'UPDATED_POLICY_REVALIDATED':'اصلاح روش اعلان و بازبینی ساختارهای غیررنج',
                   'RESTORED_STATE':'بازیابی وضعیت ذخیره‌شده', 'HISTORICAL_BOOTSTRAP':'آماده‌سازی با کندل‌های تاریخی',
                   'HISTORICAL_EVENTS_SUPPRESSED_AFTER_GAP':'خلاصهٔ بازیابی پس از قطعی؛ جزئیات در پایگاه پژوهشی ثبت شد'}
        lines.append('دلیل: '+reasons.get(payload['reason'],payload['reason']))
    lines.append('پژوهشی؛ بدون اجرای معامله')
    return '\n'.join(lines)[:3900]


class Sender(threading.Thread):
    def __init__(self, path, token, chat, stop):
        super().__init__(daemon=True)
        self.path,self.token,self.chat,self.stop = path,token,chat,stop

    def run(self):
        db = sqlite3.connect(str(self.path),timeout=15)
        delay = 0
        try:
            while not self.stop.wait(delay):
                delay = 5
                row = db.execute('SELECT id,payload FROM events WHERE delivered=0 ORDER BY at,rowid LIMIT 1').fetchone()
                if not row:
                    continue
                try:
                    body = urllib.parse.urlencode(dict(chat_id=self.chat,text=telegram_text(json.loads(row[1])))).encode()
                    request = urllib.request.Request('https://api.telegram.org/bot'+self.token+'/sendMessage',data=body)
                    with urllib.request.urlopen(request,timeout=12) as response:
                        answer = json.load(response)
                    if not answer.get('ok'):
                        raise RuntimeError('TELEGRAM_REJECTED')
                    with db:
                        db.execute('UPDATE events SET delivered=1,attempts=attempts+1 WHERE id=?',(row[0],))
                except Exception:
                    with db:
                        db.execute('UPDATE events SET attempts=attempts+1 WHERE id=?',(row[0],))
                    print('TELEGRAM_RETRY_PENDING',flush=True)
                    delay = 30
        finally:
            db.close()


class LiveFeed(threading.Thread):
    def __init__(self, stop, depth=1000):
        super().__init__(daemon=True)
        self.stop_event = stop
        self.depth = depth
        self.lock = threading.Lock()
        self.wake = threading.Event()
        self.connected = False
        self.book_ready = False
        self.bids,self.asks = {},{}
        self.book_at,self.trade_at = 0,0
        self.u,self.seq = 0,0
        self.session_start = 0
        self.trades = deque()
        self.ids = set()
        self.overflow_at = 0
        self.walls = {}
        self.last_error = None

    def reset(self):
        with self.lock:
            self.book_ready = False
            self.bids,self.asks = {},{}
            self.u,self.seq = 0,0
            self.book_at,self.trade_at = 0,0
            self.trades.clear()
            self.ids.clear()
            self.walls.clear()
            self.session_start = time.time()
            self.overflow_at = 0

    def process(self, msg, now):
        topic = msg.get('topic','')
        if topic == 'orderbook.'+str(self.depth)+'.BTCUSDT':
            d = msg['data']
            snapshot = msg.get('type')=='snapshot'
            with self.lock:
                if snapshot:
                    self.bids,self.asks = {},{}
                    self.walls.clear()
                elif not self.book_ready:
                    raise ValueError('DELTA_WITHOUT_SNAPSHOT')
                elif int(d['u'])==1:
                    raise ValueError('BOOK_RESET_REQUIRES_SNAPSHOT')
                elif int(d['u'])<=self.u or int(d.get('seq',self.seq+1))<=self.seq:
                    return
                for key, book in (('b',self.bids),('a',self.asks)):
                    for p,size in d.get(key,[]):
                        price,amount = float(p),float(size)
                        if not math.isfinite(price) or not math.isfinite(amount) or price<=0 or amount<0:
                            raise ValueError('INVALID_BOOK_LEVEL')
                        if amount==0:
                            book.pop(price,None)
                        else:
                            book[price] = amount
                self.bids = dict(sorted(self.bids.items(),reverse=True)[:self.depth])
                self.asks = dict(sorted(self.asks.items())[:self.depth])
                if not self.bids or not self.asks or max(self.bids)>=min(self.asks):
                    raise ValueError('EMPTY_OR_CROSSED_BOOK')
                self.book_ready = True
                self.book_at = float(msg.get('cts',msg.get('ts',0)))/1000
                self.u,self.seq = int(d['u']),int(d.get('seq',0))
        elif topic=='publicTrade.BTCUSDT':
            with self.lock:
                for trade in msg.get('data',[]):
                    if trade.get('BT') is True:
                        continue  # Negotiated block trades are not ordinary aggressive book flow.
                    identity = str(trade['i'])
                    at = float(trade['T'])/1000
                    if identity in self.ids or at<self.session_start-2 or at>now+5:
                        continue
                    if trade['S'] not in ('Buy','Sell'):
                        raise ValueError('INVALID_TRADE_SIDE')
                    notional = float(trade['p'])*float(trade['v'])
                    if not math.isfinite(notional) or notional<0:
                        raise ValueError('INVALID_TRADE')
                    if len(self.trades)>=200000:
                        old = self.trades.popleft()
                        self.ids.discard(old[3])
                        self.overflow_at = now
                    self.ids.add(identity)
                    self.trades.append((at,trade['S'],notional,identity))
                    self.trade_at = max(self.trade_at,at)
                while self.trades and self.trades[0][0]<now-120:
                    self.ids.discard(self.trades.popleft()[3])
        elif topic in ('kline.3.BTCUSDT','kline.5.BTCUSDT'):
            if any(x.get('confirm') is True for x in msg.get('data',[])):
                self.wake.set()

    def features(self, now):
        with self.lock:
            book_age = now-self.book_at if self.book_at else None
            trade_age = now-self.trade_at if self.trade_at else None
            fresh = self.connected and self.book_ready and book_age is not None and -5<=book_age<=5 and trade_age is not None and -5<=trade_age<=15
            complete = fresh and now-self.session_start>=60 and (not self.overflow_at or now-self.overflow_at>=60)
            health = 'OK' if complete else 'WARMING_UP' if fresh else 'DEGRADED'
            recent = [x for x in self.trades if now-60<=x[0]<=now]
            buy = sum(x[2] for x in recent if x[1]=='Buy')
            sell = sum(x[2] for x in recent if x[1]=='Sell')
            total = buy+sell
            imbalance = (buy-sell)/total if total else None
            flow = ('BUY_PRESSURE' if imbalance>=.20 else 'SELL_PRESSURE' if imbalance<=-.20 else 'BALANCED') if complete and total>=1e6 else 'UNAVAILABLE'
            result = dict(at=now,health=health,flow=flow,book_age_seconds=book_age,
                          trade_age_seconds=trade_age,flow_coverage_complete=bool(complete),
                          buy_60s_usdt=round(buy,2),sell_60s_usdt=round(sell,2),
                          delta_60s_usdt=round(buy-sell,2),imbalance_60s=imbalance,
                          trades_60s=len(recent),depth=self.depth,
                          best_bid=max(self.bids) if self.bids and fresh else None,
                          best_ask=min(self.asks) if self.asks and fresh else None,
                          visible_bid_low=min(self.bids) if self.bids and fresh else None,
                          visible_ask_high=max(self.asks) if self.asks and fresh else None,
                          bid_zone=None,ask_zone=None,last_error=self.last_error)
            if not fresh:
                self.walls.clear()
                return result
            for side,book in (('bid_zone',self.bids),('ask_zone',self.asks)):
                groups = {}
                for price,size in book.items():
                    bucket = math.floor(price/25)*25
                    groups[bucket] = groups.get(bucket,0)+price*size
                candidate = None
                if len(groups)>=5:
                    mid = statistics.median(groups.values())
                    price,volume = max(groups.items(),key=lambda x:x[1])
                    if volume>=1e6 and volume>=3*mid:
                        candidate = (price,volume)
                if candidate is None:
                    self.walls.pop(side,None)
                    continue
                previous = self.walls.get(side)
                if not previous or previous[0]!=candidate[0]:
                    self.walls[side] = (candidate[0],now)
                    continue
                age = now-previous[1]
                if age>=30:
                    result[side] = dict(low=candidate[0],high=candidate[0]+25,
                                        notional=round(candidate[1],2),persistent_seconds=round(age,1))
            return result

    def run(self):
        import websocket
        backoff = 2
        while not self.stop_event.is_set():
            ws = None
            try:
                self.reset()
                ws = websocket.create_connection(WS,timeout=10)
                with self.lock:
                    self.connected = True
                    self.last_error = None
                ws.send(json.dumps(dict(op='subscribe',args=['orderbook.'+str(self.depth)+'.BTCUSDT',
                                     'publicTrade.BTCUSDT','kline.3.BTCUSDT','kline.5.BTCUSDT'])))
                ping_at = time.monotonic()
                while not self.stop_event.is_set():
                    if time.monotonic()-ping_at>=20:
                        ws.send(json.dumps({'op':'ping'}))
                        ping_at = time.monotonic()
                    ws.settimeout(1)
                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue
                    if not raw:
                        raise RuntimeError('WEBSOCKET_CLOSED')
                    msg = json.loads(raw)
                    if msg.get('op')=='subscribe' and not msg.get('success'):
                        raise RuntimeError('SUBSCRIPTION_REJECTED')
                    self.process(msg,time.time())
                    if time.time()-self.session_start>60:
                        backoff = 2
            except Exception as exc:
                with self.lock:
                    self.last_error = type(exc).__name__
            finally:
                with self.lock:
                    self.connected = False
                    self.book_ready = False
                    self.walls.clear()
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass
            if self.stop_event.wait(backoff):
                break
            backoff = min(60,backoff*2)


class StableChanges:
    def __init__(self):
        self.published = {}
        self.pending = {}

    def step(self, context, now):
        health = 'DEGRADED' if context['health']=='WARMING_UP' else context['health']
        values = {'FEED_CHANGED':health,'FLOW_CHANGED':context['flow'],
                  'ZONES_CHANGED':tuple((side,context[side]['low'] if context.get(side) else None)
                                       for side in ('bid_zone','ask_zone'))}
        events = []
        for kind,value in values.items():
            if kind not in self.published:
                self.published[kind] = value
                continue
            if self.published[kind]==value:
                self.pending.pop(kind,None)
                continue
            if kind not in self.pending or self.pending[kind][0]!=value:
                self.pending[kind] = (value,now)
            if kind=='FEED_CHANGED':
                # REST may publish a just-closed candle shortly after the boundary.
                # A transient candle delay must not produce a failure/recovery pair.
                delay = 60 if value in ('CANDLES_DEGRADED','DEGRADED') else 10 if value=='OK' else 0
            else:
                delay = 30
            if now-self.pending[kind][1]>=delay:
                events.append({'kind':kind,'at':int(now),'from':str(self.published[kind]),'to':str(value)})
                self.published[kind] = value
                self.pending.pop(kind,None)
        return events


def acquire_lock(directory):
    import fcntl
    directory = Path(directory)
    directory.mkdir(parents=True,exist_ok=True)
    lock = (directory/'v13_5.lock').open('a')
    try:
        fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    except OSError:
        lock.close()
        raise RuntimeError('ANOTHER_MONITOR_USES_THIS_DATA_DIRECTORY')
    return lock


def public_history(engine, initial=False):
    cutoff = time.time()-2
    result = []
    for tf,seconds,base in (('5m',300,900),('3m',180,1500)):
        cached = engine.frames[tf].bars
        target = int(cutoff)//seconds*seconds-seconds
        if cached and cached[-1][0]==target and not initial:
            result.append(cached)
            continue
        count = base if initial else 6
        if cached:
            count = max(count, int((target-cached[-1][0])/seconds)+6)
        if count>10000:
            raise RuntimeError('GAP_EXCEEDS_RESEARCH_RECOVERY_LIMIT')
        incoming = fetch_bars(seconds,cutoff,count)
        overlap = {b[0]:b for b in cached}
        for b in incoming:
            old = overlap.get(b[0])
            if old and any(abs(float(x)-float(y))>1e-7 for x,y in zip(old,b)):
                raise RuntimeError('CLOSED_CANDLE_REVISION_REQUIRES_REVIEW')
        if cached and incoming[0][0]>cached[-1][0]+seconds:
            raise RuntimeError('UNRECOVERED_CANDLE_GAP')
        result.append(incoming)
    return result


def candles_current(engine, now):
    return all(machine.view()['candle_end'] is not None and
               machine.view()['candle_end']>=int((now-2)//FRAMES[tf])*FRAMES[tf]
               for tf,machine in engine.frames.items())


def finish_live_check(engine, feed):
    # The duration deadline can coincide with a just-closed candle. Reconcile before
    # deciding; retries never turn stale candles into a pass.
    error = None
    reconciled = 0
    for attempt in range(2):
        try:
            five,three = public_history(engine)
            trial = Engine(json.loads(json.dumps(engine.dump())))
            reconciled += len(trial.ingest(five,three))
            engine = trial
            error = None
        except Exception as exc:
            error = type(exc).__name__
        checked = time.time()
        if error is None and candles_current(engine,checked):
            break
    context = feed.features(checked)
    context['candles_fresh'] = candles_current(engine,checked)
    context['candle_error'] = error
    if error or not context['candles_fresh']:
        context['health'] = 'CANDLES_DEGRADED'
    passed = context['health']=='OK' and context['candles_fresh'] and all(x.ind is not None for x in engine.frames.values())
    return {'check_complete':True,'live_check_passed':passed,'checked_at_iran':stamp(checked),
            'final_reconciliation_attempts':attempt+1,'final_reconciled_events':reconciled,
            'frames':engine.view(),'live':context}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument('--once',action='store_true',help='Read-only closed-candle bootstrap; no DB/Telegram/WebSocket')
    mode.add_argument('--check',type=int,metavar='SECONDS',help='Temporary live check; no DB/Telegram')
    mode.add_argument('--watch',action='store_true',help='Persist independent state, audit and outbox')
    mode.add_argument('--replay',action='store_true',help='Offline candle-only replay')
    p.add_argument('--five',help='Offline JSON containing 5m candles')
    p.add_argument('--three',help='Offline JSON containing 3m candles')
    p.add_argument('--telegram',action='store_true')
    p.add_argument('--telegram-env',default='/opt/tmco_webhook_v4/telegram.env')
    p.add_argument('--data-dir',default='/opt/btc_v13_research/v13_5')
    p.add_argument('--depth',type=int,choices=(200,1000),default=1000)
    args = p.parse_args()
    if args.telegram and not args.watch:
        p.error('--telegram requires --watch')
    if args.check is not None and not 10<=args.check<=300:
        p.error('--check must be 10..300 seconds')
    if args.replay and (not args.five or not args.three):
        p.error('--replay requires --five and --three')
    if not args.replay and (args.five or args.three):
        p.error('--five/--three require --replay')
    engine = Engine()
    if args.replay:
        five = json.loads(Path(args.five).read_text(encoding='utf-8-sig'))['candles']
        three = json.loads(Path(args.three).read_text(encoding='utf-8-sig'))['candles']
        validate(five,300)
        validate(three,180)
        # Restrict 3m decisions to the jointly covered historical window.
        first = max(five[0][0],three[0][0])
        end = min(five[-1][0]+300,three[-1][0]+180)
        three = [b for b in three if first<=b[0] and b[0]+180<=end]
        five = [b for b in five if b[0]+300<=end]
        for event in engine.ingest(five,three):
            print(json.dumps(event,ensure_ascii=False))
        print(json.dumps({'final':engine.view(),'note':'CANDLE_ONLY_NO_HISTORICAL_ORDERFLOW'},ensure_ascii=False))
        return
    if args.once:
        five,three = public_history(engine,initial=True)
        engine.ingest(five,three)
        print(json.dumps({'version':VERSION,'frames':engine.view(),'flow':'UNAVAILABLE_IN_ONCE'},ensure_ascii=False,indent=2))
        return
    try:
        import websocket
        if not hasattr(websocket,'create_connection'):
            raise ImportError()
    except ImportError:
        p.error('Use the isolated venv_orderflow environment with websocket-client')
    token,chat = credentials(args.telegram_env) if args.telegram else (None,None)
    lock = acquire_lock(args.data_dir) if args.watch else None
    store = None
    stop = threading.Event()
    import signal
    signal.signal(signal.SIGTERM,lambda *_:stop.set())
    feed = LiveFeed(stop,args.depth)
    sender = None
    try:
        upgraded = False
        revalidated = []
        if args.watch:
            store = Store(args.data_dir)
            saved = store.load()
            if saved:
                upgraded = saved.get('version')!=VERSION
                if upgraded:
                    store.archive_before_upgrade(saved)
                engine = Engine(saved)
                if upgraded:
                    revalidated = engine.revalidate_ranges()
        feed.start()
        before = engine.last_decision
        five,three = public_history(engine,initial=True)
        initial_events = engine.ingest(five,three)
        if before and store:
            store.save(engine,initial_events,False,{'health':'HISTORICAL_NO_ORDERFLOW','flow':'UNAVAILABLE'})
        context = feed.features(time.time())
        startup = {'kind':'STARTED','at':int(time.time()),
                   'reason':'UPDATED_POLICY_REVALIDATED' if upgraded else 'RESTORED_STATE' if before else 'HISTORICAL_BOOTSTRAP',
                   'revalidated':revalidated}
        if store:
            store.save(engine,[startup],args.telegram,context)
            if args.telegram:
                sender = Sender(store.path,token,chat,stop)
                sender.start()
        print(json.dumps({'startup':startup,'frames':engine.view(),'live':context},ensure_ascii=False),flush=True)
        changes = StableChanges()
        started = time.monotonic()
        next_poll = next_sample = next_print = 0
        candle_error = None
        while not stop.is_set() and (args.check is None or time.monotonic()-started<args.check):
            now = time.time()
            events = []
            if time.monotonic()>=next_poll or feed.wake.is_set():
                feed.wake.clear()
                next_poll = time.monotonic()+10
                try:
                    previous = engine.last_decision
                    five,three = public_history(engine)
                    trial = Engine(json.loads(json.dumps(engine.dump())))
                    new_events = trial.ingest(five,three)
                    engine = trial
                    if previous and engine.last_decision-previous>360:
                        if store:
                            store.save(engine,new_events,False,{'health':'HISTORICAL_NO_ORDERFLOW','flow':'UNAVAILABLE'})
                        events.append({'kind':'CATCHUP','at':int(now),'reason':'HISTORICAL_EVENTS_SUPPRESSED_AFTER_GAP'})
                    else:
                        events += new_events
                    candle_error = None
                except Exception as exc:
                    candle_error = type(exc).__name__
                    print('CANDLE_RETRY '+candle_error,flush=True)
            context = feed.features(now)
            context['candle_error'] = candle_error
            context['candles_fresh'] = candles_current(engine,now)
            if candle_error or not context['candles_fresh']:
                context['health'] = 'CANDLES_DEGRADED'
            events += changes.step(context,now)
            if store:
                if events or time.monotonic()>=next_sample:
                    store.save(engine,events,args.telegram,context)
                    store.sample(context,now)
                    next_sample = time.monotonic()+10
            if events:
                print(json.dumps({'events':events,'frames':engine.view(),'live':context},ensure_ascii=False),flush=True)
            if time.monotonic()>=next_print:
                print(json.dumps({'heartbeat':stamp(now),'frames':engine.view(),'live':context},ensure_ascii=False),flush=True)
                next_print = time.monotonic()+30
            stop.wait(1)
        if args.check is not None:
            print(json.dumps(finish_live_check(engine,feed),ensure_ascii=False),flush=True)
    except KeyboardInterrupt:
        print('STOPPED_BY_OPERATOR',flush=True)
    finally:
        stop.set()
        if feed.ident is not None:
            feed.join(timeout=15)
        if sender:
            sender.join(timeout=15)
        if store:
            store.db.close()
        if lock:
            lock.close()


if __name__=='__main__':
    main()
