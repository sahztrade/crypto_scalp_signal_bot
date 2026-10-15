#!/usr/bin/env python3
"""BTC V13.1 independent, experimental BTCUSDT market range monitor.

Reads ONLY closed Bybit linear 5-minute BTCUSDT candles through the existing V8
fetcher. Aggregates aligned, complete 15m and 1h candles, uses standalone V13
technical indicators, and never consumes/changes any TMCO trade or signal.

Safety: --once is completely read-only. --watch writes ONLY an independent
SQLite DB under /opt/btc_v13_research. --telegram is opt-in; it reads existing
Telegram credentials without changing them. Does NOT block trades.

Heuristics/thresholds are exploratory, NOT validated trading rules. Alerts may
lag the actual beginning of a range; 'range detected' means an indicator-based
classification at the current closed candle, not exact onset.
"""
import argparse
import datetime as dt
import importlib.util
import json
import math
import os
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

V8_DEFAULT = '/opt/btc_v12_shadow/btc_v4_structure_context_v8_200_244.py'
V13_DEFAULT = '/opt/btc_v13_research/BTC_V13_Range_Detection.py'
STATE_DEFAULT = '/opt/btc_v13_research/v13_market_monitor.db'
TELEGRAM_ENV_DEFAULT = '/opt/tmco_webhook_v4/telegram.env'
COUNT_5M = 480  # 40h of BTC candles: up to 160 completed 15m and 40 1h bars.


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError('Cannot load ' + str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def aggregate(bars, seconds):
    """Use strictly complete, contiguous groups; bars are (open_ts,O,H,L,C)."""
    if seconds % 300:
        raise ValueError('Aggregate timeframe must divide into 5m bars')
    n = seconds // 300
    groups = {}
    for bar in bars:
        groups.setdefault((int(bar[0]) // seconds) * seconds, []).append(bar)
    out = []
    for start in sorted(groups):
        grp = groups[start]
        if len(grp) != n or any(int(b[0]) != start + i * 300 for i, b in enumerate(grp)):
            continue
        out.append((start, grp[0][1], max(b[2] for b in grp),
                    min(b[3] for b in grp), grp[-1][4]))
    return out


def safe_float(x, digits=3):
    if x is None or not math.isfinite(x):
        return None
    return round(x, digits)


def timeframe_metrics(bars, v13, struct_window, name):
    if len(bars) < 120:
        raise ValueError('%s requires >=120 completed candles (got %d)' % (name, len(bars)))
    adx = v13.adx(bars)
    chop = v13.chop(bars)
    er = v13.efficiency(bars)
    width, width_pct = v13.bb_width_percentile(bars)
    slope = v13.ema_slope_atr(bars)
    st = v13.structure(bars, window=struct_window)
    tr = v13.true_ranges(bars)
    atr = sum(tr[-14:]) / 14
    vals = (adx, chop, er, width, width_pct, slope, atr)
    if any(x is None or not math.isfinite(x) for x in vals) or atr <= 0:
        raise ValueError('Invalid %s indicator values' % name)
    flags = {
        'adx_low': adx < 20,
        'chop_high': chop > 61.8,
        'er_low': er < .25,
        'bb_compressed': width_pct <= 20,
        'horizontal_structure': bool(st['horizontal']),
        'ema_flat': slope < .35,
    }
    weights = {'adx_low': 1, 'chop_high': 2, 'er_low': 2,
               'bb_compressed': 1, 'horizontal_structure': 2, 'ema_flat': 1}
    score = sum(weights[key] for key, enabled in flags.items() if enabled)
    support, resistance = st['support'], st['resistance']
    close = bars[-1][4]
    inside = (support is not None and resistance is not None
              and support - .15 * atr <= close <= resistance + .15 * atr)
    # Conservative range confirmation requires a price-defined horizontal box;
    # complementary indicators must also support sidewaysness. Threshold 5 is
    # a starting hypothesis to be validated, NOT a fitted optimal threshold.
    range_flag = bool(flags['horizontal_structure'] and inside and score >= 5
                      and (flags['chop_high'] or flags['er_low'] or flags['adx_low']))
    return {
        'timeframe': name, 'last_closed_open_ts': int(bars[-1][0]),
        'close': round(close, 2), 'atr14': round(atr, 2),
        'adx14': safe_float(adx), 'chop14': safe_float(chop),
        'er20': safe_float(er), 'bb_width_percentile100': safe_float(width_pct),
        'ema20_slope5_atr': safe_float(slope), 'score': score,
        'flags': flags, 'range': range_flag,
        'support': round(support, 2) if support is not None else None,
        'resistance': round(resistance, 2) if resistance is not None else None,
        'structure': st,
    }


def hourly_context(bars, v13):
    if len(bars) < 30:
        return 'UNKNOWN'
    a = v13.adx(bars)
    close = [x[4] for x in bars]
    if a is None or not math.isfinite(a) or a < 20:
        return 'NEUTRAL'
    k = 2 / 21
    ema = close[0]
    series = []
    for c in close:
        ema = ema * (1 - k) + c * k
        series.append(ema)
    if series[-1] > series[-4] and close[-1] > series[-1]:
        return 'UP'
    if series[-1] < series[-4] and close[-1] < series[-1]:
        return 'DOWN'
    return 'NEUTRAL'


def get_snapshot(epoch, v8, v13):
    bars = v8.candles(float(epoch), COUNT_5M)
    if len(bars) != COUNT_5M:
        raise ValueError('Not enough 5m bars')
    for i in range(1, len(bars)):
        if int(bars[i][0]) - int(bars[i-1][0]) != 300:
            raise ValueError('Noncontiguous 5m candles')
    expected = (int(epoch) // 300) * 300 - 300
    if int(bars[-1][0]) != expected:
        raise ValueError('Stale or unfinished latest 5m candle: %s vs expected %s' %
                         (bars[-1][0], expected))
    b15 = aggregate(bars, 900)
    b1h = aggregate(bars, 3600)
    five = timeframe_metrics(bars, v13, 72, '5m')
    fifteen = timeframe_metrics(b15, v13, 32, '15m')
    context = hourly_context(b1h, v13)
    # A preliminary, transient display label; transition alerts are governed by
    # persistent state and breakout/re-entry checks in update_state().
    provisional = ('RANGE_CONFIRMED' if five['range'] and fifteen['range']
                   else 'LOCAL_RANGE' if five['range']
                   else 'RANGE_15M_ONLY' if fifteen['range']
                   else 'NO_RANGE_DETECTED')
    return {'symbol': 'BTCUSDT', 'closed_5m_ts': int(bars[-1][0]),
            'closed_5m_utc': dt.datetime.fromtimestamp(bars[-1][0], dt.timezone.utc).isoformat(),
            '5m': five, '15m': fifteen, '1h_context': context,
            'provisional': provisional, 'mode': 'INDEPENDENT_MARKET_MONITOR'}


def connect_state(db_path):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(db_path, timeout=20)
    db.execute('PRAGMA busy_timeout=20000')
    db.execute('CREATE TABLE IF NOT EXISTS monitor (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
    db.execute('''CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        candle_ts INTEGER NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL,
        delivered INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
        telegram_message_id TEXT,
        UNIQUE(candle_ts, kind))''')
    db.commit()
    return db


def state_read(db):
    row = db.execute("SELECT value FROM monitor WHERE key='state'").fetchone()
    return json.loads(row[0]) if row else {
        'last_candle_ts': 0, 'active': False, 'confirmed_15m': False,
        'support': None, 'resistance': None, 'breakout_side': None,
        'breakout_count': 0, 'last_mode': 'INITIAL',
    }


def update_state(old, snapshot):
    """Pure state transition. Handles 5m entry, 15m confirmation and 2 closes exit.

    Boundaries freeze at 5m range detection, so subsequent pivot updates do not
    move the breakout target. Only a confirmed price break ends a tracked range.
    """
    candle_ts = snapshot['closed_5m_ts']
    if candle_ts <= old['last_candle_ts']:
        return old, []
    new = dict(old)
    new['last_candle_ts'] = candle_ts
    five, fifteen = snapshot['5m'], snapshot['15m']
    events = []

    if not old['active']:
        if five['range']:
            new.update(active=True, confirmed_15m=False,
                       support=five['support'], resistance=five['resistance'],
                       breakout_side=None, breakout_count=0,
                       last_mode='LOCAL_RANGE')
            events.append(('RANGE_5M_DETECTED', snapshot))
            if fifteen['range']:
                new['confirmed_15m'] = True
                new['last_mode'] = 'RANGE_CONFIRMED'
                events.append(('RANGE_15M_CONFIRMED', snapshot))
        else:
            new['last_mode'] = 'NO_ACTIVE_RANGE'
        return new, events

    close, atr = five['close'], five['atr14']
    support, resistance = old['support'], old['resistance']
    if support is None or resistance is None or atr <= 0:
        raise ValueError('Invalid stored range boundaries or ATR')
    buffer = 0.2 * atr
    side = ('UP' if close > resistance + buffer
            else 'DOWN' if close < support - buffer else None)
    if side:
        new['breakout_count'] = old['breakout_count'] + 1 if old['breakout_side'] == side else 1
        new['breakout_side'] = side
        new['last_mode'] = 'BREAKOUT_WATCH'
        if new['breakout_count'] >= 2:
            payload = dict(snapshot, breakout_side=side,
                           tracked_support=support, tracked_resistance=resistance)
            events.append(('RANGE_EXIT_CONFIRMED', payload))
            new.update(active=False, confirmed_15m=False,
                       support=None, resistance=None,
                       breakout_side=None, breakout_count=0,
                       last_mode='RANGE_EXIT_CONFIRMED')
    else:
        new['breakout_side'] = None
        new['breakout_count'] = 0
        if fifteen['range'] and not old['confirmed_15m']:
            new['confirmed_15m'] = True
            events.append(('RANGE_15M_CONFIRMED', snapshot))
        new['last_mode'] = ('RANGE_CONFIRMED' if new['confirmed_15m']
                            else 'LOCAL_RANGE')
    return new, events


def record_snapshot(db, snapshot, alert_enabled=True):
    """Commit transition + outbox atomically, only inside V13 DB."""
    old = state_read(db)
    new, events = update_state(old, snapshot)
    if new['last_candle_ts'] == old['last_candle_ts']:
        return new, []
    with db:
        db.execute("INSERT INTO monitor(key,value) VALUES('state',?) "
                   "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                   (json.dumps(new, ensure_ascii=False),))
        for kind, payload in events:
            db.execute('INSERT OR IGNORE INTO events(candle_ts,kind,payload,delivered) VALUES(?,?,?,?)',
                       (snapshot['closed_5m_ts'], kind,
                        json.dumps(payload, ensure_ascii=False), 0 if alert_enabled else 1))
    return new, [kind for kind, _ in events]


def credentials_from_env(path):
    pairs = {}
    with open(path, encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, val = line.split('=', 1)
            key = key.strip().removeprefix('export ')
            pairs[key] = val.strip().strip('"').strip("'")
    token = pairs.get('BOT_TOKEN')
    chat = pairs.get('CHAT_ID')
    if not token or not chat:
        raise ValueError('BOT_TOKEN / CHAT_ID missing in Telegram env')
    return token, chat


def message(kind, payload):
    m5 = payload['5m']
    ts = payload['closed_5m_utc'][:16].replace('T', ' ') + ' UTC'
    if kind == 'RANGE_5M_DETECTED':
        title = '🟠 BTCUSDT | رنج در تایم‌فریم ۵ دقیقه شناسایی شد'
    elif kind == 'RANGE_15M_CONFIRMED':
        title = '🔴 BTCUSDT | رنج در تایم‌فریم ۱۵ دقیقه نیز تأیید شد'
    else:
        title = ('🟢 BTCUSDT | خروج صعودی از رنج تأیید شد'
                 if payload['breakout_side'] == 'UP' else
                 '🟢 BTCUSDT | خروج نزولی از رنج تأیید شد')
    support = payload.get('tracked_support', m5['support'])
    resistance = payload.get('tracked_resistance', m5['resistance'])
    return '\n'.join([
        title, f'زمان کندل: {ts}',
        f'قیمت بسته‌شدن: {m5["close"]:,.2f}',
        f'حمایت محدوده: {support:,.2f}' if support is not None else 'حمایت: نامشخص',
        f'مقاومت محدوده: {resistance:,.2f}' if resistance is not None else 'مقاومت: نامشخص',
        f'امتیاز رنج ۵m: {m5["score"]}/9 | ۱۵m: {payload["15m"]["score"]}/9',
        f'روند یک‌ساعته: {payload["1h_context"]}',
        '⚠️ آزمایشی | صرفاً هشدار؛ معاملات V3/V4 بدون تغییر.',
    ])


def send_pending(db, env_path, max_per_pass=4):
    rows = db.execute('SELECT id,kind,payload FROM events WHERE delivered=0 '
                      'ORDER BY id LIMIT ?', (max_per_pass,)).fetchall()
    if not rows:
        return
    token, chat = credentials_from_env(env_path)
    url = 'https://api.telegram.org/bot' + token + '/sendMessage'
    for eid, kind, payload in rows:
        params = urllib.parse.urlencode({'chat_id': chat,
                                         'text': message(kind, json.loads(payload))}).encode()
        try:
            req = urllib.request.Request(url, data=params,
                                         headers={'User-Agent': 'BTC-V13-market-monitor/1.1'})
            with urllib.request.urlopen(req, timeout=12) as resp:
                answer = json.load(resp)
            if not answer.get('ok'):
                raise RuntimeError('Telegram rejected message: ' + str(answer.get('description')))
            msgid = str(answer.get('result', {}).get('message_id', ''))
            with db:
                db.execute('UPDATE events SET delivered=1,attempts=attempts+1, '
                           'telegram_message_id=? WHERE id=?', (msgid, eid))
            print('TELEGRAM_SENT', eid, kind, 'id=' + msgid, flush=True)
        except Exception as exc:
            with db:
                db.execute('UPDATE events SET attempts=attempts+1 WHERE id=?', (eid,))
            print('TELEGRAM_RETRY_PENDING', eid, type(exc).__name__,
                  file=sys.stderr, flush=True)
            break  # preserve event order and avoid hammering the API


def main():
    p = argparse.ArgumentParser(description='Independent BTC range monitor: 5m + 15m + 1h context')
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument('--once', action='store_true', help='snapshot, no files written or messages sent')
    mode.add_argument('--watch', action='store_true', help='watch closed 5m candles and persist state')
    p.add_argument('--telegram', action='store_true', help='ONLY with --watch: send change alerts')
    p.add_argument('--epoch', type=float, help='--once only: historical decision epoch for snapshot')
    p.add_argument('--db', default=STATE_DEFAULT, help='independent V13 state/output DB')
    p.add_argument('--v8', default=V8_DEFAULT)
    p.add_argument('--v13', default=V13_DEFAULT)
    p.add_argument('--telegram-env', default=TELEGRAM_ENV_DEFAULT)
    p.add_argument('--poll-seconds', type=int, default=30)
    args = p.parse_args()
    if args.telegram and not args.watch:
        p.error('--telegram requires --watch')
    if args.epoch is not None and not args.once:
        p.error('--epoch requires --once')
    if not 10 <= args.poll_seconds <= 300:
        p.error('--poll-seconds must be between 10 and 300')
    v8 = load_module(args.v8, 'v13_existing_v8')
    v13 = load_module(args.v13, 'v13_existing_classifier')
    if args.once:
        snapshot = get_snapshot(args.epoch if args.epoch is not None else time.time() - 10, v8, v13)
        print(json.dumps(snapshot, ensure_ascii=False, indent=2))
        return
    # Only our private research DB is written; no live service is altered.
    db = connect_state(args.db)
    print('STARTED V13 independent monitor; telegram=' + str(args.telegram), flush=True)
    try:
        while True:
            if args.telegram:
                try:
                    send_pending(db, args.telegram_env)
                except Exception as exc:
                    print('TELEGRAM_SETUP_OR_SEND_ERROR:', repr(exc), file=sys.stderr, flush=True)
            expected = (int((time.time() - 10) // 300) * 300) - 300
            last = state_read(db)['last_candle_ts']
            if expected > last:
                try:
                    snapshot = get_snapshot(time.time() - 10, v8, v13)
                    current, events = record_snapshot(db, snapshot, alert_enabled=args.telegram)
                    print(json.dumps({'candle_ts': snapshot['closed_5m_ts'],
                                      '5m_score': snapshot['5m']['score'],
                                      '15m_score': snapshot['15m']['score'],
                                      '5m_range': snapshot['5m']['range'],
                                      '15m_range': snapshot['15m']['range'],
                                      'mode': current['last_mode'], 'events': events},
                                     ensure_ascii=False), flush=True)
                    if args.telegram:
                        send_pending(db, args.telegram_env)
                except Exception as exc:
                    print('MONITOR_RETRY_NEXT_POLL:', repr(exc), file=sys.stderr, flush=True)
            time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        print('STOPPED by operator', flush=True)
    finally:
        db.close()


if __name__ == '__main__':
    main()
