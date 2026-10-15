#!/usr/bin/env python3
# coding: utf-8
"""BTC V13.2 - independent BTCUSDT market regime monitor.

Three independent closed-candle classifications: 5m, 15m, 1h.
Statuses: UP, DOWN, RANGE, UNCERTAIN. No trade or signal interaction.

--once: one read-only snapshot; no DB writes, no Telegram.
--watch: persist state/events ONLY to a dedicated V13.2 SQLite file.
--telegram: explicitly opt in with --watch to send status-change messages.

Depends on existing read-only V8 Bybit candles module on the server.
Experimental classifications: not validated for blocking live trades.
"""
import argparse
import datetime as dt
import importlib.util
import json
import math
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

V8_DEFAULT = '/opt/btc_v12_shadow/btc_v4_structure_context_v8_200_244.py'
CORE_DEFAULT = '/opt/btc_v13_research/BTC_V13_Range_Detection.py'
STATE_DEFAULT = '/opt/btc_v13_research/v13_2_market_state.db'
TELEGRAM_ENV_DEFAULT = '/opt/tmco_webhook_v4/telegram.env'
COUNT_5M = 900  # 75h: 900 x 5m; 300 x 15m; 75 x 1h if aligned
STATUSES = ('UP', 'DOWN', 'RANGE', 'UNCERTAIN')
PERIODS = {'5m': 300, '15m': 900, '1h': 3600}


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError('Cannot load module: ' + str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def aggregate(bars, seconds):
    """Only completely closed and contiguous aligned higher-TF candles."""
    n = seconds // 300
    if n < 1 or n * 300 != seconds:
        raise ValueError('Bad timeframe')
    out = []
    groups = {}
    for bar in bars:
        start = (int(bar[0]) // seconds) * seconds
        groups.setdefault(start, []).append(bar)
    for start in sorted(groups):
        group = groups[start]
        if len(group) == n and all(int(b[0]) == start + i * 300 for i, b in enumerate(group)):
            out.append((start, group[0][1], max(b[2] for b in group),
                        min(b[3] for b in group), group[-1][4]))
    return out


def signed_ema_slope(bars, core, n=20, distance=5):
    closes = [b[4] for b in bars]
    alpha = 2 / (n + 1)
    ema = closes[0]
    values = []
    for close in closes:
        ema = alpha * close + (1 - alpha) * ema
        values.append(ema)
    atr = sum(core.true_ranges(bars)[-14:]) / 14
    if atr <= 0:
        raise ValueError('Zero ATR')
    return (values[-1] - values[-1 - distance]) / atr, ema, atr


def classify(data, timeframe, core):
    """No other timeframe passed in. Trend and range assessed independently."""
    if len(data) < 65:
        raise ValueError(f'Not enough {timeframe} bars: {len(data)}')
    adx = core.adx(data)
    chop = core.chop(data)
    er = core.efficiency(data)
    slope, ema, atr = signed_ema_slope(data, core)
    window = {'5m': 72, '15m': 40, '1h': 32}[timeframe]
    struct = core.structure(data, window=window)
    if any(x is None or not math.isfinite(x) for x in (adx, chop, er, slope, ema, atr)):
        raise ValueError('Invalid indicator')
    price = data[-1][4]
    short_move = price - data[-6][4]
    horizontal = bool(struct['horizontal'])
    support, resistance = struct['support'], struct['resistance']
    inside = (support is not None and resistance is not None and
              support - 0.15 * atr <= price <= resistance + 0.15 * atr)

    # Initial research hypotheses, intentionally not treated as a validated trade filter.
    sideways = (horizontal and inside and
                ((adx < 20 and er < 0.25) or
                 (chop >= 61.8 and er < 0.35) or
                 (er < 0.16 and abs(slope) < 0.35)))
    if sideways:
        status = 'RANGE'
    elif adx >= 20 and slope >= 0.25 and price > ema and short_move > 0:
        status = 'UP'
    elif adx >= 20 and slope <= -0.25 and price < ema and short_move < 0:
        status = 'DOWN'
    else:
        status = 'UNCERTAIN'

    return {
        'status': status,
        'last_closed_open_ts': int(data[-1][0]),
        'last_closed_end_utc': dt.datetime.fromtimestamp(int(data[-1][0]) + PERIODS[timeframe],
                                                        dt.timezone.utc).isoformat(),
        'close': round(price, 2),
        'adx14': round(adx, 3), 'chop14': round(chop, 3),
        'er20': round(er, 3), 'ema20_slope5_atr': round(slope, 3),
        'atr14': round(atr, 2), 'horizontal_structure': horizontal,
        'support': round(support, 2) if support is not None else None,
        'resistance': round(resistance, 2) if resistance is not None else None,
        'bars_used': len(data),
    }


def snapshot(epoch, v8, core):
    bars = v8.candles(float(epoch), COUNT_5M)
    if len(bars) != COUNT_5M:
        raise RuntimeError('Incomplete source candles')
    if any(int(bars[i][0]) - int(bars[i-1][0]) != 300 for i in range(1, len(bars))):
        raise RuntimeError('Noncontiguous source candles')
    expected = (int(epoch) // 300) * 300 - 300
    if int(bars[-1][0]) != expected:
        raise RuntimeError(f'Stale/latest candle mismatch: {bars[-1][0]} vs {expected}')
    frames = {
        '5m': bars,
        '15m': aggregate(bars, 900),
        '1h': aggregate(bars, 3600),
    }
    result = {tf: classify(frames[tf], tf, core) for tf in PERIODS}
    return {'symbol': 'BTCUSDT', 'closed_5m_open_ts': expected,
            'checked_closed_5m_utc': dt.datetime.fromtimestamp(expected + 300,
                                                              dt.timezone.utc).isoformat(),
            'timeframes': result, 'mode': 'V13_2_INDEPENDENT_RESEARCH'}


def open_state(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=20)
    db.execute('PRAGMA busy_timeout=20000')
    db.execute('CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), '
               'last_candle_ts INTEGER NOT NULL, statuses TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS events ('
               'id INTEGER PRIMARY KEY AUTOINCREMENT, candle_ts INTEGER NOT NULL, '
               'kind TEXT NOT NULL, payload TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0, '
               'attempts INTEGER NOT NULL DEFAULT 0, telegram_message_id TEXT, '
               'UNIQUE(candle_ts,kind))')
    db.commit()
    return db


def record(db, snap, telegram_enabled=False):
    """Atomic update and outbox, deduplicated by closed 5m candle."""
    candle = snap['closed_5m_open_ts']
    row = db.execute('SELECT last_candle_ts,statuses FROM state WHERE id=1').fetchone()
    if row and candle <= row[0]:
        return []
    previous = json.loads(row[1]) if row else {}
    current = {tf: snap['timeframes'][tf]['status'] for tf in PERIODS}
    changed = [tf for tf in PERIODS if previous.get(tf) != current[tf]]
    with db:
        db.execute('INSERT INTO state(id,last_candle_ts,statuses) VALUES(1,?,?) '
                   'ON CONFLICT(id) DO UPDATE SET last_candle_ts=excluded.last_candle_ts, '
                   'statuses=excluded.statuses',
                   (candle, json.dumps(current)))
        if changed:
            kind = 'MARKET_STATUS_CHANGED'
            payload = dict(snap, changed_timeframes=changed, previous_statuses=previous)
            db.execute('INSERT OR IGNORE INTO events(candle_ts,kind,payload,delivered) '
                       'VALUES(?,?,?,?)',
                       (candle, kind, json.dumps(payload, ensure_ascii=False),
                        0 if telegram_enabled else 1))
    return changed


def credentials(path):
    variables = {}
    with open(path, encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, value = line.split('=', 1)
            variables[key.strip().removeprefix('export ')] = value.strip().strip('"').strip("'")
    token, chat = variables.get('BOT_TOKEN'), variables.get('CHAT_ID')
    if not token or not chat:
        raise ValueError('Missing BOT_TOKEN or CHAT_ID')
    return token, chat


def telegram_text(payload):
    words = {'UP': '🟢 صعودی', 'DOWN': '🔴 نزولی',
             'RANGE': '🟠 رنج', 'UNCERTAIN': '⚪ نامشخص'}
    lines = ['📊 BTCUSDT | تغییر وضعیت بازار',
             'زمان آخرین کندل ۵ دقیقه: ' + payload['checked_closed_5m_utc'][:16].replace('T',' ') + ' UTC',
             'تایم‌فریم تغییرکرده: ' + '، '.join(payload['changed_timeframes']), '']
    for tf in PERIODS:
        row = payload['timeframes'][tf]
        mark = ' ↻' if tf in payload['changed_timeframes'] else ''
        lines.append(f"{tf}: {words[row['status']]}{mark} | قیمت: {row['close']:,.2f}")
        if row['status'] == 'RANGE' and row['support'] is not None:
            lines.append(f"  حمایت: {row['support']:,.2f} | مقاومت: {row['resistance']:,.2f}")
    lines.extend(['', '⚠️ V13.2 آزمایشی؛ هیچ معامله‌ای مسدود یا تغییر نمی‌کند.'])
    return '\n'.join(lines)


def send_pending(db, env_path, max_per_pass=4):
    rows = db.execute('SELECT id,payload FROM events WHERE delivered=0 '
                      'ORDER BY id LIMIT ?', (max_per_pass,)).fetchall()
    if not rows:
        return
    token, chat = credentials(env_path)
    url = 'https://api.telegram.org/bot' + token + '/sendMessage'
    for eid, raw in rows:
        try:
            payload = json.loads(raw)
            data = urllib.parse.urlencode({'chat_id':chat, 'text':telegram_text(payload)}).encode()
            req = urllib.request.Request(url, data=data,
                                         headers={'User-Agent':'BTC-V13.2-market/1.0'})
            with urllib.request.urlopen(req, timeout=12) as resp:
                answer = json.load(resp)
            if not answer.get('ok'):
                raise RuntimeError('Telegram rejected: ' + str(answer.get('description')))
            msgid = str(answer.get('result', {}).get('message_id', ''))
            with db:
                db.execute('UPDATE events SET delivered=1,attempts=attempts+1, '
                           'telegram_message_id=? WHERE id=?', (msgid, eid))
            print('TELEGRAM_SENT event=' + str(eid) + ' id=' + msgid, flush=True)
        except Exception as exc:
            with db:
                db.execute('UPDATE events SET attempts=attempts+1 WHERE id=?', (eid,))
            print('TELEGRAM_RETRY_PENDING', eid, type(exc).__name__, file=sys.stderr, flush=True)
            break


def main():
    p = argparse.ArgumentParser(description='BTC independent 5m/15m/1h market monitor')
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument('--once', action='store_true', help='Read-only market snapshot')
    mode.add_argument('--watch', action='store_true', help='Watch closed 5m candles; own DB only')
    p.add_argument('--epoch', type=float, help='Historical snapshot (only with --once)')
    p.add_argument('--telegram', action='store_true', help='Opt-in with --watch only')
    p.add_argument('--poll-seconds', type=int, default=30)
    p.add_argument('--db', default=STATE_DEFAULT)
    p.add_argument('--v8', default=V8_DEFAULT)
    p.add_argument('--core', default=CORE_DEFAULT)
    p.add_argument('--telegram-env', default=TELEGRAM_ENV_DEFAULT)
    args = p.parse_args()
    if args.telegram and not args.watch:
        p.error('--telegram requires --watch')
    if args.epoch is not None and not args.once:
        p.error('--epoch requires --once')
    if not 10 <= args.poll_seconds <= 300:
        p.error('poll-seconds must be 10..300')
    v8 = load_module(args.v8, 'v13_2_existing_v8')
    core = load_module(args.core, 'v13_2_existing_core')
    if args.once:
        result = snapshot(args.epoch if args.epoch is not None else time.time()-10, v8, core)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    db = open_state(args.db)
    print('V13.2 STARTED; telegram=' + str(args.telegram), flush=True)
    try:
        while True:
            if args.telegram:
                try:
                    send_pending(db, args.telegram_env)
                except Exception as exc:
                    print('TELEGRAM_SETUP_ERROR:', type(exc).__name__, file=sys.stderr, flush=True)
            target = (int((time.time()-10)//300)*300)-300
            row = db.execute('SELECT last_candle_ts FROM state WHERE id=1').fetchone()
            if not row or target > row[0]:
                try:
                    snap = snapshot(time.time()-10, v8, core)
                    changed = record(db, snap, telegram_enabled=args.telegram)
                    print(json.dumps({'candle_ts':snap['closed_5m_open_ts'],
                                      'statuses':{tf:snap['timeframes'][tf]['status'] for tf in PERIODS},
                                      'changed':changed}, ensure_ascii=False), flush=True)
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
