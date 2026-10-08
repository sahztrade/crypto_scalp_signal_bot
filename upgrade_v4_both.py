#!/usr/bin/env python3
"""V4 paper-only LONG+SHORT upgrade. Run on server in /opt/tmco_webhook_v4."""
import ast
import os
import py_compile
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

BASE = Path('/opt/tmco_webhook_v4')
SERVICE = 'tmco-v4-paper.service'
FILES = ('v4_trade_registry.py','v4_trade_monitor.py','v4_paper_evaluator.py','v4_signal_classifier.py')

CLASSIFIER = '''def classify_trade(direction, previous_trade, new_tp2):
    if direction not in ("LONG", "SHORT"):
        raise ValueError("Unsupported direction")
    if previous_trade is None:
        return {"classification": "new", "title": f"سیگنال جدید — BTCUSDT {direction}", "reference_signal_id": None, "tp2_delta": None}
    previous_id = previous_trade["signal_id"]
    delta = round(float(new_tp2) - float(previous_trade["tp2"]), 2)
    status = previous_trade["trade_status"]
    if status in ("sl", "tp2"):
        classification, title = "new", f"سیگنال جدید — BTCUSDT {direction}"
    elif status == "unknown" or previous_trade.get("entry_minute_unverified", 0):
        classification, title = "unknown_previous", f"وضعیت معامله قبلی نامشخص — BTCUSDT {direction}"
    elif status == "active" and ((direction == "LONG" and delta > 0) or (direction == "SHORT" and delta < 0)):
        classification, title = "better_tp2", f"TP2 بهتر — BTCUSDT {direction}"
    else:
        classification, title = "reentry_review", f"فرصت مجدد — بررسی ورود — BTCUSDT {direction}"
    return {"classification": classification, "title": title, "reference_signal_id": previous_id, "tp2_delta": delta}


def classify_short(previous_trade, new_tp2):
    return classify_trade("SHORT", previous_trade, new_tp2)


def classify_long(previous_trade, new_tp2):
    return classify_trade("LONG", previous_trade, new_tp2)
'''

REGISTRY = '''import sqlite3
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from v4_signal_classifier import classify_trade
from v4_trade_monitor import market_entry_state


def register_trade(db_path, direction, epoch, entry, sl, tp1, tp2, score):
    if direction not in ("LONG", "SHORT"):
        raise ValueError("Invalid direction")
    entry, sl, tp1, tp2 = map(float, (entry, sl, tp1, tp2))
    if not ((sl < entry < tp1 < tp2) if direction == "LONG" else (tp2 < tp1 < entry < sl)):
        raise ValueError("Invalid stop/target order")
    now = time.time()
    with sqlite3.connect(db_path, timeout=10) as db:
        db.execute("BEGIN IMMEDIATE")
        confirmation = db.execute("SELECT status,direction FROM paper_confirmations WHERE epoch=?", (epoch,)).fetchone()
        if confirmation != ("pending", direction):
            return {"status": "skipped"}
        existing = db.execute("SELECT id FROM paper_signal_outbox WHERE epoch=?", (epoch,)).fetchone()
        existing_trade = db.execute("SELECT signal_id FROM paper_trades_v4 WHERE epoch=?", (epoch,)).fetchone()
        if existing or existing_trade:
            if existing and existing_trade:
                db.execute("UPDATE paper_confirmations SET status='queued',error=NULL,attempts=attempts+1 WHERE epoch=? AND status='pending'", (epoch,))
            return {"status": "duplicate", "signal_id": existing_trade[0] if existing_trade else None}
        previous = db.execute("SELECT signal_id,tp2,trade_status,entry_minute_unverified FROM paper_trades_v4 WHERE direction=? ORDER BY signal_id DESC LIMIT 1", (direction,)).fetchone()
        previous_trade = dict(zip(("signal_id", "tp2", "trade_status", "entry_minute_unverified"), previous)) if previous else None
        classification = classify_trade(direction, previous_trade, tp2)
        max_id = db.execute("SELECT COALESCE(MAX(signal_id),0) FROM paper_trades_v4").fetchone()[0]
        row = db.execute("SELECT value FROM paper_metadata WHERE key='next_short_signal_id'").fetchone()
        signal_id = max(int(max_id) + 1, int(row[0]) if row else 1)
        state = market_entry_state(now)
        utc = datetime.fromtimestamp(now, timezone.utc)
        iran = utc.astimezone(ZoneInfo("Asia/Tehran"))
        message = (f"{classification['title']}\\n"
                   f"سیگنال شماره: {signal_id}\\n"
                   f"نماد: BTCUSDT — Bybit Futures\\n"
                   f"جهت: {direction}\\n"
                   f"Entry (Market): {entry:.2f}\\n"
                   f"SL: {sl:.2f}\\nTP1: {tp1:.2f}\\nTP2: {tp2:.2f}\\n"
                   f"امتیاز فیلتر: {score}/6\\n"
                   f"زمان UTC: {utc:%Y-%m-%d %H:%M:%S}\\n"
                   f"زمان ایران: {iran:%Y-%m-%d %H:%M:%S}\\n")
        if classification["reference_signal_id"] is not None:
            message += (f"سیگنال مرجع: {classification['reference_signal_id']}\\n"
                        f"اختلاف TP2: {classification['tp2_delta']:+.2f} USDT\\n")
        message += "معامله فرضی — بدون سفارش واقعی"
        db.execute("""INSERT INTO paper_trades_v4
            (signal_id,epoch,direction,entry,sl,tp1,tp2,decision_ts,reference_signal_id,
             classification,entry_status,trade_status,entry_filled_at,monitor_start_ts,entry_minute_unverified)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
            (signal_id,epoch,direction,entry,sl,tp1,tp2,now,classification["reference_signal_id"],
             classification["classification"],state["entry_status"],state["trade_status"],
             state["entry_filled_at"],state["monitor_start_ts"]))
        db.execute("INSERT INTO paper_signal_outbox(epoch,direction,message,status) VALUES (?,?,?,'pending')", (epoch,direction,message))
        db.execute("INSERT INTO paper_metadata(key,value) VALUES ('next_short_signal_id',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(signal_id+1),))
        db.execute("INSERT INTO paper_metadata(key,value) VALUES ('last_announced_direction',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (direction,))
        db.execute("UPDATE paper_confirmations SET status='queued',error=NULL,attempts=attempts+1 WHERE epoch=? AND status='pending'", (epoch,))
        return {"status":"queued", "signal_id":signal_id, "classification":classification["classification"]}


def register_short(db_path, epoch, entry, sl, tp1, tp2, score):
    return register_trade(db_path,"SHORT",epoch,entry,sl,tp1,tp2,score)


def register_long(db_path, epoch, entry, sl, tp1, tp2, score):
    return register_trade(db_path,"LONG",epoch,entry,sl,tp1,tp2,score)
'''


def build_monitor(original):
    old = '''def check_candle(c, entry, sl, tp2, entry_status):'''
    assert original.count(old) == 1
    original = original.replace(old, '''def check_candle(c, entry, sl, tp2, entry_status, direction="SHORT"):
    if direction not in ("SHORT", "LONG"):
        raise ValueError("Invalid direction")''')
    assert original.count('if high >= sl or low <= tp2:') == 1
    original = original.replace('if high >= sl or low <= tp2:', 'if (high >= sl or low <= tp2) if direction == "SHORT" else (low <= sl or high >= tp2):')
    assert original.count('hit_sl = high >= sl') == 1
    original = original.replace('hit_sl = high >= sl\n        hit_tp2 = low <= tp2', '''hit_sl = high >= sl if direction == "SHORT" else low <= sl
        hit_tp2 = low <= tp2 if direction == "SHORT" else high >= tp2''')
    original = original.replace('"""Initial paper-trade state for a MARKET SHORT entry."""', '"""Initial paper-trade state for a MARKET paper entry."""')
    assert original.count('SELECT signal_id, entry, sl, tp2,') == 1
    original = original.replace('SELECT signal_id, entry, sl, tp2,', 'SELECT signal_id, direction, entry, sl, tp2,')
    assert original.count('signal_id, entry, sl, tp2,\n        entry_status') == 1
    original = original.replace('signal_id, entry, sl, tp2,\n        entry_status', 'signal_id, direction, entry, sl, tp2,\n        entry_status')
    assert original.count('c, entry, sl, tp2, new_entry') == 1
    original = original.replace('c, entry, sl, tp2, new_entry', 'c, entry, sl, tp2, new_entry, direction')
    return original


def build_evaluator(original):
    assert original.count('from v4_long_alert import register_long_alert') == 1
    original = original.replace('from v4_long_alert import register_long_alert', 'from v4_trade_registry import register_long')
    start = original.index('    if status == "queued" and direction == "LONG":')
    end = original.index('    with sqlite3.connect(DB, timeout=10) as db:', start)
    # first with belongs to duplicate guard; instead locate final generic write block
    end = original.index('    with sqlite3.connect(DB, timeout=10) as db:\n        db.execute("BEGIN IMMEDIATE")', start)
    block = original[start:end]
    assert 'register_long_alert(' in block
    block = block.replace('register_long_alert(', 'register_long(')
    block = block.replace('            score=score\n', '            entry=current,\n            sl=stop,\n            tp1=tp1,\n            tp2=tp2,\n            score=score\n')
    assert 'entry=current' in block
    return original[:start] + block + original[end:]


def run(cmd, **kwargs):
    return subprocess.run(cmd, check=True, text=True, **kwargs)


def main():
    if os.geteuid() != 0 or not BASE.is_dir():
        raise RuntimeError('Run as root on the V4 server')
    if not (BASE/'data/v4_paper_live.db').exists():
        raise RuntimeError('V4 database missing')
    for name in FILES:
        if not (BASE/name).is_file():
            raise RuntimeError('Missing file: '+name)
    monitor = build_monitor((BASE/'v4_trade_monitor.py').read_text())
    evaluator = build_evaluator((BASE/'v4_paper_evaluator.py').read_text())
    outputs = {'v4_signal_classifier.py': CLASSIFIER, 'v4_trade_registry.py': REGISTRY,
               'v4_trade_monitor.py': monitor, 'v4_paper_evaluator.py': evaluator}
    for name, source in outputs.items():
        ast.parse(source, filename=name)
    with tempfile.TemporaryDirectory(prefix='v4-both-test-') as tmp:
        test = Path(tmp)
        for name, source in outputs.items():
            (test/name).write_text(source)
            py_compile.compile(str(test/name), doraise=True)
        # Test module imports from temporary folder, not the live service.
        sys.path.insert(0, str(test))
        sys.path.insert(1, str(BASE))
        import v4_signal_classifier as cls
        import v4_trade_monitor as mon
        assert cls.classify_trade('LONG',None,110)['classification'] == 'new'
        assert cls.classify_trade('LONG',{'signal_id':1,'tp2':110,'trade_status':'active','entry_minute_unverified':0},120)['classification'] == 'better_tp2'
        assert cls.classify_trade('SHORT',{'signal_id':1,'tp2':110,'trade_status':'active','entry_minute_unverified':0},100)['classification'] == 'better_tp2'
        candle = {'high':110,'low':90}
        assert mon.check_candle(candle,100,95,108,'filled','LONG')[1] == 'unknown'
        assert mon.check_candle({'high':109,'low':100},101,95,108,'filled','LONG')[1] == 'tp2'
        assert mon.check_candle({'high':101,'low':94},100,95,108,'filled','LONG')[1] == 'sl'
        assert mon.check_candle({'high':106,'low':95},100,105,90,'filled','SHORT')[1] == 'sl'
        assert mon.check_candle({'high':104,'low':89},100,105,90,'filled','SHORT')[1] == 'tp2'
        import v4_trade_registry as reg
        test_db = test/'test.db'
        with sqlite3.connect(BASE/'data/v4_paper_live.db', timeout=15) as src, sqlite3.connect(test_db) as dst:
            src.backup(dst)
        with sqlite3.connect(test_db) as db:
            epoch = max(
                db.execute('SELECT COALESCE(MAX(epoch),0) FROM paper_confirmations').fetchone()[0],
                db.execute('SELECT COALESCE(MAX(epoch),0) FROM paper_signal_outbox').fetchone()[0],
                db.execute('SELECT COALESCE(MAX(epoch),0) FROM paper_trades_v4').fetchone()[0],
            ) + 100
            cols = [r[1] for r in db.execute('PRAGMA table_info(paper_confirmations)')]
            # Clone a real confirmation to preserve any extra required schema columns.
            template = db.execute('SELECT * FROM paper_confirmations ORDER BY epoch DESC LIMIT 1').fetchone()
            if template is None:
                raise RuntimeError('No confirmation available for safe schema-compatible test')
            for offset, direction in ((0, 'LONG'), (1, 'SHORT')):
                values = dict(zip(cols, template))
                values.update(epoch=epoch+offset, direction=direction,
                              decision_close=int(time.time()),
                              received_at='2026-10-09T00:00:00+00:00',
                              status='pending', attempts=0)
                if 'error' in values:
                    values['error'] = None
                placeholders = ','.join('?' for _ in cols)
                db.execute('INSERT INTO paper_confirmations (' + ','.join(cols) +
                           ') VALUES (' + placeholders + ')', tuple(values[c] for c in cols))
        a = reg.register_long(test_db,epoch,100,95,107.5,112.5,5)
        b = reg.register_short(test_db,epoch+1,100,105,92.5,87.5,5)
        assert a['status'] == b['status'] == 'queued', (a,b)
        assert b['signal_id'] == a['signal_id']+1, (a,b)
        assert reg.register_long(test_db,epoch,100,95,107.5,112.5,5)['status'] == 'skipped'
        with sqlite3.connect(test_db) as db:
            assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
            assert db.execute("SELECT COUNT(*) FROM paper_trades_v4 WHERE epoch IN (?,?)",(epoch,epoch+1)).fetchone()[0] == 2
            assert db.execute("SELECT COUNT(*) FROM paper_signal_outbox WHERE epoch IN (?,?)",(epoch,epoch+1)).fetchone()[0] == 2
        print('PASS: compilation, LONG/SHORT monitoring, classification, numbering, test DB registration', flush=True)
    # All tests passed. Deploy while service stopped; automatic rollback on failure.
    backup = BASE/'backups'/('v4_both_'+time.strftime('%Y%m%d_%H%M%S'))
    backup.mkdir(parents=True, exist_ok=False)
    for name in FILES:
        shutil.copy2(BASE/name, backup/name)
    with sqlite3.connect(BASE/'data/v4_paper_live.db', timeout=15) as src:
        with sqlite3.connect(backup/'v4_paper_live_before_upgrade.db') as dst:
            src.backup(dst)
    was_active = subprocess.run(['systemctl','is-active','--quiet',SERVICE]).returncode == 0
    try:
        if was_active:
            run(['systemctl','stop',SERVICE])
        for name, source in outputs.items():
            temp_file = BASE/(name+'.v4new')
            temp_file.write_text(source)
            os.replace(temp_file,BASE/name)
        if was_active:
            run(['systemctl','start',SERVICE])
            time.sleep(3)
            run(['systemctl','is-active','--quiet',SERVICE])
        print('SUCCESS: V4 upgraded for LONG and SHORT PAPER signals', flush=True)
        print('BACKUP:', backup, flush=True)
        print('V3 untouched; no exchange orders; no test Telegram sent', flush=True)
    except Exception:
        for name in FILES:
            shutil.copy2(backup/name,BASE/name)
        if was_active:
            subprocess.run(['systemctl','restart',SERVICE],check=False)
        print('ROLLBACK: restored original V4 files',file=sys.stderr,flush=True)
        raise

if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print('UPGRADE FAILED:',type(exc).__name__,str(exc),file=sys.stderr)
        sys.exit(1)
