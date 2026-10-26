#!/usr/bin/env python3
"""Guarded installer: add only the worse-reentry rule to current BTC and ADA.
Default mode validates patches/tests without modifying bots. Use --apply to install.
"""
import argparse,ast,datetime,hashlib,json,os,pathlib,sqlite3,subprocess,tempfile,time
from decimal import Decimal

BTC=pathlib.Path('/opt/tmco_webhook_v4/v4_trade_registry.py')
ADA=pathlib.Path('/opt/btc_v13_research/ADA_V1_3_BTC_Rules_Scalp_Research.py')
PYTHON='/opt/btc_v13_research/venv_orderflow/bin/python'
SERVICES=('tmco-v4-paper.service','ada-v1-1-scalp-research.service')
EXPECTED=('8406ebf3bcfc40f93cba5c05c6f63b19d348cabf33316b8455311641bf38a24c',
          '96538679ef78e189ce47da03fab23cf459132a8bf65963165b3f97042015fa5d')
MARKER='WORSE_REENTRY_SAME_ACTIVE_STOP'

BTC_GUARD='''        # WORSE_REENTRY_SAME_ACTIVE_STOP: initial SL, active same-side trades only.
        from decimal import Decimal
        active_reentries = db.execute(
            "SELECT signal_id,entry,sl FROM paper_trades_v4 WHERE direction=? AND trade_status='active' ORDER BY signal_id",
            (direction,)).fetchall()
        for old_id,old_entry,old_sl in active_reentries:
            same_stop = abs(Decimal(str(old_sl))-Decimal(str(sl))) < Decimal('0.05')
            worse = entry > float(old_entry) if direction == 'LONG' else entry < float(old_entry)
            if same_stop and worse:
                reason = 'WORSE_REENTRY_SAME_ACTIVE_STOP: signal_id='+str(old_id)
                db.execute("UPDATE paper_confirmations SET status='rejected',error=?,attempts=attempts+1 WHERE epoch=? AND status='pending'", (reason,epoch))
                return {'status':'rejected','reason':'WORSE_REENTRY_SAME_ACTIVE_STOP','blocking_signal_id':old_id}
'''

ADA_HELPER='''
def worse_reentry_blocker(candidate, active, tick):
    """Reject only strictly worse entry against an active same-side initial SL."""
    tolerance=Decimal(str(tick))/2
    side=candidate['side']
    sl=Decimal(str(candidate['initial_stop']))
    for previous in active:
        if previous.get('side')!=side or previous.get('remaining',1)<=0:
            continue
        old_sl=previous.get('initial_stop',previous.get('stop'))
        if old_sl is None or abs(Decimal(str(old_sl))-sl)>=tolerance:
            continue
        worse=candidate['entry']>previous['entry'] if side=='LONG' else candidate['entry']<previous['entry']
        if worse:
            return previous.get('signal_id','legacy')
    return None

'''

ADA_GUARD='''                                # WORSE_REENTRY_SAME_ACTIVE_STOP; consumed crosses stay consumed.
                                if p:
                                    active_reentries=list(store.state.get('positions',[]))
                                    if store.state.get('position'):
                                        active_reentries.append(store.state['position'])
                                    blocker=worse_reentry_blocker(p,active_reentries,tick)
                                    if blocker is not None:
                                        reason=dict(reject='WORSE_REENTRY_SAME_ACTIVE_STOP',blocking_signal_id=blocker)
                                        p=None
'''

def patches(btc,ada):
    needle='        previous = db.execute("SELECT signal_id,tp2,trade_status,entry_minute_unverified FROM paper_trades_v4 WHERE direction=? ORDER BY signal_id DESC LIMIT 1", (direction,)).fetchone()'
    if btc.count(needle)!=1 or MARKER in btc:raise ValueError('UNEXPECTED_BTC_SOURCE')
    btc=btc.replace(needle,BTC_GUARD+needle,1)
    needle="                                attempts.append(dict(cross=candidate,result=reason))"
    if ada.count(needle)!=1 or MARKER in ada or ada.count('\ndef main():')!=1:
        raise ValueError('UNEXPECTED_ADA_SOURCE')
    ada=ada.replace(needle,ADA_GUARD+needle,1)
    ada=ada.replace('\ndef main():','\n'+ADA_HELPER+'\ndef main():',1)
    for name,source in (('BTC',btc),('ADA',ada)):compile(source,name,'exec')
    return btc,ada

def tests(btc,ada):
    # Compile only the affected function; execute against disposable SQLite.
    function=next(n for n in ast.parse(btc).body if isinstance(n,ast.FunctionDef) and n.name=='register_trade')
    from types import SimpleNamespace
    connections=[]
    def connect(*args,**kwargs):
        connection=sqlite3.connect(*args,**kwargs);connections.append(connection);return connection
    env=dict(sqlite3=SimpleNamespace(connect=connect),time=time,datetime=datetime.datetime,timezone=datetime.timezone,
             ZoneInfo=lambda _:datetime.timezone(datetime.timedelta(hours=3,minutes=30)),
             classify_trade=lambda *a:dict(reference_signal_id=None,classification='new'),
             market_entry_state=lambda now:dict(entry_status='filled',trade_status='active',entry_filled_at=now,monitor_start_ts=now))
    exec(compile(ast.Module(body=[function],type_ignores=[]),'BTC_TEST','exec'),env)
    columns='signal_id INTEGER PRIMARY KEY,epoch INTEGER UNIQUE,direction TEXT,entry REAL,sl REAL,tp1 REAL,tp2 REAL,decision_ts REAL,reference_signal_id INTEGER,classification TEXT,entry_status TEXT,trade_status TEXT,entry_filled_at REAL,monitor_start_ts REAL,entry_minute_unverified INTEGER,tp3 REAL,sl_current REAL,management_version TEXT,tp1_hit INTEGER,tp2_hit INTEGER,sl_method TEXT,risk_atr_new REAL,old_risk_check INTEGER'
    cases=[('LONG',101,90,'LONG',100,90,'active',True),
           ('SHORT',99,110,'SHORT',100,110,'active',True),
           ('LONG',100,90,'LONG',100,90,'active',False),
           ('LONG',99,90,'LONG',100,90,'active',False),
           ('SHORT',101,110,'SHORT',100,110,'active',False),
           ('LONG',101,89,'LONG',100,90,'active',False),
           ('LONG',101,90,'LONG',100,90,'sl',False),
           ('LONG',101,90,'SHORT',80,90,'active',False)]
    for side,entry,sl,old_side,old_entry,old_sl,status,blocked in cases:
        with tempfile.TemporaryDirectory() as folder:
            path=str(pathlib.Path(folder)/'test.db')
            with sqlite3.connect(path) as db:
                db.executescript('CREATE TABLE paper_trades_v4('+columns+');CREATE TABLE paper_confirmations(epoch INTEGER,status TEXT,direction TEXT,error TEXT,attempts INTEGER);CREATE TABLE paper_signal_outbox(id INTEGER PRIMARY KEY,epoch INTEGER,direction TEXT,message TEXT,status TEXT);CREATE TABLE paper_metadata(key TEXT PRIMARY KEY,value TEXT);')
                db.execute('INSERT INTO paper_trades_v4(signal_id,epoch,direction,entry,sl,trade_status,tp2,entry_minute_unverified) VALUES(1,1,?,?,?,?,?,0)',(old_side,old_entry,old_sl,status,old_entry))
                db.execute("INSERT INTO paper_confirmations VALUES(2,'pending',?,NULL,0)",(side,))
            db.close()
            sign=1 if side=='LONG' else -1
            result=env['register_trade'](path,side,2,entry,sl,entry+sign,entry+2*sign,4,tp3=entry+3*sign)
            for connection in connections:connection.close()
            connections.clear()
            with sqlite3.connect(path) as db:
                count=db.execute('SELECT COUNT(*) FROM paper_trades_v4').fetchone()[0]
                outbox=db.execute('SELECT COUNT(*) FROM paper_signal_outbox').fetchone()[0]
                confirmation=db.execute('SELECT status,error,attempts FROM paper_confirmations WHERE epoch=2').fetchone()
            db.close()
            assert result['status']==('rejected' if blocked else 'queued')
            assert count==(1 if blocked else 2) and outbox==(0 if blocked else 1)
            assert confirmation[0]==('rejected' if blocked else 'queued') and confirmation[2]==1
            if blocked:assert MARKER in confirmation[1]
    helper=next(n for n in ast.parse(ada).body if isinstance(n,ast.FunctionDef) and n.name=='worse_reentry_blocker')
    env=dict(Decimal=Decimal)
    exec(compile(ast.Module(body=[helper],type_ignores=[]),'ADA_TEST','exec'),env)
    for side,entry,sl,old_side,old_entry,old_sl,status,blocked in cases:
        candidate=dict(side=side,entry=entry,initial_stop=sl)
        previous=dict(signal_id=1,side=old_side,entry=old_entry,initial_stop=old_sl,remaining=1 if status=='active' else 0)
        assert (env['worse_reentry_blocker'](candidate,[previous],.0001) is not None)==blocked
    # Several open positions: a better latest entry must not hide an older blocker.
    candidate=dict(side='LONG',entry=.201,initial_stop=.19)
    active=[dict(signal_id=1,side='LONG',entry=.2,initial_stop=.19,remaining=1),dict(signal_id=2,side='LONG',entry=.202,initial_stop=.19,remaining=1)]
    assert env['worse_reentry_blocker'](candidate,active,.0001)==1
    print('PATCH_TESTS_OK: BTC transactional rejection, both directions, equal/better entries, other stops, closed trades, ADA precision',flush=True)

def run(*args):return subprocess.run(args,check=True,capture_output=True,text=True).stdout

def replace(path,data,mode):
    temporary=path.with_name(path.name+'.reentry.tmp')
    temporary.write_bytes(data);os.chmod(temporary,mode);os.replace(temporary,path)

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--apply',action='store_true');args=parser.parse_args()
    paths=(BTC,ADA);original=[p.read_bytes() for p in paths]
    if tuple(hashlib.sha256(v).hexdigest() for v in original)!=EXPECTED:
        raise SystemExit('UNEXPECTED SOURCE HASH; NO CHANGES MADE')
    updated=patches(*(v.decode('utf-8') for v in original));tests(*updated)
    if not args.apply:
        print('VALIDATION ONLY; NO BOT FILES OR SERVICES CHANGED');return
    for unit,expected in zip(SERVICES,('v4_paper_live.py','ADA_V1_3_BTC_Rules_Scalp_Research.py')):
        if expected not in run('systemctl','show',unit,'--property=ExecStart','--value'):
            raise SystemExit('UNEXPECTED SERVICE COMMAND; unchanged')
        run('systemctl','is-active',unit)
    # Preserve the BTC-style ADA message formatter and all existing self-tests.
    staged=ADA.with_name(ADA.name+'.reentry_check.py')
    try:
        staged.write_text(updated[1],encoding='utf-8')
        print(run(PYTHON,'-B',str(staged),'--self-test'),end='',flush=True)
    finally:
        if staged.exists():staged.unlink()
    tag=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
    modes=[p.stat().st_mode & 0o7777 for p in paths]
    stopped=[]
    try:
        for unit in SERVICES:
            stopped.append(unit);run('systemctl','stop',unit)
        if [p.read_bytes() for p in paths]!=original:raise RuntimeError('SOURCE CHANGED DURING INSTALL')
        for p,data in zip(paths,original):
            backup=p.with_name(p.name+'.before_worse_reentry.'+tag)
            backup.open('xb').write(data);os.chmod(backup,0o600)
            print('CODE BACKUP:',backup,flush=True)
        for filename in ('/opt/tmco_webhook_v4/data/v4_paper_live.db','/opt/ada_scalp_5m_research/ada_v1.sqlite3'):
            p=pathlib.Path(filename)
            if not p.is_file():raise RuntimeError('EXPECTED DATABASE MISSING')
            backup=p.with_name(p.name+'.before_worse_reentry.'+tag)
            print('BACKING UP DATABASE:',p,flush=True)
            with sqlite3.connect('file:'+str(p)+'?mode=ro',uri=True,timeout=30) as source:
                with sqlite3.connect(str(backup)) as destination:
                    source.backup(destination)
                    if destination.execute('PRAGMA integrity_check').fetchone()!=('ok',):raise RuntimeError('BACKUP CHECK FAILED')
            os.chmod(backup,0o600);print('DATABASE BACKUP:',backup,flush=True)
        for p,source,mode in zip(paths,updated,modes):replace(p,source.encode('utf-8'),mode)
        for unit in SERVICES:run('systemctl','start',unit)
        time.sleep(4)
        for unit in SERVICES:
            run('systemctl','is-active',unit)
            print(unit,run('systemctl','show',unit,'--property=ActiveState,SubState,MainPID,NRestarts'),flush=True)
        for p in paths:print('SHA256:',p,hashlib.sha256(p.read_bytes()).hexdigest(),flush=True)
        print('BTC AND ADA WORSE REENTRY FILTER ACTIVE; OTHER RULES AND OPEN TRADES PRESERVED',flush=True)
    except Exception:
        for unit in stopped:
            subprocess.run(['systemctl','stop',unit],capture_output=True,text=True)
        for p,data,mode in zip(paths,original,modes):replace(p,data,mode)
        for unit in stopped:
            result=subprocess.run(['systemctl','start',unit],capture_output=True,text=True)
            print('ROLLBACK RESTART:',unit,result.returncode,flush=True)
        print('ORIGINAL CODE RESTORED; DATABASE HISTORY NOT REWOUND',flush=True)
        raise

if __name__=='__main__':main()
