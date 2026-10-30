#!/usr/bin/env python3
"""Install BTC-only early-breakout permission and disable its B3 entry gate.
Run with the orderflow venv Python. --self-test validates only; --apply installs.
ADA and the shared original runner are not modified.
"""
import argparse, ast, datetime, hashlib, importlib.util, json, os
from pathlib import Path
import sqlite3, subprocess, sys, tempfile, time

SOURCE = Path('/opt/btc_v13_research/BTC_ADA_Dual_Mode_Research.py')
TARGET = SOURCE.with_name('BTC_Dual_Mode_Early_No_B3.py')
PYTHON = Path('/opt/btc_v13_research/venv_orderflow/bin/python')
DATA = Path('/opt/btc_dual_h1_research')
BTC_UNIT = 'btc-dual-h1-research.service'
ADA_UNIT = 'ada-dual-h1-research.service'
DROPIN = Path('/etc/systemd/system')/(BTC_UNIT+'.d')/'50-btc-early-no-b3.conf'
EXPECTED = 'fce61e6031f23a0f872dde326c1cec6b3e327e6ad94b1616b8aa3917892bd4e2'
POLICY = 'BTC-H1-OR-EARLY15M-2X5M-NO-B3-V1'

HELPER = "BTC_POLICY = 'BTC-H1-OR-EARLY15M-2X5M-NO-B3-V1'\n\nclass BTCEarlyBreakout:\n    \"\"\"Known 15m pivots, two closed 5m breaks, 30-minute permission.\"\"\"\n    def __init__(self, saved=None):\n        self.history = []\n        self.pivots = {}\n        self.streaks = {}\n        self.used = {}\n        self.active = None\n        self.last15 = 0\n        self.last5 = 0\n        if saved:\n            self.__dict__.update(json.loads(json.dumps(saved)))\n\n    def step15(self, b):\n        end = b[0]+900\n        if end <= self.last15:\n            return\n        if self.last15 and b[0] != self.last15:\n            raise ValueError('BTC_EARLY_15M_GAP')\n        self.last15 = end\n        self.history = (self.history+[list(b)])[-300:]\n        if len(self.history)<15:\n            return\n        p = self.history[-3]\n        neighbors = self.history[-5:-3]+self.history[-2:]\n        buffer = .10*atr(self.history[-100:])\n        for side,index in (('LONG',2),('SHORT',3)):\n            valid = all(p[index]>x[index] if side=='LONG' else p[index]<x[index]\n                        for x in neighbors)\n            if valid:\n                self.pivots[side] = dict(identity=side+':'+str(p[0]),\n                    known=end,level=p[index],buffer=buffer)\n                self.streaks[side] = 0\n\n    def step5(self,b):\n        end = b[0]+300\n        if end<=self.last5:\n            return\n        if self.last5 and b[0]!=self.last5:\n            raise ValueError('BTC_EARLY_5M_GAP')\n        self.last5 = end\n        if self.active:\n            back = (b[4]<=self.active['level'] if self.active['side']=='LONG'\n                    else b[4]>=self.active['level'])\n            if end>self.active['expires'] or back:\n                self.active = None\n        for side in ('LONG','SHORT'):\n            p = self.pivots.get(side)\n            if not p:\n                continue\n            if b[0]<p['known']:\n                self.streaks[side] = 0\n                continue\n            outside = (b[4]>p['level']+p['buffer'] if side=='LONG'\n                       else b[4]<p['level']-p['buffer'])\n            self.streaks[side] = self.streaks.get(side,0)+1 if outside else 0\n            if self.streaks[side]>=2 and self.used.get(side)!=p['identity']:\n                self.used[side] = p['identity']\n                self.active = dict(side=side,level=p['level'],formed=end,\n                                   expires=end+1800,pivot=p['identity'])\n\n\ndef _btc_update_early(frames,trend,at):\n    state = BTCEarlyBreakout(getattr(trend,'btc_early_state',None))\n    events = []\n    for tf,seconds,last,kind in (('15m',900,state.last15,0),('5m',300,state.last5,1)):\n        bars = frames[tf]\n        finish = at//seconds*seconds\n        if last and finish>last and bars[0][0]>last:\n            bars = candles('BTCUSDT',tf,at,int((finish-last)//seconds))\n        events.extend((b[0]+seconds,kind,b) for b in bars\n                      if last<b[0]+seconds<=at)\n    for end,kind,b in sorted(events,key=lambda x:(x[0],x[1])):\n        if kind==0:\n            state.step15(b)\n        else:\n            state.step5(b)\n    trend.btc_early_state = state.__dict__\n    return state\n\n\ndef _btc_early_permission(trend,at):\n    s = getattr(trend,'btc_early_state',{})\n    if s.get('last15')!=at//900*900 or s.get('last5')!=at//300*300:\n        return None\n    a = s.get('active')\n    return a if a and a['formed']<=at<=a['expires'] else None\n\n"

def build(source):
    tree = ast.parse(source)
    nodes = {n.name:n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
    for name in ('plan','run','main','HourTrend'):
        if name not in nodes:
            raise RuntimeError('MISSING_'+name)
    if 'HOUR_STRUCTURE_CONFIRMED_PIVOTS_V1' not in ast.get_source_segment(source,nodes['HourTrend']):
        raise RuntimeError('UNEXPECTED_HOUR_RULE')
    plan = ast.get_source_segment(source,nodes['plan'])
    old = "    if trend.local!=side:\n        return None,'H1_TREND_NOT_ALIGNED'"
    new = "    if symbol!='BTCUSDT':\n        raise ValueError('BTC_ONLY_RUNNER')\n    early = _btc_early_permission(trend,pair['at'])\n    hour_ok = trend.local==side\n    early_ok = bool(early and early['side']==side)\n    if not (hour_ok or early_ok):\n        return None,'H1_OR_EARLY_TREND_NOT_ALIGNED'"
    plan = replace_once(plan,old,new)
    plan = replace_once(plan,"if f['score']<4 or f['b3_reject']:","if f['score']<4:")
    plan = replace_once(plan,"cross_pair=pair,policy=VERSION)",
        "cross_pair=pair,policy=VERSION,btc_policy=BTC_POLICY,\n                entry_path='H1' if hour_ok else 'EARLY_15M_2X5M',\n                early_breakout=early if early_ok else None,b3_gate_enabled=False)")
    run = ast.get_source_segment(source,nodes['run'])
    run = replace_once(run,"    cache = fetch_frames(args.symbol,{},cutoff)",
        "    cache = fetch_frames(args.symbol,{},cutoff)\n    _btc_update_early(cache,trend,cutoff)")
    run = replace_once(run,"                with store.db:\n                    for mode,pair,received in pairs:",
        "                _btc_update_early(cache,trend,minute)\n                with store.db:\n                    for mode,pair,received in pairs:")
    run = replace_once(run,"store.put('heartbeat',dict(at=time.time(),minute=cursor,trend=trend.local))",
        "store.put('heartbeat',dict(at=time.time(),minute=cursor,trend=trend.local,\n                        btc_policy=BTC_POLICY,early=_btc_early_permission(trend,minute)))")
    main = ast.get_source_segment(source,nodes['main'])
    main = replace_once(main,"choices=('BTCUSDT','ADAUSDT')","choices=('BTCUSDT',)")
    main = replace_once(main,"        install()","        raise SystemExit('USE_DEDICATED_BTC_INSTALLER')")
    lines = source.splitlines(keepends=True)
    replacements = [('plan',HELPER+plan),('run',run),('main',main)]
    for name,text in sorted(replacements,key=lambda x:nodes[x[0]].lineno,reverse=True):
        n = nodes[name]
        lines[n.lineno-1:n.end_lineno] = [text+'\n']
    result = ''.join(lines)
    compile(result,'btc_early_runner','exec')
    return result


def replace_once(source,old,new):
    if source.count(old)!=1:
        raise RuntimeError('PATCH_ANCHOR_MISMATCH: '+old[:70])
    return source.replace(old,new,1)

def extra_tests(module):
    from unittest.mock import patch
    E = module.BTCEarlyBreakout
    def candle(at,close):
        return [at,close,close+.5,close-.5,close,10]
    # Two closes required; no use of a candle opened before pivot confirmation.
    for side,price in (('LONG',102),('SHORT',98)):
        e = E()
        e.pivots[side] = dict(identity=side+':x',known=900,level=100,buffer=.1)
        e.step5(candle(600,price))
        assert e.active is None
        e.step5(candle(900,price))
        assert e.active is None
        e.step5(candle(1200,price))
        assert e.active['side']==side and e.active['formed']==1500
        restored = E(json.loads(json.dumps(e.__dict__)))
        for at in (1500,1800):
            e.step5(candle(at,price));restored.step5(candle(at,price))
        assert e.__dict__==restored.__dict__
        e.step5(candle(2100,100))
        assert e.active is None
        e.step5(candle(2400,price));e.step5(candle(2700,price))
        assert e.active is None, 'RETRIGGERED_CONSUMED_PIVOT'
    e = E()
    e.pivots['LONG'] = dict(identity='LONG:x',known=0,level=100,buffer=.1)
    for at in range(0,2700,300):
        e.step5(candle(at,102))
    assert e.active is None, 'EXPIRY_FAILED'
    # A pivot becomes known only after both right-hand 15m bars have closed.
    e = E()
    for i in range(15):
        e.step15([i*900,100,101,99,100,10])
    e.step15([13500,100,110,99,100,10])
    e.step15([14400,100,101,99,100,10])
    assert 'LONG' not in e.pivots
    e.step15([15300,100,101,99,100,10])
    assert e.pivots['LONG']['known']==16200
    frozen = e.pivots['LONG']['buffer']
    e.step15([16200,100,109,90,100,10])
    assert e.pivots['LONG']['buffer']==frozen
    # Batch bootstrap, incremental updates and saved-state restart must agree.
    import math
    fifteen = [[i*900,100,102+math.sin(i),98+math.sin(i),100+math.sin(i),10]
               for i in range(100)]
    five = [[i*300,100,102,98,100+1.5*math.sin(i/5),10]
            for i in range(300)]
    frames = {'15m':fifteen,'5m':five}
    a,b = module.HourTrend(),module.HourTrend()
    module._btc_update_early(frames,a,90000)
    for at in range(900,90001,900):
        prefix = {tf:[x for x in bars if x[0]+module.SECONDS[tf]<=at]
                  for tf,bars in frames.items()}
        module._btc_update_early(prefix,b,at)
        b = module.HourTrend(json.loads(json.dumps(b.__dict__)))
    assert a.btc_early_state==b.btc_early_state, 'PREFIX_OR_RESTORE_MISMATCH'
    # Score, age, price chase and hourly freshness remain enforced; B3 no longer blocks BTC.
    at = 180000
    frames = {tf:[[at//seconds*seconds-(300-i)*seconds,100,101,99,100,10]
                  for i in range(300)]
              for tf,seconds in module.SECONDS.items()}
    h = module.HourTrend()
    h.bars = [[at-3600,100,101,99,100,10]]
    h.local = 'LONG'
    h.btc_early_state = dict(last15=at,last5=at,active=dict(
        side='SHORT',level=101,formed=at,expires=at+1800))
    f = dict(score=4,b3_reject=True)
    ticker = {'list':[{'symbol':'BTCUSDT','lastPrice':'100'}]}
    with patch.object(module,'filters',return_value=f), patch.object(module,'api',return_value=ticker), patch.object(module.time,'time',return_value=at+1):
        for mode in ('M1','M2'):
            p,reason = module.plan('BTCUSDT',mode,dict(side='SHORT',at=at),frames,h,.1,at)
            assert p and p['entry_path']=='EARLY_15M_2X5M' and p['b3_gate_enabled'] is False
        h.btc_early_state['active'] = None
        assert module.plan('BTCUSDT','M1',dict(side='SHORT',at=at),frames,h,.1,at)[0] is None
        h.local = 'SHORT'
        assert module.plan('BTCUSDT','M1',dict(side='SHORT',at=at),frames,h,.1,at)[0]
        f['score'] = 3
        assert module.plan('BTCUSDT','M1',dict(side='SHORT',at=at),frames,h,.1,at)[0] is None
        f['score'] = 4
        assert module.plan('BTCUSDT','M1',dict(side='SHORT',at=at),frames,h,.1,at-40)[1]=='STALE_DECISION'
        ticker['list'][0]['lastPrice'] = '99'
        assert module.plan('BTCUSDT','M1',dict(side='SHORT',at=at),frames,h,.1,at)[1]=='PRICE_CHASE'
    print('BTC_EARLY_TESTS_OK: two closes, known pivots, frozen buffer, expiry, invalidation, no retrigger, state restore, causality, both modes, B3 bypass, other gates retained')

def load_module(path):
    spec = importlib.util.spec_from_file_location('btc_early_validation',path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def service_value(unit,name):
    return subprocess.check_output(['systemctl','show',unit,'--property='+name,'--value'],text=True).strip()


def atomic_write(path,data,mode=0o600):
    temporary = path.with_name(path.name+'.tmp.'+str(os.getpid()))
    with temporary.open('xb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(temporary,mode)
    os.replace(temporary,path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--self-test',action='store_true')
    args = parser.parse_args()
    if os.geteuid()!=0:
        raise SystemExit('ROOT_REQUIRED')
    original = SOURCE.read_bytes()
    if hashlib.sha256(original).hexdigest()!=EXPECTED:
        raise SystemExit('SOURCE_HASH_MISMATCH; NO CHANGES')
    updated = build(original.decode('utf-8')).encode('utf-8')
    with tempfile.TemporaryDirectory(prefix='btc_early_validate_',dir=SOURCE.parent) as folder:
        stage = Path(folder)/'btc_early_runner.py'
        stage.write_bytes(updated)
        module = load_module(stage)
        module.self_test()
        extra_tests(module)
        if not args.apply:
            print('SELF_TEST_ONLY; NO SERVICES OR BOT FILES CHANGED')
            return
        subprocess.run([sys.executable,'-B',str(stage),'--check','--symbol','BTCUSDT',
                        '--data-dir',str(Path(folder)/'check')],check=True)

    expected_args = [str(PYTHON),'-B',str(SOURCE),'--watch','--telegram',
                     '--symbol','BTCUSDT','--data-dir',str(DATA)]
    if Path(sys.executable).resolve()!=PYTHON.resolve():
        raise SystemExit('USE_ORDERFLOW_VENV_PYTHON; NO CHANGES')
    if service_value(BTC_UNIT,'ActiveState')!='active':
        raise SystemExit('BTC_SERVICE_NOT_ACTIVE; NO CHANGES')
    if service_value(ADA_UNIT,'ActiveState')!='active':
        raise SystemExit('ADA_SERVICE_NOT_ACTIVE; NO CHANGES')
    old_pid = int(service_value(BTC_UNIT,'MainPID'))
    actual_args = Path('/proc/'+str(old_pid)+'/cmdline').read_bytes().decode().rstrip('\0').split('\0')
    if actual_args!=expected_args:
        raise SystemExit('UNEXPECTED_BTC_START_OPTIONS; NO CHANGES')
    ada_pid = service_value(ADA_UNIT,'MainPID')
    if TARGET.exists() or DROPIN.exists():
        raise SystemExit('TARGET_OR_DROPIN_ALREADY_EXISTS; NO CHANGES')
    if SOURCE.read_bytes()!=original:
        raise SystemExit('SOURCE_CHANGED_DURING_VALIDATION; NO CHANGES')
    suffix = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
    backup = SOURCE.parent/('before_btc_early_no_b3_'+suffix)
    backup.mkdir(mode=0o700)
    (backup/SOURCE.name).write_bytes(original)
    unit_text = subprocess.check_output(['systemctl','cat',BTC_UNIT])
    (backup/'btc_service_before.txt').write_bytes(unit_text)
    print('BACKUP_DIRECTORY:',backup,flush=True)
    new_args = [str(PYTHON),'-B',str(TARGET),'--watch','--telegram',
                '--symbol','BTCUSDT','--data-dir',str(DATA)]
    def quote(arg):
        return '"'+arg.replace('\\','\\\\').replace('"','\\"').replace('%','%%')+'"'
    drop_text = ('[Service]\nExecStart=\nExecStart='+
                 ' '.join(quote(a) for a in new_args)+'\n').encode()
    stopped = False
    changed = False
    try:
        subprocess.run(['systemctl','stop',BTC_UNIT],check=True)
        stopped = True
        db_path = DATA/'dual.sqlite3'
        with sqlite3.connect('file:'+str(db_path)+'?mode=ro',uri=True,timeout=30) as db:
            with sqlite3.connect(backup/'dual.sqlite3') as dest:
                db.backup(dest)
        print('DATABASE_BACKUP:',backup/'dual.sqlite3',flush=True)
        if SOURCE.read_bytes()!=original:
            raise RuntimeError('SOURCE_CHANGED_BEFORE_INSTALL')
        atomic_write(TARGET,updated,0o644)
        DROPIN.parent.mkdir(parents=True,exist_ok=True)
        atomic_write(DROPIN,drop_text,0o644)
        changed = True
        subprocess.run(['systemctl','daemon-reload'],check=True)
        started = time.time()
        subprocess.run(['systemctl','start',BTC_UNIT],check=True)
        verified = None
        deadline = time.monotonic()+120
        while time.monotonic()<deadline:
            if service_value(BTC_UNIT,'ActiveState')=='failed':
                raise RuntimeError('BTC_SERVICE_FAILED')
            try:
                with sqlite3.connect('file:'+str(db_path)+'?mode=ro',uri=True,timeout=5) as db:
                    row = db.execute("SELECT value FROM meta WHERE key='heartbeat'").fetchone()
                heartbeat = json.loads(row[0]) if row else {}
                if heartbeat.get('btc_policy')==POLICY and heartbeat.get('at',0)>=started-1:
                    pid = int(service_value(BTC_UNIT,'MainPID'))
                    running = Path('/proc/'+str(pid)+'/cmdline').read_bytes().decode().rstrip('\0').split('\0')
                    if running==new_args and service_value(BTC_UNIT,'SubState')=='running':
                        verified = heartbeat
                        break
            except (sqlite3.Error,FileNotFoundError,ValueError):
                pass
            time.sleep(1)
        if verified is None:
            raise RuntimeError('NEW_BTC_HEARTBEAT_NOT_VERIFIED')
        if SOURCE.read_bytes()!=original:
            raise RuntimeError('SHARED_SOURCE_UNEXPECTEDLY_CHANGED')
        if service_value(ADA_UNIT,'MainPID')!=ada_pid or service_value(ADA_UNIT,'ActiveState')!='active':
            raise RuntimeError('ADA_SERVICE_CHANGED_DURING_INSTALL')
        print(subprocess.check_output(['systemctl','show',BTC_UNIT,
            '--property=MainPID,NRestarts,ActiveState,SubState,UnitFileState'],text=True),flush=True)
        print('BTC_HEARTBEAT:',json.dumps(verified,ensure_ascii=False),flush=True)
        print('BTC_RUNNER_SHA256:',hashlib.sha256(updated).hexdigest(),flush=True)
        print('BTC_EARLY_15M_TWO_5M_AND_NO_B3_ACTIVE_FOR_BOTH_MODES')
        print('ADA_PID_UNCHANGED:',ada_pid)
        print('SHARED_SOURCE_UNCHANGED; HISTORY_AND_OPEN_TRADES_PRESERVED')
        print('CROSS_PAIRS, SCORE_4, FIXED_ORIGINAL_SL, 1.5R/2.5R, REENTRY_AND_TELEGRAM_POLICY_PRESERVED')
    except BaseException as exc:
        print('INSTALL_FAILED:',type(exc).__name__,str(exc),flush=True)
        try:
            if changed:
                subprocess.run(['systemctl','stop',BTC_UNIT],check=True)
                if DROPIN.exists() and DROPIN.read_bytes()==drop_text:
                    DROPIN.unlink()
                subprocess.run(['systemctl','daemon-reload'],check=True)
            if stopped:
                subprocess.run(['systemctl','start',BTC_UNIT],check=True)
            print('BTC_PREVIOUS_START_CONFIGURATION_RESTORED; TRADE_HISTORY_NOT_ROLLED_BACK',flush=True)
        except BaseException as rollback:
            print('ROLLBACK_ERROR:',type(rollback).__name__,str(rollback),flush=True)
        raise


if __name__=='__main__':
    main()
