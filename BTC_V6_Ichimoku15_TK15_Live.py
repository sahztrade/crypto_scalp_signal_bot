#!/usr/bin/env python3
"""BTC paper-only: existing cross paths + V6/EMA/stop100/Ichimoku15 and independent TK15.
Dedicated installer; existing runner, ADA, source producer and open trade levels stay intact.
"""
import ast,datetime,hashlib,importlib.util,json,math,os,pathlib,shutil,sqlite3,subprocess,sys,tempfile,time
SOURCE=pathlib.Path('/opt/btc_v13_research/BTC_Dual_Mode_Early_No_B3.py')
SHADOW=SOURCE.with_name('BTC_V6_Forward_Shadow.py')
ICHI=SOURCE.with_name('BTC_V6_Ichimoku_Comparison.py')
PYTHON=SOURCE.parent/'venv_orderflow/bin/python'
DATA=pathlib.Path('/opt/btc_dual_h1_research')
UNIT='btc-dual-h1-research.service'
DROP=pathlib.Path('/etc/systemd/system')/(UNIT+'.d')/'90-v6-ichi15-tk15.conf'
HASHES={str(SOURCE):'63a5123a142febe4b71e049a0f7838034dd8bd630af13a3d5e5f58c440c3eebd',str(SHADOW):'9aa19e404cc9b444db348d67d9a90230dd4f9d8821951eacba546d5ce5fd600c',str(ICHI):'8c2cf80b555782f728a8c7ef6438882aa767cce57d345e31efbb82f79499b268'}
POLICY='BTC-V6-EMA-STOP100-ICHI15-CORE-AND-TK15-FIXED-V1'
ADA_UNITS=('ada-dual-h1-research.service','ada-v13-5-3-movement.service','ada-v1-1-scalp-research.service')
def checked(name,path):
 if hashlib.sha256(path.read_bytes()).hexdigest()!=HASHES[str(path)]:raise RuntimeError('DEPENDENCY_HASH_MISMATCH '+str(path))
 s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def bind(m,v6,ichi):
 original_plan=m.plan;original_fetch=m.fetch_frames;cache_views={}
 def verify(frames,at):
  for tf,seconds in (('1m',60),('3m',180),('5m',300),('15m',900)):
   bs=frames.get(tf,[]);end=at//seconds*seconds
   if not bs or bs[-1][0]+seconds!=end:raise RuntimeError('STALE_'+tf)
   if any(b[0]+seconds>end for b in bs):raise RuntimeError('OPEN_'+tf)
   if any(z[0]-a[0]!=seconds for a,z in zip(bs,bs[1:])):raise RuntimeError('GAP_'+tf)
  if len(frames['5m'])<1000 or len(frames['15m'])<78:raise RuntimeError('INSUFFICIENT_CONTEXT')
 def cloud(frames,at):
  through=at//900*900;bars=[b for b in frames['15m'] if b[0]+900<=through]
  values=ichi.clouds(bars,900)
  if not values or values[-1]['at']!=through:raise RuntimeError('ICHI_CLOUD_STALE')
  return values[-1]
 def local(frames,at):
  end=at//300*300
  if end not in cache_views:
   bars=[b for b in frames['5m'] if b[0]+300<=end][-1000:]
   if len(bars)!=1000 or bars[-1][0]+300!=end:raise RuntimeError('V6_HISTORY_MISSING')
   q=v6.Context()
   for b in bars:view=q.on_bar(b,b[0]+300)
   cache_views[end]=view
   for key in sorted(cache_views)[:-16]:cache_views.pop(key,None)
  return cache_views[end]
 def fetch_frames(symbol,cache,cutoff):
  if symbol!='BTCUSDT':raise ValueError('BTC_ONLY')
  original_fetch(symbol,cache,cutoff)
  through=cutoff//300*300
  if len(cache.get('5m',[]))<1000 or cache['5m'][-1][0]+300!=through:
   cache['5m']=m.candles(symbol,'5m',cutoff,1000)
  verify(cache,cutoff)
  return cache
 def context_gates(side,frames,at,f):
  if f.get('ema_aligned') is not True:return None,dict(reject='EMA_NOT_ALIGNED',filters=f)
  cl=m._ichi_cloud(frames,at)
  if cl['direction']!=side:return None,dict(reject='ICHI15_NOT_ALIGNED',cloud15=cl)
  state=m._v6_local(frames,at)
  if state['regime'] in ('RANGE','WARMUP'):return None,dict(reject='V6_RANGE',range5m=state)
  return dict(cloud15=cl,range5m=state),None
 def tk_event(frames,at):
  if at%900:return None
  bars=[b for b in frames['15m'] if b[0]+900<=at]
  if len(bars)<79 or bars[-1][0]+900!=at:raise RuntimeError('TK15_HISTORY_STALE')
  def mid(end,n):
   w=bars[end-n:end];return (max(b[2] for b in w)+min(b[3] for b in w))/2
  n=len(bars);ten,kij,pt,pk=mid(n,9),mid(n,26),mid(n-1,9),mid(n-1,26)
  side='LONG' if pt<=pk and ten>kij else 'SHORT' if pt>=pk and ten<kij else None
  if side is None:return None
  cl=cloud(frames,at)
  if cl['direction']!=side:return None
  return dict(at=at,side=side,rule='TK_CROSS15',tf='15m',tenkan=ten,kijun=kij,previous_tenkan=pt,previous_kijun=pk)
 def boot(store,cutoff):
  # Skip the last closed TK candle at each restart; never backfill entries.
  previous=store.get('tk15_last_processed',0)
  store.put('tk15_last_processed',max(previous,cutoff//900*900))
  store.put('btc_entry_policy',POLICY)
 def collect(store,frames,minute,pairs):
  if minute%900 or minute<=store.get('tk15_last_processed',0):return
  if time.time()-minute>45:
   store.put('tk15_last_processed',minute);return
  verify(frames,minute);event=tk_event(frames,minute)
  store.put('tk15_last_processed',minute)
  if event:pairs.append(('TK15',event,time.time()))
 def plan(symbol,mode,pair,frames,trend,tick,received):
  if symbol!='BTCUSDT' or mode not in ('M1','M2','TK15'):raise ValueError('INVALID_LIVE_PATH')
  at=int(pair['at']);side=pair['side'];now=time.time()
  if side not in ('LONG','SHORT') or at%60:return None,'INVALID_DECISION'
  if not 0<=now-received<=30 or not 0<=now-at<=45:return None,'STALE_DECISION'
  verify(frames,at)
  if mode in ('M1','M2'):
   p,reason=original_plan(symbol,mode,pair,frames,trend,tick,received)
   if p is None:return None,reason
   gate,reason=context_gates(side,frames,at,p['filters'])
   if reason:return None,reason
  else:
   if at%900 or tk_event(frames,at)!=pair:return None,'TK15_EVENT_NOT_CONFIRMED'
   f=m.filters(side,frames)
   if f['score']<4:return None,dict(reject='BTC_FILTERS',filters=f)
   gate,reason=context_gates(side,frames,at,f)
   if reason:return None,reason
   result=m.api('/v5/market/tickers',category='linear',symbol=symbol)['list']
   if len(result)!=1 or result[0]['symbol']!=symbol:raise ValueError('INVALID_TICKER')
   entry=float(result[0]['lastPrice']);opened=time.time()
   if not math.isfinite(entry) or entry<=0:raise ValueError('INVALID_ENTRY')
   if not 0<=opened-received<=30 or not 0<=opened-at<=45:return None,'STALE_DECISION'
   sign=1 if side=='LONG' else -1;reference=round(frames['1m'][-1][4],2)
   if sign*(entry-reference)/reference*100>.10:return None,'PRICE_CHASE'
   sl,tp1,tp2=m.levels(side,entry,frames['5m'][-100:],tick)
   p=dict(side=side,entry=entry,sl=sl,tp1=tp1,tp2=tp2,opened=opened,decision=at,tick=tick,score=f['score'],filters=f,cross_pair=pair,policy=m.VERSION,b3_gate_enabled=False)
  if abs(p['entry']-p['sl'])+1e-9<100:return None,dict(reject='STOP_LT100',distance=abs(p['entry']-p['sl']))
  if time.time()-p['opened']>10 or time.time()-at>45:return None,'CONTEXT_EVALUATION_STALE'
  p.update(btc_policy=POLICY,entry_path='TK15_ICHI15' if mode=='TK15' else 'CORE_V6_ICHI15',entry_context=gate,management='FIXED_INITIAL_STOP_FULL_EXIT_2_5R',hour_direction_at_decision=getattr(trend,'local',None),b3_gate_enabled=False)
  return p,'ACCEPTED'
 def entry_text(symbol,mode,sid,p):
  fmt=lambda v:format(v,'.10f').rstrip('0').rstrip('.')
  path='تقاطع تنکان/کیجون ۱۵ دقیقه' if mode=='TK15' else 'تقاطع‌های فعلی با فیلتر V6 و ابر ۱۵ دقیقه'
  direction='خرید / LONG' if p['side']=='LONG' else 'فروش / SHORT'
  return '\n'.join([symbol+' | '+path,'سیگنال پژوهشی شماره '+str(sid),'جهت: '+direction,'زمان ایران: '+m.stamp(p['opened']),'','ورود مرجع: '+fmt(p['entry']),'استاپ ثابت: '+fmt(p['sl']),'هدف اول 1.5R: '+fmt(p['tp1']),'هدف نهایی 2.5R: '+fmt(p['tp2']),'','امتیاز فیلتر: '+str(p['score'])+' از ۶','ابر ۱۵ دقیقه: هم‌جهت | رنج ۵ دقیقه: مسدود نیست','خروج کامل در استاپ یا هدف نهایی؛ بدون جابه‌جایی استاپ','پژوهشی؛ بدون سفارش واقعی'])
 m._ichi_cloud=cloud;m._v6_local=local;m._tk_event=tk_event;m._ichi_boot=boot;m._ichi_collect=collect
 m.plan=plan;m.fetch_frames=fetch_frames;m.entry_text=entry_text;m.BTC_POLICY=POLICY
 text=SOURCE.read_text(encoding='utf-8');tree=ast.parse(text)
 run_node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='run')
 run=ast.get_source_segment(text,run_node)
 def replace_once(s,old,new):
  if s.count(old)!=1:raise RuntimeError('PATCH_ANCHOR '+old[:80])
  return s.replace(old,new,1)
 run=replace_once(run,"        store.put('symbol',args.symbol)","        store.put('symbol',args.symbol)\n        _ichi_boot(store,cutoff)")
 run=replace_once(run,"                with store.db:\n                    for mode,pair,received in pairs:","                with store.db:\n                    _ichi_collect(store,cache,minute,pairs)\n                    for mode,pair,received in pairs:")
 run=replace_once(run,"frames={k:v[-1][0]+SECONDS[k] for k,v in cache.items()},policy=VERSION)","frames={k:v[-1][0]+SECONDS[k] for k,v in cache.items()},policy=VERSION,btc_policy=BTC_POLICY,cloud15=_ichi_cloud(cache,cutoff),range5m=_v6_local(cache,cutoff))")
 class_node=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='Store')
 report_node=next(n for n in class_node.body if isinstance(n,ast.FunctionDef) and n.name=='report')
 report=ast.get_source_segment(text,report_node)
 report=replace_once(report,"(('M1','اول'),('M2','دوم'),('LEGACY','قبلی'))","(('M1','تقاطع فعلی ۱/۳/۵'),('M2','تقاطع فعلی ۱۵/۶۰'),('TK15','تنکان/کیجون ۱۵'),('LEGACY','قبلی'))")
 compile(run,'btc_ichi_run','exec');compile(report,'btc_ichi_report','exec')
 ns={};exec(compile(report,'btc_ichi_report','exec'),m.__dict__,ns);m.Store.report=ns['report']
 exec(compile(run,'btc_ichi_run','exec'),m.__dict__)
 return m

def boot_runtime():
 m=checked('btc_core_frozen',SOURCE);v6=checked('v6_frozen',SHADOW);ichi=checked('ichi_frozen',ICHI)
 return bind(m,v6,ichi)

def tests(m):
 from unittest.mock import patch
 from types import SimpleNamespace
 # Use helpers with causal bars. No requests, source DB writes, or Telegram calls.
 at=90000
 def bars(seconds,count):return [[at-count*seconds+i*seconds,78000,78001,77999,78000,10] for i in range(count)]
 frames={tf:bars(seconds,1000 if tf=='5m' else 300) for tf,seconds in m.SECONDS.items()}
 cl=m._ichi_cloud(frames,at);assert cl['at']==at and cl['direction']=='INSIDE'
 assert m._tk_event(frames,at) is None
 old=dict(frames);old['15m']=[list(x) for x in frames['15m']];old['15m'][-1][2]=999999
 assert m._ichi_cloud(old,at)['high']==cl['high'],'CURRENT_BAR_LEAK_INTO_DISPLACED_CLOUD'
 for side,kind in (('LONG','UP'),('SHORT','DOWN')):
  f={tf:[list(b) for b in bs] for tf,bs in frames.items()}
  if side=='LONG':
   f['15m'][-27][2]=78100
   for i in range(26):f['15m'][-27-i][2]=78100
   f['15m'][-1][4]=78200;f['15m'][-1][2]=78201
  else:
   for i in range(26):f['15m'][-27-i][3]=77900
   f['15m'][-1][4]=77800;f['15m'][-1][3]=77799
  # The exact crossover helper is separately tested with synthetic chronological histories below.
  assert m._ichi_cloud(f,at)['direction']==side
 class FakeStore:
  def __init__(self):self.d={}
  def get(self,k,default=None):return self.d.get(k,default)
  def put(self,k,v):self.d[k]=v
 store=FakeStore();m._ichi_boot(store,at);pairs=[]
 m._ichi_collect(store,frames,at,pairs);assert not pairs
 assert store.get('tk15_last_processed')==at
 m._ichi_boot(store,at+1800);assert store.get('tk15_last_processed')==at+1800
 with patch.object(m.time,'time',return_value=at+1):
  p,reason=m.plan('BTCUSDT','M1',dict(at=at-60,side='LONG'),frames,SimpleNamespace(local='LONG'),.1,at)
  assert p is None and reason=='STALE_DECISION'
 # Existing fixed management and both target formulas remain unchanged.
 p=dict(side='LONG',entry=78000,sl=77800,tp1=78300,tp2=78500,hit1=0)
 status,_,hit=m.observe(p,[0,78000,78310,77900,78200,1]);assert status=='OPEN' and hit==1 and p['sl']==77800
 for side in ('LONG','SHORT'):
  sl,t1,t2=m.levels(side,78000,frames['5m'][-100:],.1);risk=abs(78000-sl)
  assert abs(abs(t1-78000)/risk-1.5)<.05 and abs(abs(t2-78000)/risk-2.5)<.05
 # Readable new-path message must not claim an hourly confirmation.
 text=m.entry_text('BTCUSDT','TK15',7,dict(side='SHORT',opened=at,entry=78000,sl=78200,tp1=77700,tp2=77500,score=4))
 assert 'تنکان/کیجون' in text and 'روند یک‌ساعته' not in text
 # Entry gates are tested against the original live plan with requests mocked.
 from contextlib import ExitStack
 base_filters=dict(score=4,ema_aligned=True,rsi_aligned=True,b3_reject=True)
 def level_stub(side,entry,five,tick):
  sign=1 if side=='LONG' else -1
  return entry-sign*200,entry+sign*300,entry+sign*500
 trend=SimpleNamespace(local='LONG',bars=frames['1h'])
 with ExitStack() as stack:
  stack.enter_context(patch.object(m.time,'time',return_value=at+1))
  stack.enter_context(patch.object(m,'_btc_early_permission',return_value=None,create=True))
  api=stack.enter_context(patch.object(m,'api',return_value={'list':[{'symbol':'BTCUSDT','lastPrice':'78000'}]}))
  filt=stack.enter_context(patch.object(m,'filters',return_value=base_filters))
  levels_mock=stack.enter_context(patch.object(m,'levels',side_effect=level_stub))
  cv=stack.enter_context(patch.object(m,'_ichi_cloud',return_value={'at':at,'direction':'LONG'}))
  rv=stack.enter_context(patch.object(m,'_v6_local',return_value={'regime':'NON_RANGE'}))
  p,reason=m.plan('BTCUSDT','M1',dict(at=at,side='LONG'),frames,trend,.1,at)
  assert p and p['entry_path']=='CORE_V6_ICHI15' and p['b3_gate_enabled'] is False
  assert p['sl']==77800 and p['tp2']==78500
  filt.return_value=dict(base_filters,ema_aligned=False)
  assert m.plan('BTCUSDT','M1',dict(at=at,side='LONG'),frames,trend,.1,at)[1]['reject']=='EMA_NOT_ALIGNED'
  filt.return_value=base_filters;rv.return_value={'regime':'RANGE'}
  assert m.plan('BTCUSDT','M1',dict(at=at,side='LONG'),frames,trend,.1,at)[1]['reject']=='V6_RANGE'
  rv.return_value={'regime':'NON_RANGE'};cv.return_value={'direction':'SHORT'}
  assert m.plan('BTCUSDT','M1',dict(at=at,side='LONG'),frames,trend,.1,at)[1]['reject']=='ICHI15_NOT_ALIGNED'
  cv.return_value={'direction':'LONG'};levels_mock.side_effect=None;levels_mock.return_value=(77901,78148.5,78247.5)
  assert m.plan('BTCUSDT','M1',dict(at=at,side='LONG'),frames,trend,.1,at)[1]['reject']=='STOP_LT100'
  levels_mock.side_effect=level_stub
  tf={key:[list(b) for b in bs] for key,bs in frames.items()}
  for i in range(1,27):tf['15m'][-i][2:5]=[78500,77900,78000]
  for i in range(1,10):tf['15m'][-i][2:5]=[78100,78000,78050]
  tf['15m'][-1][1:5]=[78500,78600,78400,78550]
  event=m._tk_event(tf,at);assert event and event['side']=='LONG'
  p,reason=m.plan('BTCUSDT','TK15',event,tf,SimpleNamespace(local=None),.1,at)
  assert p and p['entry_path']=='TK15_ICHI15','TK incorrectly required H1/TMCO'
  store2=FakeStore();store2.put('tk15_last_processed',at-900);pairs=[]
  m._ichi_collect(store2,tf,at,pairs);m._ichi_collect(store2,tf,at,pairs)
  assert len(pairs)==1 and pairs[0][0]=='TK15','duplicate TK candle'
  # Both paths may overlap; same-stop worse reentry guard stays active.
 with tempfile.TemporaryDirectory(prefix='btc_ichi_test_',dir=SOURCE.parent) as directory:
  s=m.Store(directory)
  p=dict(side='LONG',entry=78000,sl=77800,tp1=78300,tp2=78500,tick=.1,opened=at+1,decision=at,score=4)
  with s.db:
   assert s.add('BTCUSDT','M1',p,'core')[0]
   assert s.add('BTCUSDT','TK15',p,'tk')[0]
   assert s.add('BTCUSDT','M1',dict(p,entry=78001),'worse')[1]=='WORSE_REENTRY_SAME_ACTIVE_STOP'
   assert s.add('BTCUSDT','M1',dict(p,sl=77700),'different-stop')[0]
   s.report('BTCUSDT',m.dt.datetime.fromtimestamp(at,m.IRAN).date())
  text=s.db.execute("SELECT text FROM outbox WHERE kind='REPORT'").fetchone()[0]
  assert 'تنکان/کیجون ۱۵' in text
  assert s.db.execute("SELECT count(*) FROM outbox WHERE kind='ENTRY'").fetchone()[0]==3
  s.db.close()
 print('BTC_ICHI_ENTRY_TEST_OK: core gates, EMA/range/cloud/stop100 rejection, TK without H1, dedup/restart, cross-path overlap, same-stop guard, daily report')
 print('BTC_ICHI_LIVE_TEST_OK: displaced cloud, no historical TK bootstrap, state restore, stale rejection, fixed exits, original targets, readable paths')

def command(args,check=True):
 r=subprocess.run(args,text=True,capture_output=True,env=dict(os.environ,SYSTEMD_PAGER='cat'))
 if check and r.returncode:raise RuntimeError('COMMAND_FAILED '+repr(args)+': '+r.stderr[-500:])
 return r.stdout.strip()
def property_value(unit,key):return command(['systemctl','show','--no-pager',unit,'--property='+key,'--value'])
def install():
 if os.geteuid()!=0:raise RuntimeError('ROOT_REQUIRED')
 script=pathlib.Path(__file__).resolve()
 if script.parent!=SOURCE.parent or not PYTHON.is_file():raise RuntimeError('EXPECTED_RESEARCH_LOCATION')
 if DROP.exists():raise RuntimeError('INSTALL_DROPIN_ALREADY_EXISTS; NO_CHANGES')
 m=boot_runtime();tests(m)
 if property_value(UNIT,'ActiveState')!='active':raise RuntimeError('BTC_SERVICE_NOT_ACTIVE')
 pid=property_value(UNIT,'MainPID');argv=pathlib.Path('/proc',pid,'cmdline').read_bytes().decode().rstrip('\0').split('\0')
 if argv.count(str(SOURCE))!=1 or '--watch' not in argv or '--telegram' not in argv:raise RuntimeError('UNEXPECTED_BTC_COMMAND')
 def option(name):
  if argv.count(name)!=1:raise RuntimeError('MISSING_OR_DUPLICATE_OPTION '+name)
  return argv[argv.index(name)+1]
 if option('--symbol')!='BTCUSDT' or option('--data-dir')!=str(DATA):raise RuntimeError('UNEXPECTED_SYMBOL_OR_DATABASE')
 if any('%' in a or '\n' in a or '\r' in a for a in argv):raise RuntimeError('UNSUPPORTED_UNIT_ARGUMENT')
 ada_before={u:property_value(u,'ActiveState') for u in ADA_UNITS}
 if any(v=='active' for v in ada_before.values()):raise RuntimeError('ADA_UNEXPECTEDLY_ACTIVE; NO_CHANGES')
 # Network preflight creates only a temporary independent DB, never sends Telegram.
 with tempfile.TemporaryDirectory(prefix='btc_ichi_check_',dir=SOURCE.parent) as tmp:
  print(command([str(PYTHON),'-B',str(script),'--check','--symbol','BTCUSDT','--data-dir',tmp]),flush=True)
 tag=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
 backup=SOURCE.parent/('before_btc_ichi15_tk15_'+tag);backup.mkdir(mode=0o700)
 (backup/'service.txt').write_text(command(['systemctl','cat','--no-pager',UNIT]),encoding='utf-8')
 shutil.copy2(SOURCE,backup/SOURCE.name);shutil.copy2(script,backup/script.name)
 dbpath=DATA/'dual.sqlite3';stopped=False;created=False
 try:
  command(['systemctl','stop',UNIT]);stopped=True
  c=sqlite3.connect(dbpath.resolve().as_uri()+'?mode=ro',uri=True,timeout=20);dest=sqlite3.connect(backup/'dual.sqlite3')
  try:
   c.backup(dest)
   before=c.execute('SELECT id,entry,sl,tp1,tp2,opened,decision,mode FROM trades').fetchall()
  finally:dest.close();c.close()
  newargs=[str(script) if a==str(SOURCE) else a for a in argv]
  DROP.parent.mkdir(exist_ok=True)
  with DROP.open('x',encoding='utf-8') as f:f.write('[Service]\nExecStart=\nExecStart='+' '.join(json.dumps(a) for a in newargs)+'\n')
  created=True
  command(['systemctl','daemon-reload']);command(['systemctl','restart',UNIT])
  deadline=time.monotonic()+90;heartbeat=None
  while time.monotonic()<deadline:
   if property_value(UNIT,'ActiveState')=='failed':raise RuntimeError('SERVICE_FAILED')
   with sqlite3.connect(dbpath.resolve().as_uri()+'?mode=ro',uri=True,timeout=10) as c:
    row=c.execute("SELECT value FROM meta WHERE key='heartbeat'").fetchone()
    hb=json.loads(row[0]) if row else {}
    if hb.get('btc_policy')==POLICY and time.time()-hb.get('at',0)<20:
     heartbeat=hb;break
   time.sleep(1)
  if heartbeat is None:raise RuntimeError('NEW_POLICY_HEARTBEAT_NOT_CONFIRMED')
  with sqlite3.connect(dbpath.resolve().as_uri()+'?mode=ro',uri=True,timeout=10) as c:
   for row in before:
    after=c.execute('SELECT id,entry,sl,tp1,tp2,opened,decision,mode FROM trades WHERE id=?',(row[0],)).fetchone()
    if row!=after:raise RuntimeError('EXISTING_TRADE_LEVEL_CHANGED')
  for u,v in ada_before.items():
   if property_value(u,'ActiveState')!=v:raise RuntimeError('ADA_STATE_CHANGED')
  print('BACKUP_DIRECTORY:',backup,flush=True)
  print('DATABASE_BACKUP:',backup/'dual.sqlite3',flush=True)
  print(command(['systemctl','show','--no-pager',UNIT,'-p','MainPID','-p','ActiveState','-p','SubState','-p','NRestarts']),flush=True)
  print('BTC_HEARTBEAT:',json.dumps(heartbeat),flush=True)
  print('BTC_V6_ICHI15_AND_TK15_ACTIVE; FIXED_SL_1.5R_2.5R; OVERLAP_ALLOWED',flush=True)
  print('SAME_ACTIVE_STOP_WORSE_REENTRY_GUARD_RETAINED; NO_HISTORICAL_TK_ENTRIES',flush=True)
  print('OPEN_TRADE_LEVELS_AND_HISTORY_PRESERVED; ADA_REMAINS_DISABLED; ENTRY_AND_MIDNIGHT_REPORTS_ONLY',flush=True)
 except Exception as e:
  if created:
   command(['systemctl','stop',UNIT],False);DROP.unlink();command(['systemctl','daemon-reload'],False)
  if stopped:command(['systemctl','start',UNIT],False)
  raise RuntimeError('INSTALL_FAILED; ORIGINAL_EXECSTART_RESTORED; DATABASE_NOT_REWOUND; BACKUP '+str(backup)+': '+str(e))

def main():
 if '--install' in sys.argv:
  if sys.argv[1:]!=['--install']:raise SystemExit('USE --install ALONE')
  install();return
 m=boot_runtime()
 if sys.argv[1:]==['--self-test']:
  tests(m);m.self_test();return
 m.main()
if __name__=='__main__':main()
