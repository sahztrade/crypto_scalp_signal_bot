"""Read-only BTC entry context study; never writes to a trading database."""
import sys,sqlite3,json,datetime,math
sys.path.insert(0,'/opt/tmco_webhook_v4')
import bybit_futures_v4 as market

def epoch(text):return datetime.datetime.fromisoformat(text).timestamp()

def history(tf,start,end):
    seconds=int(tf[:-1])*60
    cursor=int(end)//seconds*seconds
    rows={}
    while cursor>start:
        count=min(950,(cursor-start+seconds-1)//seconds)
        bars=market.fetch(tf,cursor,count)
        for b in bars:rows[b['timestamp']]=b
        next_cursor=bars[0]['timestamp']
        if next_cursor>=cursor:raise RuntimeError('HISTORY_NOT_ADVANCING')
        cursor=next_cursor
        print('HISTORY_PROGRESS',tf,len(rows),flush=True)
    result=[rows[t] for t in sorted(rows)]
    if any(b['timestamp']-a['timestamp']!=seconds for a,b in zip(result,result[1:])):
        raise RuntimeError('HISTORY_GAP')
    return result

def aggregate15(five):
    groups={}
    for b in five:groups.setdefault(b['timestamp']//900*900,[]).append(b)
    out=[]
    for t,group in sorted(groups.items()):
        if [b['timestamp'] for b in group]!=[t,t+300,t+600]:continue
        out.append(dict(timestamp=t,open=group[0]['open'],high=max(b['high'] for b in group),
                        low=min(b['low'] for b in group),close=group[-1]['close']))
    return out

def features(trade,decision,one,five,fifteen):
    # Every input is cut at the decision's information boundary.
    first_day=decision//86400*86400
    minute=[b for b in one if first_day<=b['timestamp'] and b['timestamp']+60<=decision]
    expected=(decision-first_day)//60
    if len(minute)!=expected:raise RuntimeError('INCOMPLETE_VWAP_DAY')
    def vwap(bars):
        volume=sum(b['volume'] for b in bars)
        return sum((b['high']+b['low']+b['close'])/3*b['volume'] for b in bars)/volume if volume>0 else None
    vw=vwap(minute)
    vw_before=vwap(minute[:-5]) if len(minute)>5 else None
    closed=[b for b in five if b['timestamp']+300<=decision]
    if len(closed)<21 or closed[-1]['timestamp']+300!=decision//300*300:
        raise RuntimeError('INCOMPLETE_BREAKOUT_HISTORY')
    previous=closed[-21:-1]
    high=max(b['high'] for b in previous);low=min(b['low'] for b in previous)
    latest=closed[-1]
    long=trade['direction']=='LONG'
    levels=[]
    prior_day=[b for b in closed if first_day-86400<=b['timestamp']<first_day]
    if len(prior_day)!=288:raise RuntimeError('INCOMPLETE_PREVIOUS_DAY')
    levels.extend([(max(b['high'] for b in prior_day),'PREVIOUS_DAY_HIGH'),
                   (min(b['low'] for b in prior_day),'PREVIOUS_DAY_LOW')])
    context=[b for b in fifteen if b['timestamp']+900<=decision][-32:]
    for i in range(2,len(context)-2):
        if all(context[i]['high']>context[j]['high'] for j in (i-2,i-1,i+1,i+2)):
            levels.append((context[i]['high'],'CONFIRMED_15M_HIGH'))
        if all(context[i]['low']<context[j]['low'] for j in (i-2,i-1,i+1,i+2)):
            levels.append((context[i]['low'],'CONFIRMED_15M_LOW'))
    entry=trade['entry'];risk=abs(entry-trade['sl'])
    ahead=[(p,k) for p,k in levels if (p>entry if long else p<entry)]
    nearest=min(ahead,key=lambda x:abs(x[0]-entry)) if ahead else None
    aligned=(entry>vw and vw>vw_before if long else entry<vw and vw<vw_before) if vw is not None and vw_before is not None else None
    return dict(signal_id=trade['signal_id'],direction=trade['direction'],decision_close=decision,
                entry=entry,initial_sl=trade['sl'],vwap_hlc3_1m_utc_day=vw,
                vwap_five_minutes_earlier=vw_before,vwap_aligned=aligned,
                last_5m_close=latest['close'],prior_20_high=high,prior_20_low=low,
                donchian20_break=(latest['close']>high if long else latest['close']<low),
                nearest_ahead_level=nearest[0] if nearest else None,
                level_type=nearest[1] if nearest else None,
                headroom_R=abs(nearest[0]-entry)/risk if nearest and risk>0 else None,
                level_count=len(levels))

def main():
    if market.SYMBOL!='BTCUSDT':raise RuntimeError('UNEXPECTED_MARKET_SYMBOL')
    db=sqlite3.connect('file:/opt/tmco_webhook_v4/data/v4_paper_live.db?mode=ro',uri=True,timeout=15)
    db.row_factory=sqlite3.Row
    trades=[dict(r) for r in db.execute('''SELECT t.signal_id,t.direction,t.entry,t.sl,c.decision_close
        FROM paper_trades_v4 t JOIN paper_confirmations c ON c.epoch=t.epoch
        WHERE t.signal_id BETWEEN 558 AND 747 ORDER BY t.signal_id''')]
    db.close()
    if len(trades)!=190:raise RuntimeError('EXPECTED_190_SIGNALS')
    end=epoch('2026-10-26T23:21:00+03:30')
    if any(r['decision_close']>end for r in trades):raise RuntimeError('UNEXPECTED_TIME_WINDOW')
    one=history('1m',epoch('2026-10-21T00:00:00+00:00'),end)
    five=history('5m',epoch('2026-10-20T00:00:00+00:00'),end)
    fifteen=aggregate15(five)
    result=[features(r,int(r['decision_close']),one,five,fifteen) for r in trades]
    print(json.dumps(dict(study='BTC_CONTEXT_FEATURES_190',
        definitions=dict(vwap='UTC daily HLC3*base-volume approximation from closed 1m candles',
                         breakout='Latest closed 5m close beyond preceding 20 closed 5m highs/lows',
                         levels='Previous UTC day high/low and strict 2-left/2-right pivots in last 32 closed 15m bars',
                         unknown_headroom='No level found ahead in this defined set; not proof of no resistance/support'),
        features=result),ensure_ascii=False,indent=2))

if __name__=='__main__':main()
