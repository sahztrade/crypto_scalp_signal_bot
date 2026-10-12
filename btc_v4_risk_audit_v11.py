#!/usr/bin/env python3
"""V11 BTC paper signal risk audit. READ ONLY; no orders, DB writes or services.
Consumes V10 features already computed from 288 completed 5-minute candles.
Rules are exploratory hypotheses, NOT validated filters or trading advice.
"""
import argparse
import csv
from pathlib import Path

DEFAULT_IN='/tmp/btc_v4_structure_context_v10/all_signals.csv'
DEFAULT_OUT='/tmp/btc_v4_risk_audit_v11'

def number(row, key):
    try:
        val=row.get(key,'')
        return float(val) if val not in ('',None) else None
    except (ValueError,TypeError):
        return None

def evaluate(r):
    side=r['SIDE'].upper()
    p3=number(r,'RANGE_3H_POSITION_PCT')
    p6=number(r,'RANGE_6H_POSITION_PCT')
    m15=number(r,'MOVE_TRADE_15M_PREATR')
    m30=number(r,'MOVE_TRADE_30M_PREATR')
    m60=number(r,'MOVE_TRADE_60M_PREATR')
    high=number(r,'PRIOR_3H_DIST_HIGH_ATR')
    low=number(r,'PRIOR_3H_DIST_LOW_ATR')
    # All measures come from V10 features known at decision time.
    required=(p3,p6,m15,m30,m60,high,low)
    if any(x is None for x in required):
        return dict(RISK_STATUS='INSUFFICIENT_DATA',RISK_REASONS='missing V10 feature',
                    LATE_LONG_SHOCK='',SHORT_NEAR_LOW='',LONG_REVERSAL='',FLAG='')
    # Hypotheses fixed BEFORE running V11 on the CSV. Do not optimize to outcomes.
    # High location AND unusually strong 60-minute move; no trend inferred from shock.
    late_long=(side=='LONG' and p3>=80 and m60>=2.5)
    # Short close to 3-hour bottom, or near 3-hour bottom and at/below prior support.
    short_low=(side=='SHORT' and p3<=30 and (m60>=1.2 or low<=1.25))
    # Long after an abrupt adverse 30-minute move, or rapid retracement in last 15m.
    # This is a separate "falling knife" watch, not necessarily a losing setup.
    long_reversal=(side=='LONG' and (m30<=-3.0 or (m15<=-2.0 and m60>0)))
    reasons=[]
    if late_long: reasons.append('LONG_AFTER_STRONG_RISE_NEAR_3H_HIGH')
    if short_low: reasons.append('SHORT_NEAR_3H_LOW')
    if long_reversal: reasons.append('LONG_AFTER_FAST_ADVERSE_MOVE')
    return dict(RISK_STATUS='REVIEW' if reasons else 'NO_RULE_TRIGGER',
                RISK_REASONS=';'.join(reasons) if reasons else '-',
                LATE_LONG_SHOCK=int(late_long),SHORT_NEAR_LOW=int(short_low),
                LONG_REVERSAL=int(long_reversal),FLAG=int(bool(reasons)))

def write_csv(path, rows, fields):
    with path.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore')
        w.writeheader();w.writerows(rows)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--input',default=DEFAULT_IN)
    ap.add_argument('--out',default=DEFAULT_OUT)
    args=ap.parse_args()
    source=Path(args.input)
    with source.open(newline='',encoding='utf-8') as f:
        rows=list(csv.DictReader(f))
    if not rows: raise SystemExit('ERROR: V10 report empty')
    needed=['ID','SIDE','HISTORICAL_OUTCOME','GROUP_ROLE',
            'RANGE_3H_POSITION_PCT','RANGE_6H_POSITION_PCT',
            'MOVE_TRADE_15M_PREATR','MOVE_TRADE_30M_PREATR',
            'MOVE_TRADE_60M_PREATR','PRIOR_3H_DIST_HIGH_ATR',
            'PRIOR_3H_DIST_LOW_ATR']
    missing=[k for k in needed if k not in rows[0]]
    if missing: raise SystemExit('ERROR: Missing V10 columns: '+', '.join(missing))
    enriched=[]
    for r in rows:
        item=dict(r);item.update(evaluate(r));enriched.append(item)
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    fields=list(dict.fromkeys(k for r in enriched for k in r))
    write_csv(out/'all_signals.csv',enriched,fields)
    parents=[r for r in enriched if r['GROUP_ROLE']=='PARENT']
    write_csv(out/'parent_signals.csv',parents,fields)
    focus={'159','164','165','166','174','175','176','177','178','179'}
    write_csv(out/'key_comparisons.csv',[r for r in enriched if r['ID'] in focus],fields)
    print('BTC V11 READ ONLY | exploratory, predeclared risk hypotheses')
    print('ID SIDE OUTCOME ROLE POS3H MOVE30 MOVE60 STATUS REASON')
    for r in enriched:
        if r['GROUP_ROLE']=='PARENT':
            print(r['ID'],r['SIDE'],r['HISTORICAL_OUTCOME'],r['GROUP_ROLE'],
                  r['RANGE_3H_POSITION_PCT'],r['MOVE_TRADE_30M_PREATR'],
                  r['MOVE_TRADE_60M_PREATR'],r['RISK_STATUS'],r['RISK_REASONS'])
    eligible=[r for r in parents if r['FLAG']!='']
    flagged=[r for r in eligible if r['FLAG']==1]
    print('\nPARENT AUDIT | total',len(parents),'evaluated',len(eligible),
          'flagged',len(flagged))
    print('Flagged SL',sum(r['HISTORICAL_OUTCOME']=='SL' for r in flagged),
          'flagged TP3',sum(r['HISTORICAL_OUTCOME']=='TP3' for r in flagged))
    print('Unflagged SL',sum(r['HISTORICAL_OUTCOME']=='SL' and r['FLAG']==0 for r in eligible),
          'unflagged TP3',sum(r['HISTORICAL_OUTCOME']=='TP3' and r['FLAG']==0 for r in eligible))
    print('WARNING: this is an IN-SAMPLE descriptive audit, not out-of-sample validation.')
    print('WARNING: these rules are NOT approved to delete or suppress any signal.')
    print('REPORT',out/'all_signals.csv')
    print('REPORT',out/'parent_signals.csv')
    print('REPORT',out/'key_comparisons.csv')
    print('FINISHED | no orders, no DB writes, no service changes')

if __name__=='__main__':
    main()
