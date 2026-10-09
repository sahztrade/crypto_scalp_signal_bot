#!/usr/bin/env python3
"""TMCO 2244 independent closed-candle detector; DRY RUN ONLY.

No DB writes, Telegram messages, orders, or service changes.
Run with: /usr/bin/python3 -u tmco_2244_producer.py
"""
import argparse
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from bybit_futures_v4 import fetch
from tmco_2244_backtest import crossings

TIMEFRAMES = ('1m', '3m', '5m')
SECONDS = {'1m': 60, '3m': 180, '5m': 300}
LOG = logging.getLogger('tmco_2244_producer')


def iso_utc(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def detect(tf, now, count):
    candles = fetch(tf, now, count)
    # A bar's timestamp is its OPEN; crossings() emits its CLOSE epoch.
    events = crossings(candles, tf)
    return candles[-1]['timestamp'] + SECONDS[tf], events


def main():
    parser = argparse.ArgumentParser(description='Read-only TMCO 2244 crossing detector')
    parser.add_argument('--count', type=int, default=300)
    parser.add_argument('--poll', type=float, default=1.0)
    parser.add_argument('--max-lateness', type=float, default=15.0)
    args = parser.parse_args()
    if not 50 <= args.count <= 900 or not 0.25 <= args.poll <= 10:
        parser.error('count must be 50..900 and poll 0.25..10 seconds')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    # Ignore every bar already closed at startup, including any during initial warm-up.
    startup = int(time.time())
    last_processed = {tf: (startup // SECONDS[tf]) * SECONDS[tf] for tf in TIMEFRAMES}
    LOG.info('DRY RUN ONLY; no writes or alerts. startup=%s', iso_utc(startup))
    with ThreadPoolExecutor(max_workers=3) as pool:
        while True:
            now = time.time()
            # Wait until just after the candle boundary to avoid a request at the boundary.
            due = [tf for tf in TIMEFRAMES if (int(now) // SECONDS[tf]) * SECONDS[tf] > last_processed[tf]]
            if not due:
                time.sleep(args.poll)
                continue
            decision = int(now)
            futures = {pool.submit(detect, tf, decision, args.count): tf for tf in due}
            batch = []
            for future in as_completed(futures):
                tf = futures[future]
                try:
                    latest_close, events = future.result()
                except Exception:
                    LOG.exception('Fetch/calculation failed tf=%s; will retry', tf)
                    continue
                # Catch-up is informational only. No historical crosses are emitted.
                expected_close = (decision // SECONDS[tf]) * SECONDS[tf]
                if latest_close != expected_close:
                    LOG.warning('Unexpected last closed candle tf=%s expected=%s got=%s', tf, expected_close, latest_close)
                    continue
                recent = [e for e in events if last_processed[tf] < e['time'] <= latest_close]
                for event in recent:
                    age = time.time() - event['time']
                    if age < 0 or age > args.max_lateness:
                        LOG.warning('SKIP LATE tf=%s side=%s close=%s age=%.2fs', tf, event['direction'], iso_utc(event['time']), age)
                    else:
                        batch.append(event)
                last_processed[tf] = latest_close
            # Sort by candle close, then stable TF order, as expected by the existing engine.
            batch.sort(key=lambda e: (e['time'], TIMEFRAMES.index(e['tf'])))
            for event in batch:
                tf = event['tf']
                bar_open = event['time'] - SECONDS[tf]
                LOG.info('DRY CROSS symbol=BTCUSDT side=%s tf=%s bar_time=%s close=%s line=%.6f wave=%.6f',
                         event['direction'], tf, iso_utc(bar_open), iso_utc(event['time']), event['line'], event['wave'])


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nStopped; no live changes.', flush=True)
