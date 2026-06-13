import os
import time
import threading
import requests
from datetime import datetime, timezone
from flask import Flask
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
    "XRPUSDT", "ENAUSDT", "DOGEUSDT", "LINKUSDT",
    "AVAXUSDT", "TRXUSDT", "WIFUSDT", "ICPUSDT",
    "APTUSDT", "SUIUSDT", "ETCUSDT", "HYPEUSDT",
    "UNIUSDT", "DOTUSDT", "UNIUSDT", "TAOUSDT",
    "WUSDT", "WLDUSDT", "ASTERUSDT", "ZECUSDT",
    "INJUSDT", "NEARUSDT", "ORDIUSDT", "ATOMUSDT",
    "BCHUSDT"
]

INTERVAL = os.getenv("INTERVAL", "5m")
CHECK_MINUTES = int(os.getenv("CHECK_MINUTES", "5"))

MIN_SCORE = 4
MIN_VOLUME_RATIO = 1.5
MAX_LONG_RSI = 75
MIN_SHORT_RSI = 40

LBANK_BASE_URLS = [
    "https://api.lbkex.com",
    "https://www.lbkex.net"
]

app = Flask(__name__)
sent_signals = {}

no_signal_count = 0

@app.route("/")
def home():
    return "Crypto Scalp Signal Bot V3 LBank is running ✅"


def log(*args):
    print(*args, flush=True)


def now_text():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def telegram_send(text):
    if not BOT_TOKEN or not CHAT_ID:
        log("Telegram missing BOT_TOKEN or CHAT_ID")
        return False

    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        r = requests.post(
            url,
            json={
                "chat_id": CHAT_ID,
                "text": text,
                "disable_web_page_preview": True
            },
            timeout=(3, 8)
        )
        log("TG STATUS:", r.status_code)
        if r.status_code != 200:
            log("TG RESPONSE:", r.text[:300])
        return r.status_code == 200
    except Exception as e:
        log("Telegram error:", e)
        return False


def to_lbank_symbol(symbol):
    s = symbol.upper().replace("/", "").replace("-", "").replace("_", "")
    if s.endswith("USDT"):
        return s[:-4].lower() + "_usdt"
    return s.lower()


def lbank_interval(interval):
    mapping = {
        "1m": "minute1",
        "5m": "minute5",
        "15m": "minute15",
        "30m": "minute30",
        "1h": "hour1",
        "4h": "hour4",
        "8h": "hour8",
        "12h": "hour12",
        "1d": "day1",
        "1w": "week1"
    }
    return mapping.get(interval, "minute5")


def lbank_get(path, params):
    last_error = None

    for base in LBANK_BASE_URLS:
        try:
            url = base + path
            log("LBANK TRY:", url, params)

            r = requests.get(
                url,
                params=params,
                timeout=(3, 10)
            )

            log("LBANK STATUS:", r.status_code)

            try:
                data = r.json()
            except Exception:
                last_error = r.text[:300]
                log("LBANK NON JSON:", last_error)
                continue

            if r.status_code != 200:
                last_error = data
                continue

            if isinstance(data, dict):
                if data.get("result") == "false" or "error_code" in data:
                    last_error = data
                    continue

            return data

        except Exception as e:
            last_error = e
            log("LBANK ERROR:", base, e)

    raise RuntimeError(f"LBank failed: {last_error}")


def parse_lbank_kline_row(row):
    """
    LBank spot v1 kline is commonly:
    [timestamp, open, high, low, close, volume]
    Some mirrors may return dicts. This parser supports both.
    """
    if isinstance(row, dict):
        return {
            "open": float(row.get("open") or row.get("o")),
            "high": float(row.get("high") or row.get("h")),
            "low": float(row.get("low") or row.get("l")),
            "close": float(row.get("close") or row.get("c")),
            "volume": float(row.get("volume") or row.get("vol") or row.get("v") or 0)
        }

    if isinstance(row, (list, tuple)) and len(row) >= 6:
        return {
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5])
        }

    raise ValueError(f"Unknown kline row format: {row}")


def get_klines(symbol, interval="5m", limit=300):
    pair = to_lbank_symbol(symbol)
    k_type = lbank_interval(interval)

    seconds_map = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400
    }

    seconds = seconds_map.get(interval, 300)
    start_time = int(time.time()) - (int(limit) * seconds)

    data = lbank_get("/v1/kline.do", {
        "symbol": pair,
        "type": k_type,
        "size": min(int(limit), 1000),
        "time": start_time
    })

    if isinstance(data, dict) and "data" in data:
        data = data["data"]

    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected LBank kline response for {symbol}: {str(data)[:300]}")

    candles = []

    for row in data:
        try:
            c = parse_lbank_kline_row(row)
            if c["high"] >= c["low"] and c["close"] > 0:
                candles.append(c)
        except Exception as e:
            log("KLINE PARSE ERROR:", symbol, row, e)

    log(symbol, interval, "candles loaded:", len(candles))

    return candles[-limit:]


def average(values):
    return sum(values) / len(values) if values else 0


def ema(values, period):
    if len(values) < period:
        return None

    k = 2 / (period + 1)
    result = sum(values[:period]) / period

    for v in values[period:]:
        result = (v - result) * k + result

    return result


def rsi(values, period=14):
    if len(values) <= period:
        return None

    gains, losses = [], []

    for i in range(1, period + 1):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    for i in range(period + 1, len(values)):
        diff = values[i] - values[i - 1]
        gain = max(diff, 0)
        loss = abs(min(diff, 0))

        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(candles, period=14):
    if len(candles) <= period:
        return None

    trs = []

    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        trs.append(tr)

    return average(trs[-period:])


def precision(price):
    if price >= 1000:
        return 2
    if price >= 1:
        return 4
    return 6


def market_trend(candles):
    closes = [c["close"] for c in candles]

    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    e200 = ema(closes, 200)

    if not all([e20, e50, e200]):
        return "NEUTRAL"

    last = closes[-1]

    if e20 > e50 > e200 and last > e20:
        return "BULLISH"

    if e20 < e50 < e200 and last < e20:
        return "BEARISH"

    return "NEUTRAL"


def btc_bias():
    try:
        log("BTC BIAS START")
        candles = get_klines("BTCUSDT", "15m", 250)

        if len(candles) < 220:
            log("BTC BIAS not enough candles:", len(candles))
            return "NEUTRAL"

        bias = market_trend(candles)
        log("BTC BIAS:", bias)

        return bias

    except Exception as e:
        log("BTC BIAS ERROR:", e)
        return "NEUTRAL"


def stoch_rsi_value(closes, rsi_period=14, stoch_length=7):
    if len(closes) < rsi_period + stoch_length + 5:
        return None

    rsi_values = []

    for i in range(rsi_period + 1, len(closes) + 1):
        value = rsi(closes[:i], rsi_period)
        if value is not None:
            rsi_values.append(value)

    if len(rsi_values) < stoch_length:
        return None

    recent = rsi_values[-stoch_length:]
    lowest = min(recent)
    highest = max(recent)

    if highest == lowest:
        return 50

    return ((rsi_values[-1] - lowest) / (highest - lowest)) * 100


def stoch_xy_confirm(closes, side):
    fib_lengths = [
        6, 7, 8, 9, 11, 13, 15, 18, 21, 25,
        30, 36, 43, 51, 60, 70, 82, 95,
        110, 126, 143, 161, 180, 200,
        220, 240, 260, 280
    ]

    values = []

    for length in fib_lengths:
        if len(closes) > length + 20:
            val = stoch_rsi_value(closes[-(length + 30):], 2, length)
            if val is not None:
                values.append(val)

    if not values:
        return False, 0

    if side == "LONG":
        strength = len([v for v in values if 50 < v < 80]) / len(values)
        log("STOCH LONG", values[-5:], strength)
        return strength >= 0.60, round(strength * 100, 1)

    if side == "SHORT":
        strength = len([v for v in values if 20 < v < 50]) / len(values)
        log("STOCH SHORT", values[-5:], strength)
        return strength >= 0.60, round(strength * 100, 1)

    return False, 0


def tmco_confirm(closes, side):
    if len(closes) < 20:
        return False

    line = ema(closes[-10:], 2)
    wave = ema(closes[-10:], 3)

    prev_line = ema(closes[-11:-1], 2)
    prev_wave = ema(closes[-11:-1], 3)

    if not all([line, wave, prev_line, prev_wave]):
        return False

    if side == "LONG":
        return wave > line and wave > prev_wave

    if side == "SHORT":
        return wave < line and wave < prev_wave

    return False


def candle_body_ratio(candle):
    high = candle["high"]
    low = candle["low"]
    body = abs(candle["close"] - candle["open"])

    if high == low:
        return 0

    return body / (high - low)


def analyze_symbol(symbol, btc_market_bias):
    try:
        candles = get_klines(symbol, INTERVAL, 300)
        candles_15m = get_klines(symbol, "15m", 250)

        if len(candles) < 80 or len(candles_15m) < 80:
            log(symbol, "not enough candles", len(candles), len(candles_15m))
            return None

        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]
        volumes = [c["volume"] for c in candles]

        price = closes[-1]
        prev_price = closes[-2]

        last_candle = candles[-1]
        prev_candle = candles[-2]

        r = rsi(closes, 14)
        a = atr(candles, 14)

        if r is None or a is None:
            return None

        trend_5m = market_trend(candles)
        trend_15m = market_trend(candles_15m)

        recent_high = max(highs[-35:-2])
        recent_low = min(lows[-35:-2])

        avg_vol = average(volumes[-30:-1])
        volume_ratio = volumes[-1] / avg_vol if avg_vol > 0 else 0

        momentum = ((price - prev_price) / prev_price) * 100

        score = 0
        reasons = []
        side = None

        body_ok = candle_body_ratio(last_candle) >= 0.35

        breakout_long = price > recent_high
        near_breakout_long = price >= recent_high * 0.999
        confirm_long = last_candle["close"] > last_candle["open"] and body_ok

        breakout_short = price < recent_low
        near_breakout_short = price <= recent_low * 1.001
        confirm_short = last_candle["close"] < last_candle["open"] and body_ok

        log(
            symbol,
            f"RSI={round(r,1)} "
            f"VOL={round(volume_ratio,2)} "
            f"MOM={round(momentum,2)} "
            f"T5={trend_5m} "
            f"T15={trend_15m} "
            f"BL={breakout_long} "
            f"NBL={near_breakout_long} "
            f"BS={breakout_short} "
            f"NBS={near_breakout_short}"
        )

        long_candidate = (
            (breakout_long and confirm_long) or
            (near_breakout_long and momentum > 0 and r >= 50) or
            (trend_5m == "BULLISH" and momentum > 0.05 and r >= 52)
        )

        short_candidate = (
            (breakout_short and confirm_short) or
            (near_breakout_short and momentum < 0 and r <= 50) or
            (trend_5m == "BEARISH" and momentum < -0.05 and r <= 48)
        )

        if long_candidate and not short_candidate:
            side = "LONG"
        elif short_candidate and not long_candidate:
            side = "SHORT"
        elif long_candidate and short_candidate:
            if momentum >= 0:
                side = "LONG"
            else:
                side = "SHORT"

        if not side:
            log(symbol, "NO SIDE")
            return None

        stoch_ok, stoch_strength = stoch_xy_confirm(closes, side)
        tmco_ok = tmco_confirm(closes, side)

        if side == "LONG":
            if r > MAX_LONG_RSI:
                log(symbol, "LONG rejected: RSI too high", r)
                return None

            if volume_ratio < MIN_VOLUME_RATIO:
                log(symbol, "LONG rejected: low volume", volume_ratio)
                return None

            if btc_market_bias == "BEARISH" and symbol != "BTCUSDT":
                score -= 1
                reasons.append("جهت BTC کمی مخالف است.")

            if trend_5m == "BULLISH":
                score += 1
                reasons.append("روند ۵ دقیقه صعودی است.")

            if trend_15m == "BULLISH":
                score += 1
                reasons.append("روند ۱۵ دقیقه صعودی است.")

            if breakout_long:
                score += 1
                reasons.append("شکست مقاومت دیده شد.")
            elif near_breakout_long:
                score += 1
                reasons.append("قیمت نزدیک مقاومت و در حال فشار صعودی است.")

            if confirm_long:
                score += 1
                reasons.append("کندل تاییدی صعودی است.")

            if volume_ratio >= MIN_VOLUME_RATIO:
                score += 1
                reasons.append(f"حجم معاملات {volume_ratio:.2f} برابر میانگین است.")

            if r >= 50:
                score += 1
                reasons.append(f"RSI مناسب لانگ است: {r:.1f}")

            if momentum > 0:
                score += 1
                reasons.append(f"مومنتوم مثبت است: {momentum:.2f}%")

            if stoch_ok:
                score += 1
                reasons.append(f"Stoch X/Y تایید لانگ داد. قدرت: {stoch_strength}%")

            if tmco_ok:
                score += 1
                reasons.append("TMCO تایید لانگ داد.")

            entry = price
            stop = min(last_candle["low"], entry - (a * 1.2))
            risk = entry - stop

            if risk <= 0:
                return None

            target1 = entry + (risk * 1.5)
            target2 = entry + (risk * 2.5)

        else:
            if r < MIN_SHORT_RSI:
                log(symbol, "SHORT rejected: RSI too low", r)
                return None

            if volume_ratio < MIN_VOLUME_RATIO:
                log(symbol, "SHORT rejected: low volume", volume_ratio)
                return None
                
            if btc_market_bias == "BULLISH" and symbol != "BTCUSDT":
                score -= 1
                reasons.append("جهت BTC کمی مخالف است.")

            if trend_5m == "BEARISH":
                score += 1
                reasons.append("روند ۵ دقیقه نزولی است.")

            if trend_15m == "BEARISH":
                score += 1
                reasons.append("روند ۱۵ دقیقه نزولی است.")

            if breakout_short:
                score += 1
                reasons.append("شکست حمایت دیده شد.")
            elif near_breakout_short:
                score += 1
                reasons.append("قیمت نزدیک حمایت و در حال فشار نزولی است.")

            if confirm_short:
                score += 1
                reasons.append("کندل تاییدی نزولی است.")

            if volume_ratio >= MIN_VOLUME_RATIO:
                score += 1
                reasons.append(f"حجم معاملات {volume_ratio:.2f} برابر میانگین است.")

            if r <= 50:
                score += 1
                reasons.append(f"RSI مناسب شورت است: {r:.1f}")

            if momentum < 0:
                score += 1
                reasons.append(f"مومنتوم منفی است: {momentum:.2f}%")

            if stoch_ok:
                score += 1
                reasons.append(f"Stoch X/Y تایید شورت داد. قدرت: {stoch_strength}%")

            if tmco_ok:
                score += 1
                reasons.append("TMCO تایید شورت داد.")

            entry = price
            stop = max(last_candle["high"], entry + (a * 1.2))
            risk = stop - entry

            if risk <= 0:
                return None

            target1 = entry - (risk * 1.5)
            target2 = entry - (risk * 2.5)

        log(symbol, f"SCORE={score} SIDE={side}")
            
        if score < MIN_SCORE:
            return None

        rr = abs(target1 - entry) / abs(entry - stop)

        if rr < 1.5:
            return None

        score = min(score, 7)
        
        p = precision(entry)

        grade = "A+ ⭐⭐⭐⭐⭐" if score >= 6 else "A ⭐⭐⭐⭐"

        return {
            "id": f"{symbol}-{side}-{round(entry, p)}",
            "symbol": symbol,
            "side": side,
            "score": score,
            "grade": grade,
            "entry": round(entry, p),
            "stop": round(stop, p),
            "target1": round(target1, p),
            "target2": round(target2, p),
            "rsi": round(r, 2),
            "rr": round(rr, 2),
            "volume_ratio": round(volume_ratio, 2),
            "reasons": reasons,
            "time": now_text()
        }

    except Exception as e:
        log(f"{symbol} analyze error:", e)
        return None

def format_signal(s):
    emoji = "🟢" if s["side"] == "LONG" else "🔴"

    reasons = "\n".join([f"• {x}" for x in s["reasons"]])

    return f"""
🚨 سیگنال اسکلپ نسخه ۳ - LBank

{emoji} نوع معامله: {s["side"]}
🪙 ارز: {s["symbol"]}
⏱ تایم‌فریم: {INTERVAL}

📍 ورود:
{s["entry"]}

🎯 تارگت ۱:
{s["target1"]}

🎯 تارگت ۲:
{s["target2"]}

🛑 حد ضرر:
{s["stop"]}

🔥 کیفیت سیگنال:
{s["grade"]}

📊 امتیاز:
{s["score"]}/7

📈 RSI:
{s["rsi"]}

📦 حجم:
{s["volume_ratio"]} برابر میانگین

⚖️ Risk / Reward:
1:{s["rr"]}

🧠 دلایل:
{reasons}

🕒 زمان:
{s["time"]}

⚠️ این سیگنال توصیه مالی قطعی نیست. حتماً مدیریت سرمایه، حد ضرر و شرایط بازار را بررسی کن.
"""


def scan_market():
    global no_signal_count
    
    log("SCAN STEP 1 - scan_market started")

    found = []

    try:
        log("SCAN STEP 2 - getting BTC bias")
        market_bias = btc_bias()
        log("SCAN STEP 3 - BTC bias:", market_bias)
    except Exception as e:
        log("BTC bias error:", e)
        market_bias = "NEUTRAL"

    log("SCAN STEP 4 - checking symbols")

    for symbol in SYMBOLS:
        try:
            log(f"Checking {symbol}")

            signal = analyze_symbol(symbol, market_bias)

            if signal:
                log(f"Signal found: {symbol} score={signal.get('score')}")
            else:
                log(f"No signal for {symbol}")

            if signal and signal["id"] not in sent_signals:
                found.append(signal)

        except Exception as e:
            log(f"{symbol} error:", e)

    found.sort(key=lambda x: x["score"], reverse=True)

    if not found:
               no_signal_count += 1

               log(f"No signal count={no_signal_count}")
               log("No strong signal found.")

               if no_signal_count >= 12:
                   telegram_send(
            "🤖 ربات فعال است\n\n"
            "❌ در یک ساعت گذشته سیگنال معتبری پیدا نشد."
        )
               no_signal_count = 0
    
               log("SCAN FINISHED - no signal")
               return

    for s in found[:3]:
        try:
            log(f"SENDING {s['symbol']} {s['side']} SCORE={s['score']}")

            sent_signals[s["id"]] = time.time()
            telegram_send(format_signal(s))

            log(f"Signal sent: {s['symbol']} {s['side']}")

        except Exception as e:
            log("Telegram send signal error:", e)

    cutoff = time.time() - 86400

    for key, ts in list(sent_signals.items()):
        if ts < cutoff:
            del sent_signals[key]

    log("SCAN FINISHED")


def run_scheduler():
    log("Scheduler Started")
    log("LBank V3 Started")

    while True:
        try:
            log(f"SCAN START {now_text()}")
            scan_market()
            log(f"SCAN END {now_text()}")
        except Exception as e:
            log("Scheduler loop error:", e)

        time.sleep(CHECK_MINUTES * 60)


if __name__ == "__main__":
    threading.Thread(target=run_scheduler).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
