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
    "UNIUSDT", "DOTUSDT", "TAOUSDT",
    "WUSDT", "ASTERUSDT", "ORDIUSDT", "ATOMUSDT",
    "INJUSDT", "NEARUSDT", "BCHUSDT", "VELVETUSDT"
]

INTERVAL = os.getenv("INTERVAL", "5m")
CHECK_MINUTES = int(os.getenv("CHECK_MINUTES", "2"))

MIN_SCORE = int(os.getenv("MIN_SCORE", "6"))
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.5"))
MAX_VOLUME_RATIO = float(os.getenv("MAX_VOLUME_RATIO", "5.0"))
MAX_LONG_RSI = float(os.getenv("MAX_LONG_RSI", "65"))
MIN_SHORT_RSI = float(os.getenv("MIN_SHORT_RSI", "35"))
BREAKOUT_BUFFER_ATR = float(os.getenv("BREAKOUT_BUFFER_ATR", "0.08"))
MAX_EXTENSION_ATR = float(os.getenv("MAX_EXTENSION_ATR", "0.65"))
MAX_BODY_ATR = float(os.getenv("MAX_BODY_ATR", "1.4"))
STOP_ATR_MULT = float(os.getenv("STOP_ATR_MULT", "1.4"))
MAX_RISK_PCT = float(os.getenv("MAX_RISK_PCT", "1.2"))
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", "90"))
SEND_NO_SIGNAL_STATUS = os.getenv("SEND_NO_SIGNAL_STATUS", "false").lower() == "true"

LBANK_BASE_URLS = [
    "https://api.lbkex.com",
    "https://www.lbkex.net"
]

app = Flask(__name__)
sent_signals = {}

no_signal_count = 0

@app.route("/")
def home():
    return "Crypto Scalp Signal Bot V4 Experimental LBank is running ✅"


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
        "3m": "minute3",
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
            "volume": float(row.get("volume") or row.get("vol") or row.get("v") or 0),
            "timestamp": float(row.get("timestamp") or row.get("time") or row.get("date") or row.get("t") or 0)
        }

    if isinstance(row, (list, tuple)) and len(row) >= 6:
        return {
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "timestamp": float(row[0])
        }

    raise ValueError(f"Unknown kline row format: {row}")


def get_klines(symbol, interval="5m", limit=300):
    pair = to_lbank_symbol(symbol)
    k_type = lbank_interval(interval)

    seconds_map = {
        "1m": 60,
        "3m": 180,
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

    # Sort by exchange timestamps and reject unfinished candles. LBank may use seconds or milliseconds.
    candles.sort(key=lambda c: c.get("timestamp", 0))
    now = time.time()
    complete = []
    for c in candles:
        ts = c.get("timestamp", 0)
        if ts > 1e11:
            ts /= 1000
        if ts and ts + seconds > now - 2:
            continue
        complete.append(c)
    return complete[-limit:]


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
    e100 = ema(closes, 100)

    if not all([e20, e50, e100]):
        return "NEUTRAL"

    last = closes[-1]

    if e20 > e50 and last > e20 and last > e100:
        return "BULLISH"

    if e20 < e50 and last < e20 and last < e100:
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
        strength = len([v for v in values if v < 20]) / len(values)
        log("STOCH LONG", values[-5:], strength)
        return strength >= 0.60, round(strength * 100, 1)

    if side == "SHORT":
        strength = len([v for v in values if v > 80]) / len(values)
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
    """V4 conservative, closed-candle breakout strategy (experimental)."""
    try:
        candles = get_klines(symbol, INTERVAL, 160)
        candles_1m = get_klines(symbol, "1m", 160)
        if len(candles) < 110 or len(candles_1m) < 110:
            return None

        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]
        volumes = [c["volume"] for c in candles]
        price = closes[-1]
        prev_price = closes[-2]
        last = candles[-1]
        a = atr(candles, 14)
        r = rsi(closes, 14)
        if not a or a <= 0 or r is None or prev_price <= 0:
            return None

        trend5 = market_trend(candles)
        trend1 = market_trend(candles_1m)
        recent_high = max(highs[-22:-1])
        recent_low = min(lows[-22:-1])
        vol_base = average(volumes[-31:-1])
        vol_ratio = volumes[-1] / vol_base if vol_base > 0 else 0
        momentum = 100 * (price - prev_price) / prev_price
        body = abs(last["close"] - last["open"])
        if not (MIN_VOLUME_RATIO <= vol_ratio <= MAX_VOLUME_RATIO):
            return None
        if body > MAX_BODY_ATR * a:
            return None
        if candle_body_ratio(last) < 0.45:
            return None

        # Require confirmed close beyond prior resistance/support, but avoid chasing extended moves.
        long_break = (last["close"] > recent_high + BREAKOUT_BUFFER_ATR * a and
                      last["close"] > last["open"] and
                      (last["close"] - recent_high) <= MAX_EXTENSION_ATR * a)
        short_break = (last["close"] < recent_low - BREAKOUT_BUFFER_ATR * a and
                       last["close"] < last["open"] and
                       (recent_low - last["close"]) <= MAX_EXTENSION_ATR * a)
        if trend5 == "BULLISH" and trend1 == "BULLISH" and long_break and 50 <= r <= MAX_LONG_RSI and momentum > 0:
            side = "LONG"
        elif trend5 == "BEARISH" and trend1 == "BEARISH" and short_break and MIN_SHORT_RSI <= r <= 50 and momentum < 0:
            side = "SHORT"
        else:
            return None

        # Opposing BTC bias is a veto rather than merely subtracting one point.
        if symbol != "BTCUSDT" and ((side == "LONG" and btc_market_bias == "BEARISH") or
                                     (side == "SHORT" and btc_market_bias == "BULLISH")):
            return None

        # Transparent seven-condition score, not a capped sum of nine conditions.
        checks = [
            trend5 == ("BULLISH" if side == "LONG" else "BEARISH"),
            trend1 == ("BULLISH" if side == "LONG" else "BEARISH"),
            long_break if side == "LONG" else short_break,
            candle_body_ratio(last) >= 0.45,
            MIN_VOLUME_RATIO <= vol_ratio <= MAX_VOLUME_RATIO,
            (50 <= r <= MAX_LONG_RSI) if side == "LONG" else (MIN_SHORT_RSI <= r <= 50),
            momentum > 0 if side == "LONG" else momentum < 0,
        ]
        score = sum(checks)
        if score < MIN_SCORE:
            return None

        # Stop behind local structure, with a volatility floor and a max-risk filter.
        entry = price
        if side == "LONG":
            stop = min(min(lows[-7:-1]) - 0.10 * a, entry - STOP_ATR_MULT * a)
            risk = entry - stop
            target1, target2 = entry + 1.5 * risk, entry + 2.5 * risk
        else:
            stop = max(max(highs[-7:-1]) + 0.10 * a, entry + STOP_ATR_MULT * a)
            risk = stop - entry
            target1, target2 = entry - 1.5 * risk, entry - 2.5 * risk
        if risk <= 0 or 100 * risk / entry > MAX_RISK_PCT:
            return None

        p = precision(entry)
        candle_id = int(last.get("timestamp", 0))
        reasons = [
            "روندهای ۵ و ۱ دقیقه هم‌جهت هستند.",
            "شکست با بسته‌شدن کندل تأیید شده است.",
            f"حجم {vol_ratio:.2f} برابر میانگین و در محدوده مجاز است.",
            f"RSI: {r:.1f}؛ مومنتوم: {momentum:.2f}%",
            "فاصله شکست و اندازه کندل با ATR کنترل شده است.",
            "حد ضرر بر اساس ATR و ساختار اخیر قیمت است.",
        ]
        return {
            "id": f"{symbol}-{side}-{candle_id}",
            "symbol": symbol, "side": side, "score": score,
            "grade": "A+ ⭐⭐⭐⭐⭐" if score == 7 else "A ⭐⭐⭐⭐",
            "entry": round(entry, p), "stop": round(stop, p),
            "target1": round(target1, p), "target2": round(target2, p),
            "rsi": round(r, 2), "rr": 1.5,
            "volume_ratio": round(vol_ratio, 2), "reasons": reasons,
            "time": now_text(), "setup_time": candle_id,
        }
    except Exception as e:
        log(f"{symbol} analyze error:", e)
        return None

def format_signal(s):
    emoji = "🟢" if s["side"] == "LONG" else "🔴"

    reasons = "\n".join([f"• {x}" for x in s["reasons"]])

    return f"""
🚨 سیگنال اسکلپ نسخه ۴ آزمایشی - LBank

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
    now_ts = time.time()

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
                last_sent = sent_signals.get(f"cooldown:{symbol}", 0)
                if now_ts - last_sent >= SIGNAL_COOLDOWN_MINUTES * 60:
                    found.append(signal)

        except Exception as e:
            log(f"{symbol} error:", e)

    found.sort(key=lambda x: x["score"], reverse=True)

    if not found:
        no_signal_count += 1
        log(f"No signal found, scans={no_signal_count}")
        if SEND_NO_SIGNAL_STATUS and no_signal_count % max(1, int(60 / max(CHECK_MINUTES, 1))) == 0:
            telegram_send("🤖 ربات نسخه ۴ فعال است؛ در بررسی اخیر سیگنال معتبری پیدا نشد.")
        return


    for s in found[:3]:
        try:
            log(f"SENDING {s['symbol']} {s['side']} SCORE={s['score']}")

            if telegram_send(format_signal(s)):
                sent_signals[s["id"]] = time.time()
                sent_signals[f"cooldown:{s['symbol']}"] = time.time()
                log(f"Signal sent: {s['symbol']} {s['side']}")
            else:
                log(f"Signal NOT delivered: {s['symbol']}")

        except Exception as e:
            log("Telegram send signal error:", e)

    cutoff = time.time() - 86400

    for key, ts in list(sent_signals.items()):
        if ts < cutoff:
            del sent_signals[key]

    log("SCAN FINISHED")


def run_scheduler():
    log("Scheduler Started")
    log("LBank V4 experimental Started")

    while True:
        try:
            log(f"SCAN START {now_text()}")
            scan_market()
            log(f"SCAN END {now_text()}")
        except Exception as e:
            log("Scheduler loop error:", e)

        time.sleep(CHECK_MINUTES * 60)


if __name__ == "__main__":
    threading.Thread(target=run_scheduler, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
