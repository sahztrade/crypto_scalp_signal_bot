import os, time, threading, requests
from datetime import datetime, timezone
from flask import Flask
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
    "XRPUSDT", "ADAUSDT", "DOGEUSDT", "LINKUSDT",
    "AVAXUSDT", "TRXUSDT"
]

INTERVAL = os.getenv("INTERVAL", "5m")
CHECK_MINUTES = int(os.getenv("CHECK_MINUTES", "5"))

MIN_SCORE = 5
MIN_RR = 1.8
LONG_RSI_MIN = 58
SHORT_RSI_MAX = 42
VOLUME_RATIO_MIN = 1.30

app = Flask(__name__)
sent_signals = {}

BINANCE_URLS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com"
]


@app.route("/")
def home():
    return "Crypto Scalp Signal Bot V2 is running ✅"


def now_text():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def telegram_send(text):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(
            url,
            json={
                "chat_id": CHAT_ID,
                "text": text,
                "disable_web_page_preview": True
            },
            timeout=20
        )
    except Exception as e:
        print("Telegram error:", e)


def binance_get(path, params):
    last_error = None

    for base in BINANCE_URLS:
        try:
            r = requests.get(base + path, params=params, timeout=15)
            data = r.json()

            if r.status_code == 200 and not (isinstance(data, dict) and "code" in data):
                return data

            last_error = data
        except Exception as e:
            last_error = e

    raise RuntimeError(f"Binance failed: {last_error}")


def get_klines(symbol, interval="5m", limit=150):
    data = binance_get("/api/v3/klines", {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    })

    candles = []
    for x in data:
        candles.append({
            "open": float(x[1]),
            "high": float(x[2]),
            "low": float(x[3]),
            "close": float(x[4]),
            "volume": float(x[5])
        })

    return candles


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

    return sum(trs[-period:]) / period


def average(values):
    return sum(values) / len(values) if values else 0


def precision(price):
    if price >= 1000:
        return 2
    if price >= 1:
        return 4
    return 6


def btc_bias():
    candles = get_klines("BTCUSDT", INTERVAL, 120)
    closes = [c["close"] for c in candles]

    e9 = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)

    last = closes[-1]

    if e9 > e21 > e50 and last > e21:
        return "BULLISH"

    if e9 < e21 < e50 and last < e21:
        return "BEARISH"

    return "NEUTRAL"


def analyze_symbol(symbol, market_bias):
    candles = get_klines(symbol, INTERVAL, 150)

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]

    price = closes[-1]
    prev_price = closes[-2]

    e9 = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)
    r = rsi(closes, 14)
    a = atr(candles, 14)

    if not all([e9, e21, e50, r, a]):
        return None

    recent_high = max(highs[-30:-1])
    recent_low = min(lows[-30:-1])

    breakout = price > recent_high
    breakdown = price < recent_low

    avg_vol = average(volumes[-25:-1])
    volume_ratio = volumes[-1] / avg_vol if avg_vol > 0 else 0
    volume_spike = volume_ratio >= VOLUME_RATIO_MIN

    momentum = ((price - prev_price) / prev_price) * 100

    long_score = 0
    short_score = 0
    long_reasons = []
    short_reasons = []

    if e9 > e21 > e50:
        long_score += 1
        long_reasons.append("روند EMA صعودی است.")

    if e9 < e21 < e50:
        short_score += 1
        short_reasons.append("روند EMA نزولی است.")

    if r >= LONG_RSI_MIN:
        long_score += 1
        long_reasons.append(f"RSI مناسب لانگ است: {r:.1f}")

    if r <= SHORT_RSI_MAX:
        short_score += 1
        short_reasons.append(f"RSI مناسب شورت است: {r:.1f}")

    if breakout:
        long_score += 2
        long_reasons.append("شکست مقاومت ۳۰ کندل اخیر دیده شد.")

    if breakdown:
        short_score += 2
        short_reasons.append("شکست حمایت ۳۰ کندل اخیر دیده شد.")

    if volume_spike:
        long_score += 1
        short_score += 1
        long_reasons.append(f"حجم معاملات {volume_ratio:.2f} برابر میانگین است.")
        short_reasons.append(f"حجم معاملات {volume_ratio:.2f} برابر میانگین است.")

    if momentum > 0.12:
        long_score += 1
        long_reasons.append(f"مومنتوم مثبت است: {momentum:.2f}%")

    if momentum < -0.12:
        short_score += 1
        short_reasons.append(f"مومنتوم منفی است: {momentum:.2f}%")

    if long_score >= short_score:
        side = "LONG"
        score = long_score
        reasons = long_reasons
    else:
        side = "SHORT"
        score = short_score
        reasons = short_reasons

    if score < MIN_SCORE:
        return None

    if side == "LONG":
        if r < LONG_RSI_MIN or not breakout or not volume_spike:
            return None

        if symbol != "BTCUSDT" and market_bias == "BEARISH":
            return None

        entry = price
        stop = min(recent_low, entry - (a * 1.1))
        risk = entry - stop

        target1 = entry + (risk * MIN_RR)
        target2 = entry + (risk * 2.6)

    else:
        if r > SHORT_RSI_MAX or not breakdown or not volume_spike:
            return None

        if symbol != "BTCUSDT" and market_bias == "BULLISH":
            return None

        entry = price
        stop = max(recent_high, entry + (a * 1.1))
        risk = stop - entry

        target1 = entry - (risk * MIN_RR)
        target2 = entry - (risk * 2.6)

    if risk <= 0:
        return None

    rr = abs(target1 - entry) / abs(entry - stop)

    if rr < MIN_RR:
        return None

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


def format_signal(s):
    emoji = "🟢" if s["side"] == "LONG" else "🔴"
    reasons = "\n".join([f"• {x}" for x in s["reasons"]])

    return f"""
🚨 سیگنال اسکلپ نسخه ۲

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
{s["score"]}/6

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

⚠️ این سیگنال توصیه مالی قطعی نیست. حتماً مدیریت سرمایه و حد ضرر را رعایت کن.
"""


def scan_market():
    print("Scanning market V2...")
    found = []

    try:
        market_bias = btc_bias()
    except Exception as e:
        print("BTC bias error:", e)
        market_bias = "NEUTRAL"

    for symbol in SYMBOLS:
        try:
            signal = analyze_symbol(symbol, market_bias)

            if signal and signal["id"] not in sent_signals:
                found.append(signal)

        except Exception as e:
            print(f"{symbol} error:", e)

    found.sort(key=lambda x: x["score"], reverse=True)

    if not found:
        print("No strong signal found.")
        return

    for s in found[:3]:
        sent_signals[s["id"]] = time.time()
        telegram_send(format_signal(s))

    cutoff = time.time() - 86400

    for key, ts in list(sent_signals.items()):
        if ts < cutoff:
            del sent_signals[key]

def run_scheduler():
    print("Scheduler Started")
    print("Checkpoint 1")

    try:
        telegram_send("ربات فعال شد")
        print("Checkpoint 2")
    except Exception as e:
        print("Telegram startup error:", e)

    print("Checkpoint 3")

    while True:
        try:
            print("Scanning market V2...")
            scan_market()
            print("Scan finished")
        except Exception as e:
            print("Scheduler loop error:", e)

        time.sleep(CHECK_MINUTES * 60)


if __name__ == "__main__":
    threading.Thread(target=run_scheduler).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
