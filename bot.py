import os
import math
import time
import threading
from datetime import datetime, timezone

import requests
from flask import Flask
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# Binance public market-data endpoint. If one endpoint fails, the code tries the next one.
BINANCE_BASE_URLS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com",
]

# You can change symbols here.
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "TRXUSDT"
]

INTERVAL = os.getenv("INTERVAL", "5m")
CHECK_MINUTES = int(os.getenv("CHECK_MINUTES", "5"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "3"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "4"))

app = Flask(__name__)
sent_signals = {}


def now_utc_text():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def telegram_send(text):
    if not BOT_TOKEN or not CHAT_ID:
        print("BOT_TOKEN or CHAT_ID is missing.")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        response = requests.post(
            url,
            json={"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True},
            timeout=20,
        )
        if response.status_code != 200:
            print("Telegram error:", response.text)
    except Exception as exc:
        print("Telegram send failed:", exc)


@app.route("/")
def home():
    return "Crypto Scalp Signal Bot is running ✅"


def binance_get(path, params=None):
    last_error = None
    for base in BINANCE_BASE_URLS:
        try:
            response = requests.get(base + path, params=params, timeout=15)
            data = response.json()
            if response.status_code == 200 and not isinstance(data, dict) or (isinstance(data, dict) and "code" not in data):
                return data
            last_error = data
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Binance request failed: {last_error}")


def get_klines(symbol, interval="5m", limit=120):
    data = binance_get("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    candles = []
    for k in data:
        candles.append({
            "open_time": int(k[0]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
        })
    return candles


def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    value = sum(values[:period]) / period
    for price in values[period:]:
        value = price * k + value * (1 - k)
    return value


def rsi(values, period=14):
    if len(values) <= period:
        return None
    gains = []
    losses = []
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
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
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(trs[-period:]) / period


def average_volume(candles, period=20):
    if len(candles) < period:
        return None
    return sum(c["volume"] for c in candles[-period:]) / period


def analyze_symbol(symbol):
    candles = get_klines(symbol, INTERVAL, 120)
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]

    last = candles[-1]
    price = last["close"]
    ema9 = ema(closes[-60:], 9)
    ema21 = ema(closes[-80:], 21)
    rsi14 = rsi(closes, 14)
    atr14 = atr(candles, 14)
    vol_avg = average_volume(candles[:-1], 20)
    vol_spike = vol_avg and last["volume"] > vol_avg * 1.35

    resistance = max(highs[-21:-1])
    support = min(lows[-21:-1])

    long_score = 0
    short_score = 0
    reasons_long = []
    reasons_short = []

    if ema9 and ema21 and ema9 > ema21:
        long_score += 1
        reasons_long.append("EMA9 بالاتر از EMA21 است")
    if ema9 and ema21 and ema9 < ema21:
        short_score += 1
        reasons_short.append("EMA9 پایین‌تر از EMA21 است")

    if rsi14 and 48 <= rsi14 <= 68:
        long_score += 1
        reasons_long.append("RSI در محدوده مناسب لانگ است")
    if rsi14 and 32 <= rsi14 <= 52:
        short_score += 1
        reasons_short.append("RSI در محدوده مناسب شورت است")

    if price > resistance:
        long_score += 2
        reasons_long.append("شکست مقاومت کوتاه‌مدت")
    if price < support:
        short_score += 2
        reasons_short.append("شکست حمایت کوتاه‌مدت")

    if vol_spike:
        long_score += 1
        short_score += 1
        reasons_long.append("افزایش حجم معاملات")
        reasons_short.append("افزایش حجم معاملات")

    if not atr14 or atr14 <= 0:
        return None

    if long_score >= MIN_SCORE and long_score > short_score:
        entry = price
        stop = entry - atr14 * 1.1
        target1 = entry + atr14 * 1.2
        target2 = entry + atr14 * 2.0
        return make_signal(symbol, "LONG", entry, stop, target1, target2, long_score, rsi14, reasons_long)

    if short_score >= MIN_SCORE and short_score > long_score:
        entry = price
        stop = entry + atr14 * 1.1
        target1 = entry - atr14 * 1.2
        target2 = entry - atr14 * 2.0
        return make_signal(symbol, "SHORT", entry, stop, target1, target2, short_score, rsi14, reasons_short)

    return None


def make_signal(symbol, side, entry, stop, target1, target2, score, rsi14, reasons):
    risk = abs(entry - stop)
    reward = abs(target1 - entry)
    rr = reward / risk if risk else 0
    signal_id = f"{symbol}:{side}:{round(entry, 4)}:{datetime.now(timezone.utc).strftime('%Y%m%d%H')}"
    return {
        "id": signal_id,
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "stop": stop,
        "target1": target1,
        "target2": target2,
        "score": score,
        "rsi": rsi14,
        "rr": rr,
        "reasons": reasons,
    }


def format_price(value):
    if value >= 100:
        return f"{value:,.2f}"
    if value >= 1:
        return f"{value:,.4f}"
    return f"{value:.6f}"


def format_signal(signal):
    side_icon = "🟢" if signal["side"] == "LONG" else "🔴"
    reasons = "\n".join([f"• {r}" for r in signal["reasons"][:4]])
    return f"""
🚨 سیگنال اسکلپ کریپتو

{side_icon} نوع معامله: {signal['side']}
🪙 ارز: {signal['symbol']}
⏱ تایم‌فریم: {INTERVAL}

🎯 ورود پیشنهادی: {format_price(signal['entry'])}
🛑 حد ضرر: {format_price(signal['stop'])}
✅ تارگت ۱: {format_price(signal['target1'])}
✅ تارگت ۲: {format_price(signal['target2'])}

📊 امتیاز سیگنال: {signal['score']}/6
📈 RSI: {signal['rsi']:.1f}
⚖️ R/R تقریبی تارگت ۱: {signal['rr']:.2f}

📌 دلایل:
{reasons}

⚠️ مدیریت ریسک:
این سیگنال تضمین سود نیست. برای اسکالپ، حجم معامله را کوچک نگه دار و بدون حد ضرر وارد نشو.

🕒 {now_utc_text()}
""".strip()


def scan_market():
    print("Scanning market...")
    found = []
    for symbol in SYMBOLS:
        try:
            signal = analyze_symbol(symbol)
            if signal and signal["id"] not in sent_signals:
                found.append(signal)
        except Exception as exc:
            print(f"{symbol} failed: {exc}")

    found.sort(key=lambda x: x["score"], reverse=True)
    for signal in found[:MAX_SIGNALS_PER_SCAN]:
        sent_signals[signal["id"]] = time.time()
        telegram_send(format_signal(signal))

    # Clean old signal ids after 24 hours
    cutoff = time.time() - 86400
    for key, ts in list(sent_signals.items()):
        if ts < cutoff:
            del sent_signals[key]


def run_scheduler():
    telegram_send("✅ ربات سیگنال اسکلپ فعال شد.\nدر حال بررسی بازار بر اساس EMA، RSI، حجم و شکست حمایت/مقاومت.")
    scan_market()
    scheduler = BlockingScheduler()
    scheduler.add_job(scan_market, "interval", minutes=CHECK_MINUTES)
    scheduler.start()


if __name__ == "__main__":
    threading.Thread(target=run_scheduler, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
