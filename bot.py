import os
import time
import math
import threading
from datetime import datetime, timezone

import requests
from flask import Flask
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

BINANCE_BASE_URLS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com",
]

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "TRXUSDT"
]

INTERVAL = os.getenv("INTERVAL", "5m")
CHECK_MINUTES = int(os.getenv("CHECK_MINUTES", "5"))
MAX_SIGNALS_PER_SCAN = int(os.getenv("MAX_SIGNALS_PER_SCAN", "3"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "3"))

app = Flask(__name__)
sent_signals = {}


def now_utc_text():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def telegram_send(text):
    if not BOT_TOKEN or not CHAT_ID:
        print("BOT_TOKEN or CHAT_ID is missing.")
        return

    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        response = requests.post(
            url,
            json={
                "chat_id": CHAT_ID,
                "text": text,
                "disable_web_page_preview": True,
            },
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

            if response.status_code == 200 and not (
                isinstance(data, dict) and "code" in data
            ):
                return data

            last_error = data

        except Exception as exc:
            last_error = exc

    raise RuntimeError(f"Binance request failed: {last_error}")


def get_klines(symbol, interval="5m", limit=120):
    data = binance_get(
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        },
    )

    candles = []

    for item in data:
        candles.append(
            {
                "open_time": item[0],
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
            }
        )

    return candles


def ema(values, period):
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)
    result = sum(values[:period]) / period

    for price in values[period:]:
        result = (price - result) * multiplier + result

    return result


def rsi(values, period=14):
    if len(values) <= period:
        return None

    gains = []
    losses = []

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


def average(values):
    if not values:
        return 0
    return sum(values) / len(values)


def price_precision(price):
    if price >= 1000:
        return 2
    if price >= 1:
        return 4
    return 6


def analyze_symbol(symbol):
    candles = get_klines(symbol, INTERVAL, 120)

    if len(candles) < 60:
        return None

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]

    last_price = closes[-1]
    previous_price = closes[-2]

    ema_9 = ema(closes, 9)
    ema_21 = ema(closes, 21)
    ema_50 = ema(closes, 50)
    rsi_14 = rsi(closes, 14)

    recent_high = max(highs[-20:-1])
    recent_low = min(lows[-20:-1])

    avg_volume = average(volumes[-21:-1])
    last_volume = volumes[-1]
    volume_spike = last_volume > avg_volume * 1.25 if avg_volume > 0 else False

    score_long = 0
    score_short = 0
    reasons_long = []
    reasons_short = []

    if ema_9 and ema_21 and ema_50:
        if ema_9 > ema_21 > ema_50:
            score_long += 1
            reasons_long.append("روند کوتاه‌مدت صعودی است؛ EMA9 بالای EMA21 و EMA50 قرار دارد.")

        if ema_9 < ema_21 < ema_50:
            score_short += 1
            reasons_short.append("روند کوتاه‌مدت نزولی است؛ EMA9 پایین EMA21 و EMA50 قرار دارد.")

    if rsi_14 is not None:
        if 52 <= rsi_14 <= 68:
            score_long += 1
            reasons_long.append(f"RSI در محدوده مثبت قرار دارد: {rsi_14:.1f}")

        if 32 <= rsi_14 <= 48:
            score_short += 1
            reasons_short.append(f"RSI در محدوده منفی قرار دارد: {rsi_14:.1f}")

    if last_price > recent_high:
        score_long += 1
        reasons_long.append("شکست مقاومت کوتاه‌مدت دیده شد.")

    if last_price < recent_low:
        score_short += 1
        reasons_short.append("شکست حمایت کوتاه‌مدت دیده شد.")

    if volume_spike:
        score_long += 1
        score_short += 1
        reasons_long.append("حجم معاملات بالاتر از میانگین است.")
        reasons_short.append("حجم معاملات بالاتر از میانگین است.")

    momentum = ((last_price - previous_price) / previous_price) * 100

    if momentum > 0.08:
        score_long += 1
        reasons_long.append(f"مومنتوم کوتاه‌مدت مثبت است: {momentum:.2f}%")

    if momentum < -0.08:
        score_short += 1
        reasons_short.append(f"مومنتوم کوتاه‌مدت منفی است: {momentum:.2f}%")

    if score_long >= score_short:
        side = "LONG"
        score = score_long
        reasons = reasons_long
    else:
        side = "SHORT"
        score = score_short
        reasons = reasons_short

    if score < MIN_SCORE:
        return None

    volatility = ((max(highs[-10:]) - min(lows[-10:])) / last_price) * 100
    risk_pct = max(0.35, min(1.2, volatility * 0.45))

    if side == "LONG":
        entry = last_price
        stop_loss = entry * (1 - risk_pct / 100)
        target_1 = entry * (1 + risk_pct * 1.3 / 100)
        target_2 = entry * (1 + risk_pct * 2.1 / 100)
    else:
        entry = last_price
        stop_loss = entry * (1 + risk_pct / 100)
        target_1 = entry * (1 - risk_pct * 1.3 / 100)
        target_2 = entry * (1 - risk_pct * 2.1 / 100)

    precision = price_precision(entry)

    return {
        "id": f"{symbol}-{side}-{round(entry, precision)}",
        "symbol": symbol,
        "side": side,
        "score": score,
        "entry": round(entry, precision),
        "stop_loss": round(stop_loss, precision),
        "target_1": round(target_1, precision),
        "target_2": round(target_2, precision),
        "rsi": round(rsi_14, 2) if rsi_14 is not None else None,
        "risk_pct": round(risk_pct, 2),
        "reasons": reasons,
        "time": now_utc_text(),
    }


def format_signal(signal):
    direction_emoji = "🟢" if signal["side"] == "LONG" else "🔴"

    reasons_text = "\n".join([f"• {reason}" for reason in signal["reasons"]])

    return f"""
🚨 سیگنال اسکلپ کریپتو

{direction_emoji} نوع معامله: {signal["side"]}
🪙 ارز: {signal["symbol"]}
⏱ تایم‌فریم: {INTERVAL}

📍 ورود پیشنهادی:
{signal["entry"]}

🎯 تارگت ۱:
{signal["target_1"]}

🎯 تارگت ۲:
{signal["target_2"]}

🛑 حد ضرر:
{signal["stop_loss"]}

📊 قدرت سیگنال:
{signal["score"]}/5

📈 RSI:
{signal["rsi"]}

⚠️ ریسک تقریبی:
{signal["risk_pct"]}%

🧠 دلایل سیگنال:
{reasons_text}

🕒 زمان:
{signal["time"]}

⚠️ این پیام توصیه مالی قطعی نیست. قبل از ورود، مدیریت سرمایه و شرایط بازار را بررسی کن.
"""
    def scan_market():
    print("Scanning market...")
    telegram_send("🔍 اسکن جدید بازار آغاز شد...")

    found = []

    for symbol in SYMBOLS:
        try:
            signal = analyze_symbol(symbol)

            if signal and signal["id"] not in sent_signals:
                found.append(signal)

        except Exception as exc:
            print(f"{symbol} failed: {exc}")

    found.sort(key=lambda x: x["score"], reverse=True)

    if not found:
        telegram_send("⚪ در این اسکن سیگنال مناسبی پیدا نشد.")
        return

    for signal in found[:MAX_SIGNALS_PER_SCAN]:
        sent_signals[signal["id"]] = time.time()
        telegram_send(format_signal(signal))

    cutoff = time.time() - 86400

    for key, ts in list(sent_signals.items()):
        if ts < cutoff:
            del sent_signals[key]


def run_scheduler():
    telegram_send(f"""
✅ ربات سیگنال اسکلپ فعال شد

🔍 اسکن اولیه بازار شروع شد.
📊 معیارها:
• EMA
• RSI
• حجم معاملات
• شکست حمایت/مقاومت
• مومنتوم کوتاه‌مدت

⏱ فاصله بررسی: هر {CHECK_MINUTES} دقیقه
🪙 تایم‌فریم: {INTERVAL}
""")

    scan_market()

    scheduler = BlockingScheduler()
    scheduler.add_job(scan_market, "interval", minutes=CHECK_MINUTES)
    scheduler.start()


if __name__ == "__main__":
    threading.Thread(target=run_scheduler, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
