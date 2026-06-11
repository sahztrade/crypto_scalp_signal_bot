# ربات سیگنال اسکلپ کریپتو

این ربات با داده عمومی Binance کندل‌های بازار را می‌گیرد و بر اساس EMA، RSI، حجم و شکست حمایت/مقاومت، سیگنال اسکالپ آموزشی ارسال می‌کند.

## راه‌اندازی روی Render

Build Command:
```bash
pip install -r requirements.txt
```

Start Command:
```bash
python bot.py
```

Environment Variables:
```env
BOT_TOKEN=توکن ربات تلگرام
CHAT_ID=آیدی چت یا کانال
INTERVAL=5m
CHECK_MINUTES=5
MAX_SIGNALS_PER_SCAN=3
MIN_SCORE=4
```

## هشدار
این ربات تضمین سود نیست. بازار کریپتو ریسک بالایی دارد. حتماً مدیریت سرمایه و حد ضرر رعایت شود.
