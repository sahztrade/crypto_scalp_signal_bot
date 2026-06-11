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


if name == "__main__":
    threading.Thread(target=run_scheduler, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
