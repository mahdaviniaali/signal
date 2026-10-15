# گزارش تصمیم‌ها و مسیر پروژه

این سند مسیر فکری و تصمیم‌های مهم پروژهٔ **High-Performance Multi-Worker Kafka Consumer** را ثبت می‌کند: مسئله چه بود، چه گزینه‌هایی مطرح شد، و چرا به نتیجهٔ فعلی رسیدیم.

---

## ۱. مسئلهٔ اولیه (صورت‌مسئله)

سرویس باید با FastAPI از دو تاپیک مستقل Kafka مصرف کند:

| تاپیک | نوع کار |
|--------|---------|
| `io-heavy-events` | I/O-bound (~100ms تأخیر شبیه‌سازی‌شده) |
| `cpu-heavy-events` | CPU-bound (محاسبات سنگین) |

الزام‌های کلیدی ارزیابی:

- اجرای هم‌زمان با **۵ worker**
- throughput مناسب زیر بار بالا، بدون صف بی‌نهایت کار pending
- I/O زیر فشار CPU همچنان پاسخگو بماند
- رفتار خطا و خاموش شدن worker صریح باشد؛ پیام پردازش‌نشده نباید «پردازش‌شده» فرض شود

تحویل‌دادنی‌ها: کد سرویس، روش اجرا با ۵ worker، ابزار تولید پیام تست، و گزارش کوتاه بار.

---

## ۲. مسیر کلی پروژه (چه ساختیم و به چه ترتیب)

```text
صورت‌مسئله
    → اسکلت FastAPI + Kafka (Docker)
    → جدا کردن مسیر I/O و CPU
    → backpressure + commit امن
    → تست بار و متریک
    → idempotency (سناریوی سفارش ۱۰۱)
    → مستندسازی تصمیم‌ها (همین گزارش)
```

خروجی‌های اصلی در ریپو:

| بخش | نقش |
|-----|-----|
| `app/main.py` | FastAPI، lifespan، health/metrics/events |
| `app/consumers/` | حلقهٔ مصرف، pause/resume، watermark commit |
| `app/processing.py` | منطق I/O و CPU + چک تکراری بودن |
| `app/store.py` | SQLite برای ذخیرهٔ یکتای رویداد/سفارش |
| `scripts/` | produce، load test، دموی idempotency |
| `docs/load_report.md` | مشاهدات throughput |
| `docs/decision_report.md` | این گزارش |

---

## ۳. تصمیم‌های معماری (چرا این‌طور شد)

### ۳.۱ پنج worker با Uvicorn

**تصمیم:** `uvicorn ... --workers 5` و هر process هر دو consumer را استارت می‌کند؛ عضویت در consumer groupهای `io-workers` و `cpu-workers`.

**چرا:**

- صورت‌مسئله صریحاً «۵ parallel workers» و رفتار «وقتی یک worker می‌ایستد» را می‌خواست → مدل process-based با rebalance طبیعی Kafka جور است.
- تاپیک‌ها با **۵ partition** ساخته شدند تا زیر ۵ worker تخصیص تقریباً یک‌به‌یک ممکن باشد.

**نتیجه:** توقف یک worker → partitionها به بقیه می‌روند؛ offset فقط برای کار موفق commitشده باقی می‌ماند.

---

### ۳.۲ جدا کردن I/O از CPU روی event loop

**تصمیم:**

- I/O: `asyncio.sleep(0.1)` (بدون بلاک کردن loop)
- CPU: `ProcessPoolExecutor` + `run_in_executor`

**چرا:**

- اگر CPU روی همان thread/event loop اجرا شود، awaitهای I/O داخل همان worker می‌خوابند → معیار «I/O تحت بار CPU زنده بماند» رد می‌شود.
- Thread pool برای CPU خالص پایتون کافی نیست (GIL)؛ process pool کار سنگین را از loop جدا می‌کند.

**نتیجهٔ مشاهده‌شده (خلاصه از `load_report.md`):** همزمان با پردازش CPU، شمارندهٔ I/O همچنان بالا می‌رود.

---

### ۳.۳ Backpressure با Semaphore + pause/resume

**تصمیم:** سقف in-flight per consumer (`IO_MAX_IN_FLIGHT` / `CPU_MAX_IN_FLIGHT`)؛ وقتی ظرفیت پر است consumer pause می‌شود و بعد resume.

**چرا:**

- صورت‌مسئله: زیر بار بالا نباید pending بی‌کران جمع شود.
- فقط `create_task` بدون سقف = رشد حافظه و تأخیر غیرقابل کنترل.

**نتیجه:** نرخ پردازش پایدار می‌ماند؛ صف داخلی worker محدود است.

---

### ۳.۴ Commit دستی و watermark per-partition

**تصمیم:** `enable_auto_commit=False`؛ commit فقط بعد از موفقیت، به‌صورت **پیشوند پیوستهٔ موفق** روی هر partition (watermark). روی خطا: commit نمی‌شود، seek به offset خطا، backoff.

**چرا این مسیر را رفتیم:**

1. Auto-commit می‌تواند پیام را قبل از اتمام کار «خورده‌شده» نشان دهد → نقض صریح الزام صورت‌مسئله.
2. نسخهٔ اولِ «commit همان offset بلافاصله بعد از موفقیت» با پردازش هم‌زمان خطرناک بود: اگر offset بالاتر زودتر تمام شود، ممکن بود از روی پیام پایین‌تر بپریم.
3. قفل per-partition (سریال کامل داخل partition) درست بود ولی concurrency I/O را خیلی پایین می‌آورد → به **watermark** برگشتیم تا هم ایمنی commit حفظ شود هم parallelism داخل partition.

**جمع‌بندی قرارداد خطا/خاموشی:**

| رویداد | رفتار |
|--------|--------|
| شکست پردازش | بدون commit؛ seek؛ متریک `failed` |
| موفقیت نامرتب | فقط تا جایی commit که همهٔ offsetهای قبلش موفق باشند |
| توقف worker | قطع fetch، drain با timeout، بستن consumer، rebalance |

---

### ۳.۵ Idempotency (سفارش ۱۰۱) — تصمیم محصولی مهم

**سناریو:** سفارش `101` در دیتابیس ذخیره می‌شود؛ قبل از اینکه offset به Kafka commit شود ربات خاموش می‌شود. بعد از روشن شدن، Kafka همان پیام را دوباره می‌فرستد.

**سؤال:** ربات چه باید بکند؟

**تصمیم تأییدشده:** قبل از ثبت دوباره چک کند؛ اگر قبلاً ثبت شده، دوباره ثبت نکند؛ سپس offset را commit کند تا حلقهٔ redelivery تمام شود.

**چرا این پاسخ درست است:**

- Kafka در این طراحی **at-least-once** است، نه exactly-once.
- Commit offset ≠ «اثر جانبی حتماً یک‌بار اعمال شده»؛ برعکس، ممکن است اثر جانبی اعمال شده باشد و commit نرسیده باشد.
- بدون idempotency، همان سفارش دو بار در سیستم ظاهر می‌شود.

**پیاده‌سازی در پروژه:**

- کلید کسب‌وکار: فیلد `id` یا `order_id` پیام
- ذخیره در SQLite با `PRIMARY KEY (topic, event_id)` در `app/store.py`
- در `app/processing.py`: اگر `exists` → `duplicates++` و skip؛ وگرنه پردازش و `save` (در race، `IntegrityError` هم duplicate حساب می‌شود)
- APIهای `GET /events/{topic}/{event_id}` و اسکریپت `scripts/demo_idempotency.py` برای اثبات

**نکتهٔ تکمیلی طراحی:** unique constraint در DB از «دو worker همزمان» هم محافظت می‌کند؛ فقط چک `exists` در dual-write کافی نیست.

---

### ۳.۶ زیرساخت محلی Kafka

**تصمیم:** Docker Compose با تصویر رسمی Kafka (KRaft)، listener دوگانه برای دسترسی از host و از داخل شبکهٔ Docker، و سرویس `kafka-init` برای ساخت تاپیک‌ها با ۵ partition.

**چرا:** اجرای قابل‌تکرار برای مصاحبه/دمو بدون نصب دستی broker.

---

### ۳.۷ متریک و مشاهده‌پذیری

**تصمیم:** متریک in-memory per-worker (`processed` / `failed` / `duplicates` / rate) روی `GET /metrics`؛ برای جمع زدن چند worker از polling مکرر (`scripts/check_metrics.py`) استفاده می‌شود.

**چرا:** با چند process uvicorn، یک request فقط یک worker را می‌بیند؛ این محدودیت صریح مستند شد تا نتیجهٔ تست غلط تفسیر نشود.

---

## ۴. چیزهایی که عمداً خارج از محدوده ماند

- Exactly-once transactional Kafka / sink ایدمپوتنت پیچیده در DB توزیع‌شده
- کلاستر چند-broker production-grade
- Idempotent DB واقعی سفارش (به‌جای SQLite دمو) — الگو همان است؛ فقط backend persistence عوض می‌شود

---

## ۵. جمع‌بندی یک‌خطی برای ارائه

> با ۵ worker روی consumer group، I/O را async و CPU را در process pool نگه داشتیم تا event loop زنده بماند؛ با semaphore و pause از صف بی‌کران جلوگیری کردیم؛ با commit بعد از موفقیت و watermark جلوی «پیام نخورده را خورده فرض کردن» را گرفتیم؛ و چون Kafka ممکن است بعد از کرش دوباره بفرستد، با کلید یکتای سفارش در DB عملیات را idempotent کردیم تا سفارش ۱۰۱ دوبار ثبت نشود.

---

## ۶. ارجاع به کد و اسناد مرتبط

| موضوع | کجا ببینید |
|--------|------------|
| حلقهٔ مصرف / watermark / pause | `app/consumers/base.py` |
| I/O و CPU + skip تکراری | `app/processing.py` |
| ذخیرهٔ یکتا | `app/store.py` |
| تنظیمات | `app/config.py` |
| راهنمای اجرا | `README.md` |
| اعداد بار | `docs/load_report.md` |
