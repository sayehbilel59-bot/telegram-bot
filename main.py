import os
import asyncio
import hashlib
import logging
import random
import threading
from datetime import date
from http.server import BaseHTTPRequestHandler, HTTPServer

import aiosqlite
import httpx
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReactionTypeEmoji,
    Update,
)
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_TELEGRAM_ID"])
TON_WALLET = "UQBmNOWBHkjpaQTCa4kEnZSfilZP-ffkAD7wfQdjGhLwsGg3"
USDT_WALLET = "UQBmNOWBHkjpaQTCa4kEnZSfilZP-ffkAD7wfQdjGhLwsGg3"
DB_PATH = os.path.join(os.path.dirname(__file__), "orders.db")
BOOKS_DIR = os.path.join(os.path.dirname(__file__), "book")
TONCENTER_BASE = "https://toncenter.com/api/v2"
TON_NANO = 1_000_000_000
POLL_INTERVAL = 30
POLL_MAX = 20
HEALTH_PORT = 8000
POINTS_PER_BUY = 10

# ---------------------------------------------------------------------------
# Product catalogue
# ---------------------------------------------------------------------------
PRODUCTS: dict = {
    "it_magiche": {
        "title": "50 Storie Magiche",
        "emoji": "✨📖",
        "lang": "it",
        "price_ton": 2,
        "price_usdt": 6,
        "file": "storie-interattive.html",
        "desc": (
            "🇮🇹 Una raccolta di 50 storie magiche per sognare ad occhi aperti.\n"
            "🇸🇦 مجموعة من 50 قصة سحرية لتحلم بعيون مفتوحة."
        ),
    },
    "it_inganno": {
        "title": "L'Ultimo Inganno",
        "emoji": "🎭🔍",
        "lang": "it",
        "price_ton": 3,
        "price_usdt": 9,
        "file": "ultimo_inganno.html",
        "desc": (
            "🇮🇹 Un thriller psicologico che ti terrà incollato fino all'ultima pagina.\n"
            "🇸🇦 رواية إثارة نفسية ستبقيك مشدوداً حتى الصفحة الأخيرة."
        ),
    },
    "ar_khidaa": {
        "title": "الخداع الأخير",
        "emoji": "🌙📜",
        "lang": "ar",
        "price_ton": 2,
        "price_usdt": 6,
        "file": "khidaa_akhir.html",
        "desc": (
            "🇮🇹 Un'avvincente storia araba di inganni e misteri.\n"
            "🇸🇦 قصة عربية مثيرة مليئة بالخداع والأسرار."
        ),
    },
}

# ---------------------------------------------------------------------------
# Daily free stories
# ---------------------------------------------------------------------------
DAILY_STORIES = [
    {
        "title": "🌟 La Stella Cadente | النجمة الساقطة",
        "text": (
            "🇮🇹 C'era una volta una piccola stella che aveva paura di cadere. "
            "Ma quando si lasciò andare, illuminò il cielo come mai prima d'ora.\n\n"
            "🇸🇦 كان يا ما كان نجمة صغيرة تخشى السقوط. "
            "لكنها حين أطلقت نفسها أخيراً، أضاءت السماء كما لم تفعل من قبل."
        ),
    },
    {
        "title": "🦁 Il Leone e la Farfalla | الأسد والفراشة",
        "text": (
            "🇮🇹 Il re della foresta si inchinò davanti alla farfalla. "
            '"Insegnami a volare" disse. Lei sorrise: "Inizia credendoci."\n\n'
            "🇸🇦 انحنى ملك الغابة أمام الفراشة. قال: 'علّميني أن أطير'. "
            "ابتسمت: 'ابدأ بالإيمان'."
        ),
    },
    {
        "title": "🌊 Il Mare e la Roccia | البحر والصخرة",
        "text": (
            "🇮🇹 Ogni giorno il mare batteva contro la roccia — non per vincere, "
            'ma per dirle: "Sono qui. Con te. Sempre."\n\n'
            "🇸🇦 كل يوم كان البحر يضرب الصخرة — ليس ليفوز، بل ليقول لها: "
            "'أنا هنا. معك. دائماً'."
        ),
    },
    {
        "title": "🕯️ La Candela | الشمعة",
        "text": (
            '🇮🇹 Una candela disse: "Brucio per illuminare gli altri." '
            'L\'oscurità rispose: "Ed io esisto perché tu splenda."\n\n'
            "🇸🇦 قالت شمعة: 'أحترق لأضيء للآخرين'. "
            "أجابها الظلام: 'وأنا موجود لكي تتألقي'."
        ),
    },
    {
        "title": "🌱 Il Seme | البذرة",
        "text": (
            "🇮🇹 Nessuno vide il seme lavorare sotto terra. Tutti videro il fiore. "
            "Sii il seme.\n\n"
            "🇸🇦 لم يرَ أحد البذرة وهي تعمل تحت الأرض. الكل رأى الزهرة. "
            "كن البذرة."
        ),
    },
]

# Active TON verification tasks  {user_id → Task}
verification_tasks: dict[int, asyncio.Task] = {}


# ---------------------------------------------------------------------------
# Health server
# ---------------------------------------------------------------------------
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok","service":"telegram-bot"}')

    def log_message(self, *_):
        pass


class _HealthServer(HTTPServer):
    allow_reuse_address = True


def start_health_server():
    s = _HealthServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
    threading.Thread(target=s.serve_forever, daemon=True).start()
    logger.info("Health server on port %d", HEALTH_PORT)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id      INTEGER PRIMARY KEY,
                username     TEXT,
                full_name    TEXT,
                points       INTEGER DEFAULT 0,
                referral_code TEXT UNIQUE,
                referred_by  INTEGER,
                free_stories INTEGER DEFAULT 0,
                last_daily   TEXT,
                created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id        INTEGER NOT NULL,
                username       TEXT,
                full_name      TEXT,
                product_id     TEXT DEFAULT 'legacy',
                payment_method TEXT DEFAULT 'TON',
                status         TEXT DEFAULT 'pending',
                created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        for col in ("payment_method", "product_id"):
            try:
                await db.execute(
                    f"ALTER TABLE orders ADD COLUMN {col} TEXT DEFAULT 'legacy'"
                )
            except Exception:
                pass
        await db.commit()


def _referral_code(user_id: int) -> str:
    return hashlib.md5(str(user_id).encode()).hexdigest()[:8].upper()


async def register_user(
    user_id: int, username: str, full_name: str, referral_code_used: str | None = None
):
    code = _referral_code(user_id)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        ) as cur:
            existing = await cur.fetchone()

        if existing:
            return dict(existing)

        # Find who referred this user
        referred_by = None
        if referral_code_used:
            async with db.execute(
                "SELECT user_id FROM users WHERE referral_code = ?",
                (referral_code_used,),
            ) as cur:
                row = await cur.fetchone()
                if row and row["user_id"] != user_id:
                    referred_by = row["user_id"]

        await db.execute(
            "INSERT INTO users (user_id, username, full_name, referral_code, referred_by)"
            " VALUES (?, ?, ?, ?, ?)",
            (user_id, username, full_name, code, referred_by),
        )
        await db.commit()

        async with db.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        ) as cur:
            return dict(await cur.fetchone())


async def get_user(user_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        ) as cur:
            row = await cur.fetchone()
    return dict(row) if row else None


async def add_points(user_id: int, pts: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET points = points + ? WHERE user_id = ?", (pts, user_id)
        )
        await db.commit()


async def add_free_story(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET free_stories = free_stories + 1 WHERE user_id = ?",
            (user_id,),
        )
        await db.commit()


async def use_free_story(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT free_stories FROM users WHERE user_id = ?", (user_id,)
        ) as cur:
            row = await cur.fetchone()
        if not row or row["free_stories"] < 1:
            return False
        await db.execute(
            "UPDATE users SET free_stories = free_stories - 1 WHERE user_id = ?",
            (user_id,),
        )
        await db.commit()
    return True


async def claim_daily(user_id: int) -> bool:
    today = str(date.today())
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT last_daily FROM users WHERE user_id = ?", (user_id,)
        ) as cur:
            row = await cur.fetchone()
        if row and row["last_daily"] == today:
            return False
        await db.execute(
            "UPDATE users SET last_daily = ? WHERE user_id = ?", (today, user_id)
        )
        await db.commit()
    return True


async def has_purchased(user_id: int, product_id: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id FROM orders WHERE user_id = ? AND product_id = ? AND status = 'delivered'",
            (user_id, product_id),
        ) as cur:
            return await cur.fetchone() is not None


async def create_order(
    user_id: int, username: str, full_name: str, product_id: str, method: str
) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO orders (user_id, username, full_name, product_id, payment_method, status)"
            " VALUES (?, ?, ?, ?, ?, 'pending')",
            (user_id, username, full_name, product_id, method),
        )
        await db.commit()
        return cur.lastrowid


async def mark_delivered(order_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE orders SET status = 'delivered' WHERE id = ?", (order_id,)
        )
        await db.commit()


async def get_pending_orders() -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM orders WHERE status = 'pending' ORDER BY created_at ASC"
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_referral_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM users WHERE referred_by = ?", (user_id,)
        ) as cur:
            row = await cur.fetchone()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Book delivery
# ---------------------------------------------------------------------------
async def send_book(bot, chat_id: int, product_id: str) -> bool:
    p = PRODUCTS.get(product_id)
    if not p:
        await bot.send_message(
            chat_id=chat_id, text="❌ Prodotto non trovato / منتج غير موجود"
        )
        return False
    path = os.path.join(BOOKS_DIR, p["file"])
    if not os.path.exists(path):
        await bot.send_message(
            chat_id=chat_id,
            text=(
                "⚠️ File temporaneamente non disponibile. Contatta l'admin.\n"
                "⚠️ الملف غير متاح مؤقتاً. تواصل مع الإدارة."
            ),
        )
        return False
    with open(path, "rb") as f:
        await bot.send_document(
            chat_id=chat_id,
            document=f,
            filename=p["file"],
            caption=(
                f"🎉 {p['emoji']} *{p['title']}*\n\n"
                f"🇮🇹 Grazie per il tuo acquisto! Buona lettura 📖\n"
                f"🇸🇦 شكراً لشرائك! قراءة ممتعة 📖\n\n"
                f"_© 2026 Bilal Sayeh_"
            ),
            parse_mode="Markdown",
        )
    return True


# ---------------------------------------------------------------------------
# TON verification
# ---------------------------------------------------------------------------
async def fetch_ton_transactions(wallet: str, limit: int = 20) -> list:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"{TONCENTER_BASE}/getTransactions",
            params={"address": wallet, "limit": limit},
        )
        data = resp.json()
    return data.get("result", []) if data.get("ok") else []


async def verify_ton_task(
    app: Application,
    user_id: int,
    order_id: int,
    product_id: str,
    expected_nano: int,
    comment: str,
):
    logger.info(
        "TON verify: user=%d order=%d product=%s comment=%s",
        user_id,
        order_id,
        product_id,
        comment,
    )
    for attempt in range(1, POLL_MAX + 1):
        await asyncio.sleep(POLL_INTERVAL)
        logger.info("TON check %d/%d user=%d", attempt, POLL_MAX, user_id)
        try:
            for tx in await fetch_ton_transactions(TON_WALLET):
                in_msg = tx.get("in_msg", {})
                value = int(in_msg.get("value", 0))
                msg = in_msg.get("message", "").strip()
                if value >= expected_nano and msg == comment:
                    logger.info("✅ Payment found for user=%d", user_id)
                    await mark_delivered(order_id)
                    await add_points(user_id, POINTS_PER_BUY)

                    # Check referral reward
                    user = await get_user(user_id)
                    if user and user.get("referred_by"):
                        referrer_id = user["referred_by"]
                        # Check if this is buyer's first purchase
                        async with aiosqlite.connect(DB_PATH) as db:
                            async with db.execute(
                                "SELECT COUNT(*) FROM orders WHERE user_id=? AND status='delivered'",
                                (user_id,),
                            ) as cur:
                                count = (await cur.fetchone())[0]
                        if count == 1:  # just this one
                            await add_free_story(referrer_id)
                            try:
                                await app.bot.send_message(
                                    chat_id=referrer_id,
                                    text=(
                                        "🎁 *Bonus referral!*\n\n"
                                        "🇮🇹 Un tuo amico ha appena acquistato! Hai guadagnato una storia gratuita 🎉\n"
                                        "🇸🇦 صديقك اشترى للتو! حصلت على قصة مجانية 🎉\n\n"
                                        "Usa /start → 🎁 per riscattarla!"
                                    ),
                                    parse_mode="Markdown",
                                )
                            except Exception:
                                pass

                    await app.bot.send_message(
                        chat_id=user_id,
                        text=(
                            f"✅ *Pagamento verificato! | تم التحقق من الدفع!*\n\n"
                            f"🇮🇹 Pagamento confermato sulla blockchain TON! +{POINTS_PER_BUY} punti 💎\n"
                            f"🇸🇦 تم تأكيد الدفع على بلوكتشين TON! +{POINTS_PER_BUY} نقطة 💎"
                        ),
                        parse_mode="Markdown",
                    )
                    await send_book(app.bot, user_id, product_id)
                    await app.bot.send_message(
                        chat_id=ADMIN_ID,
                        text=(
                            f"✅ TON auto-verified\nUser: {user_id} | Order: {order_id}\n"
                            f"Product: {product_id} | Amount: {value / TON_NANO:.4f} TON"
                        ),
                    )
                    verification_tasks.pop(user_id, None)
                    return
        except Exception as e:
            logger.error("TON verify error user=%d: %s", user_id, e)

    # Timeout fallback
    verification_tasks.pop(user_id, None)
    p = PRODUCTS.get(product_id, {})
    try:
        await app.bot.send_message(
            chat_id=user_id,
            text=(
                "⏰ *Timeout verifica automatica | انتهت مهلة التحقق التلقائي*\n\n"
                "🇮🇹 Non abbiamo rilevato il pagamento automaticamente. L'admin verificherà manualmente.\n"
                "🇸🇦 لم نتمكن من التحقق تلقائياً. سيتحقق المسؤول يدوياً."
            ),
            parse_mode="Markdown",
        )
        await app.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"⚠️ TON timeout\nUser: {user_id} | Order: {order_id}\n"
                f"Product: {p.get('title', product_id)} | Comment: {comment}"
            ),
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "✅ Confirm",
                            callback_data=f"confirm_{order_id}_{user_id}_{product_id}",
                        ),
                        InlineKeyboardButton(
                            "❌ Reject", callback_data=f"reject_{order_id}_{user_id}"
                        ),
                    ]
                ]
            ),
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------
def main_menu_keyboard(user: dict) -> InlineKeyboardMarkup:
    free = user.get("free_stories", 0)
    free_label = (
        f"🎁 Storia Gratuita ({free}) | قصة مجانية"
        if free > 0
        else "📅 Storia del Giorno | قصة اليوم"
    )
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🇮🇹 Libri Italiani", callback_data="sec_it"),
                InlineKeyboardButton("🇸🇦 كتب عربية", callback_data="sec_ar"),
            ],
            [InlineKeyboardButton(free_label, callback_data="daily")],
            [
                InlineKeyboardButton("💎 Punti | نقاطي", callback_data="points"),
                InlineKeyboardButton("🔗 Invita | ادعُ", callback_data="invite"),
            ],
        ]
    )


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_user = update.effective_user

    # Handle referral deep-link
    ref_code = None
    if context.args:
        arg = context.args[0]
        if arg.startswith("ref_"):
            ref_code = arg[4:]

    user = await register_user(
        tg_user.id, tg_user.username or "", tg_user.full_name or "", ref_code
    )

    # Fun reaction + animation
    try:
        await update.message.set_reaction([ReactionTypeEmoji("🔥")])
    except Exception:
        pass

    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    # Animated intro
    msg = await update.message.reply_text("✨")
    await asyncio.sleep(0.35)
    await msg.edit_text("✨ 📚")
    await asyncio.sleep(0.35)
    await msg.edit_text("✨ 📚 🌟")
    await asyncio.sleep(0.35)
    await msg.edit_text("✨ 📚 🌟 🎭")
    await asyncio.sleep(0.35)
    await msg.delete()

    pts = user.get("points", 0)
    free = user.get("free_stories", 0)

    welcome = (
        f"🌟✨ *Benvenuto, {tg_user.first_name}!* | *مرحباً، {tg_user.first_name}!* ✨🌟\n\n"
        f"🇮🇹 Esplora la nostra libreria digitale e vivi storie indimenticabili!\n"
        f"🇸🇦 استكشف مكتبتنا الرقمية وعش قصصاً لا تُنسى!\n\n"
        f"💎 Punti | نقاطك: *{pts}*"
    )
    if free > 0:
        welcome += f"\n🎁 Storie gratuite | قصص مجانية: *{free}*"

    await update.message.reply_text(
        welcome,
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard(user),
    )


async def section_it_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = []
    for pid, p in PRODUCTS.items():
        if p["lang"] == "it":
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"{p['emoji']} {p['title']} — {p['price_ton']} TON",
                        callback_data=f"prod_{pid}",
                    )
                ]
            )
    keyboard.append([InlineKeyboardButton("🔙 Menu | القائمة", callback_data="menu")])
    await query.edit_message_text(
        "📚 *Libri Italiani | الكتب الإيطالية* 📚\n\n"
        "🇮🇹 Scegli un libro da acquistare:\n"
        "🇸🇦 اختر كتاباً للشراء:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def section_ar_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = []
    for pid, p in PRODUCTS.items():
        if p["lang"] == "ar":
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"{p['emoji']} {p['title']} — {p['price_ton']} TON",
                        callback_data=f"prod_{pid}",
                    )
                ]
            )
    keyboard.append([InlineKeyboardButton("🔙 Menu | القائمة", callback_data="menu")])
    await query.edit_message_text(
        "📖 *الكتب العربية | Libri Arabi* 📖\n\n"
        "🇸🇦 اختر كتاباً للشراء:\n"
        "🇮🇹 Scegli un libro da acquistare:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = await get_user(update.effective_user.id) or {}
    pts = user.get("points", 0)
    free = user.get("free_stories", 0)
    text = f"🌟 *Menu Principale | القائمة الرئيسية* 🌟\n\n💎 Punti | نقاطك: *{pts}*"
    if free > 0:
        text += f"\n🎁 Storie gratuite | قصص مجانية: *{free}*"
    await query.edit_message_text(
        text,
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard(user),
    )


async def product_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    product_id = query.data[5:]  # strip "prod_"
    p = PRODUCTS.get(product_id)
    if not p:
        await query.answer("Prodotto non trovato!", show_alert=True)
        return

    user_id = update.effective_user.id
    already = await has_purchased(user_id, product_id)
    if already:
        await query.edit_message_text(
            f"✅ *Hai già acquistato questo libro! | لديك هذا الكتاب بالفعل!*\n\n"
            f"{p['emoji']} *{p['title']}*\n\n"
            f"🇮🇹 Usa /getbook per scaricarlo di nuovo.\n"
            f"🇸🇦 استخدم /getbook لتنزيله مرة أخرى.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Menu | القائمة", callback_data="menu")]]
            ),
        )
        return

    back = "sec_it" if p["lang"] == "it" else "sec_ar"
    await query.edit_message_text(
        f"{p['emoji']} *{p['title']}*\n\n"
        f"{p['desc']}\n\n"
        f"💰 TON: *{p['price_ton']} TON*\n"
        f"💵 USDT: *{p['price_usdt']} USDT*\n\n"
        f"🇮🇹 Scegli il metodo di pagamento:\n"
        f"🇸🇦 اختر طريقة الدفع:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"💎 TON ({p['price_ton']} TON)",
                        callback_data=f"ton_{product_id}",
                    ),
                    InlineKeyboardButton(
                        f"💵 USDT ({p['price_usdt']} USDT)",
                        callback_data=f"usdt_{product_id}",
                    ),
                ],
                [InlineKeyboardButton("🔙 Indietro | رجوع", callback_data=back)],
            ]
        ),
    )


async def pay_ton_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    product_id = query.data[4:]  # strip "ton_"
    p = PRODUCTS.get(product_id)
    if not p:
        return

    user = update.effective_user
    order_id = await create_order(
        user.id, user.username or "", user.full_name or "", product_id, "TON"
    )

    await query.edit_message_text(
        f"💎 *Pagamento TON — {p['title']} | الدفع بـ TON*\n\n"
        f"🇮🇹 Invia esattamente *{p['price_ton']} TON* a:\n"
        f"🇸🇦 أرسل *{p['price_ton']} TON* بالضبط إلى:\n\n"
        f"`{TON_WALLET}`\n\n"
        f"⚠️ *Importante | مهم:*\n"
        f"🇮🇹 Nel campo *commento/memo* scrivi:\n"
        f"🇸🇦 في حقل *التعليق/المذكرة* اكتب:\n\n"
        f"`{user.id}`\n\n"
        f"🇮🇹 Questo permette la verifica automatica ⚡\n"
        f"🇸🇦 هذا يتيح التحقق التلقائي ⚡",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "⚡ Ho pagato! | دفعت!",
                        callback_data=f"ptd_{product_id}_{order_id}",
                    ),
                ]
            ]
        ),
    )


async def pay_usdt_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    product_id = query.data[5:]  # strip "usdt_"
    p = PRODUCTS.get(product_id)
    if not p:
        return

    user = update.effective_user
    order_id = await create_order(
        user.id, user.username or "", user.full_name or "", product_id, "USDT"
    )

    await query.edit_message_text(
        f"💵 *Pagamento USDT — {p['title']} | الدفع بـ USDT*\n\n"
        f"🇮🇹 Invia esattamente *{p['price_usdt']} USDT* (rete TON) a:\n"
        f"🇸🇦 أرسل *{p['price_usdt']} USDT* (شبكة TON) إلى:\n\n"
        f"`{USDT_WALLET}`\n\n"
        f"🇮🇹 Dopo il pagamento, premi il pulsante.\n"
        f"🇸🇦 بعد الدفع، اضغط الزر.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ Ho pagato! | دفعت!",
                        callback_data=f"pud_{product_id}_{order_id}",
                    ),
                ]
            ]
        ),
    )


async def paid_ton_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("⚡ Verifica avviata! | بدأ التحقق!")

    _, product_id, order_id_str = query.data.split("_", 2)
    order_id = int(order_id_str)
    p = PRODUCTS.get(product_id, {})
    user = update.effective_user

    existing = verification_tasks.get(user.id)
    if existing and not existing.done():
        existing.cancel()

    expected_nano = int(p.get("price_ton", 2) * TON_NANO)
    task = asyncio.create_task(
        verify_ton_task(
            context.application,
            user.id,
            order_id,
            product_id,
            expected_nano,
            str(user.id),
        )
    )
    verification_tasks[user.id] = task

    await query.edit_message_text(
        f"⚡ *Verifica automatica avviata! | التحقق التلقائي بدأ!*\n\n"
        f"🇮🇹 Il bot controlla la blockchain ogni 30 secondi per 10 minuti.\n"
        f"🇸🇦 البوت يفحص البلوكتشين كل 30 ثانية لمدة 10 دقائق.\n\n"
        f"📚 *{p.get('title', '')}*\n\n"
        f"🇮🇹 Riceverai il libro non appena il pagamento sarà confermato 🎉\n"
        f"🇸🇦 ستتلقى الكتاب فور تأكيد الدفع 🎉",
        parse_mode="Markdown",
    )


async def paid_usdt_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("✅ Notifica inviata! | تم الإرسال!")

    _, product_id, order_id_str = query.data.split("_", 2)
    order_id = int(order_id_str)
    p = PRODUCTS.get(product_id, {})
    user = update.effective_user
    username_str = f"@{user.username}" if user.username else "—"

    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=(
            f"💵 *USDT Payment Notification*\n\n"
            f"User: {user.full_name} ({username_str})\n"
            f"User ID: `{user.id}`\n"
            f"Product: {p.get('emoji', '')} {p.get('title', product_id)}\n"
            f"Amount: {p.get('price_usdt', '?')} USDT\n"
            f"Order ID: `{order_id}`"
        ),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ Confirm",
                        callback_data=f"confirm_{order_id}_{user.id}_{product_id}",
                    ),
                    InlineKeyboardButton(
                        "❌ Reject", callback_data=f"reject_{order_id}_{user.id}"
                    ),
                ]
            ]
        ),
    )

    await query.edit_message_text(
        f"✅ *Notifica inviata! | تم إرسال الإشعار!*\n\n"
        f"🇮🇹 L'amministratore verificherà il tuo pagamento USDT a breve.\n"
        f"🇸🇦 سيتحقق المسؤول من دفعك قريباً.\n\n"
        f"📚 *{p.get('title', '')}*",
        parse_mode="Markdown",
    )


async def admin_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if update.effective_user.id != ADMIN_ID:
        await query.answer("Non autorizzato.", show_alert=True)
        return

    parts = query.data.split("_")
    order_id = int(parts[1])
    buyer_id = int(parts[2])
    product_id = parts[3] if len(parts) > 3 else "legacy"

    await mark_delivered(order_id)
    await add_points(buyer_id, POINTS_PER_BUY)

    # Referral check
    user = await get_user(buyer_id)
    if user and user.get("referred_by"):
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM orders WHERE user_id=? AND status='delivered'",
                (buyer_id,),
            ) as cur:
                count = (await cur.fetchone())[0]
        if count == 1:
            await add_free_story(user["referred_by"])
            try:
                await context.bot.send_message(
                    chat_id=user["referred_by"],
                    text=(
                        "🎁 *Bonus referral!*\n\n"
                        "🇮🇹 Il tuo amico ha acquistato! Hai una storia gratuita 🎉\n"
                        "🇸🇦 صديقك اشترى! لديك قصة مجانية 🎉"
                    ),
                    parse_mode="Markdown",
                )
            except Exception:
                pass

    await send_book(context.bot, buyer_id, product_id)
    await context.bot.send_message(
        chat_id=buyer_id,
        text=(
            f"🎉 *Pagamento confermato! | تم تأكيد الدفع!*\n\n"
            f"🇮🇹 +{POINTS_PER_BUY} punti aggiunti al tuo account! 💎\n"
            f"🇸🇦 تمت إضافة {POINTS_PER_BUY} نقطة لحسابك! 💎"
        ),
        parse_mode="Markdown",
    )
    await query.edit_message_text(
        f"✅ Order #{order_id} confirmed. Book sent to {buyer_id}. +{POINTS_PER_BUY} pts."
    )


async def admin_reject_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if update.effective_user.id != ADMIN_ID:
        await query.answer("Non autorizzato.", show_alert=True)
        return

    parts = query.data.split("_")
    order_id = int(parts[1])
    buyer_id = int(parts[2])

    await context.bot.send_message(
        chat_id=buyer_id,
        text=(
            "❌ *Pagamento rifiutato | الدفع مرفوض*\n\n"
            "🇮🇹 Non siamo riusciti a verificare il pagamento. Contatta il supporto o riprova con /start.\n"
            "🇸🇦 لم نتمكن من التحقق من الدفع. تواصل مع الدعم أو أعد المحاولة."
        ),
        parse_mode="Markdown",
    )
    await query.edit_message_text(
        f"❌ Order #{order_id} rejected. User {buyer_id} notified."
    )


async def daily_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id
    user = await get_user(user_id) or {}

    # Check free story credits first
    if user.get("free_stories", 0) > 0:
        await query.answer("🎁 Storia gratuita!")
        success = await use_free_story(user_id)
        if success:
            story = random.choice(DAILY_STORIES)
            await query.edit_message_text(
                f"🎁 *Storia Gratuita Referral! | قصة مجانية كمكافأة!*\n\n"
                f"*{story['title']}*\n\n"
                f"{story['text']}\n\n"
                f"_© 2026 Bilal Sayeh_",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Menu | القائمة", callback_data="menu")]]
                ),
            )
            return

    # Daily story
    claimed = await claim_daily(user_id)
    if not claimed:
        await query.answer("⏰ Già letta oggi! | قرأتها اليوم!", show_alert=True)
        await query.edit_message_text(
            "⏰ *Storia già letta oggi! | قرأت قصة اليوم بالفعل!*\n\n"
            "🇮🇹 Torna domani per una nuova storia gratuita 📖\n"
            "🇸🇦 عد غداً لقصة مجانية جديدة 📖",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Menu | القائمة", callback_data="menu")]]
            ),
        )
        return

    await query.answer("📖 Ecco la storia!")
    story = random.choice(DAILY_STORIES)
    await query.edit_message_text(
        f"📅 *Storia del Giorno | قصة اليوم* 📅\n\n"
        f"*{story['title']}*\n\n"
        f"{story['text']}\n\n"
        f"_© 2026 Bilal Sayeh_\n\n"
        f"🇮🇹 Torna domani per un'altra storia! Invita amici per storie extra 🎁\n"
        f"🇸🇦 عد غداً لقصة أخرى! ادعُ أصدقاء للحصول على قصص إضافية 🎁",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔙 Menu | القائمة", callback_data="menu")]]
        ),
    )


async def points_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = await get_user(update.effective_user.id) or {}
    pts = user.get("points", 0)
    free = user.get("free_stories", 0)
    # Count purchases
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM orders WHERE user_id=? AND status='delivered'",
            (update.effective_user.id,),
        ) as cur:
            purchases = (await cur.fetchone())[0]

    await query.edit_message_text(
        f"💎 *I Tuoi Punti | نقاطك* 💎\n\n"
        f"🏆 Punti | النقاط: *{pts}*\n"
        f"📚 Acquisti | المشتريات: *{purchases}*\n"
        f"🎁 Storie gratuite | قصص مجانية: *{free}*\n\n"
        f"🇮🇹 Ogni acquisto ti dà +{POINTS_PER_BUY} punti! 🚀\n"
        f"🇸🇦 كل عملية شراء تمنحك +{POINTS_PER_BUY} نقطة! 🚀",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔙 Menu | القائمة", callback_data="menu")]]
        ),
    )


async def invite_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = await get_user(update.effective_user.id) or {}
    code = user.get("referral_code") or _referral_code(update.effective_user.id)
    bot_username = (await context.bot.get_me()).username
    ref_link = f"https://t.me/{bot_username}?start=ref_{code}"
    ref_count = await get_referral_count(update.effective_user.id)

    await query.edit_message_text(
        f"🔗 *Programma Referral | برنامج الإحالة* 🔗\n\n"
        f"🇮🇹 Condividi il tuo link unico. Ogni amico che acquista ti regala una storia gratuita! 🎁\n"
        f"🇸🇦 شارك رابطك الفريد. كل صديق يشتري يمنحك قصة مجانية! 🎁\n\n"
        f"🔗 *Il tuo link | رابطك:*\n`{ref_link}`\n\n"
        f"👥 Amici invitati | الأصدقاء المدعوون: *{ref_count}*\n\n"
        f"🇮🇹 Copia il link e condividilo ovunque!\n"
        f"🇸🇦 انسخ الرابط وشاركه في كل مكان!",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔙 Menu | القائمة", callback_data="menu")]]
        ),
    )


async def getbook_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM orders WHERE user_id=? AND status='delivered' ORDER BY created_at DESC",
            (user_id,),
        ) as cur:
            orders = [dict(r) for r in await cur.fetchall()]

    if not orders:
        await update.message.reply_text(
            "📭 *Nessun acquisto trovato | لا يوجد مشتريات*\n\n"
            "🇮🇹 Usa /start per acquistare un libro.\n"
            "🇸🇦 استخدم /start لشراء كتاب.",
            parse_mode="Markdown",
        )
        return

    keyboard = []
    seen = set()
    for o in orders:
        pid = o.get("product_id", "legacy")
        if pid in seen or pid == "legacy":
            continue
        seen.add(pid)
        p = PRODUCTS.get(pid)
        if p:
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"{p['emoji']} {p['title']}", callback_data=f"dl_{pid}"
                    )
                ]
            )

    if not keyboard:
        await update.message.reply_text(
            "🇮🇹 Nessun libro disponibile per il download.\n"
            "🇸🇦 لا يوجد كتاب متاح للتنزيل."
        )
        return

    keyboard.append([InlineKeyboardButton("🔙 Menu | القائمة", callback_data="menu")])
    await update.message.reply_text(
        "📚 *I Tuoi Libri | كتبك* 📚\n\n"
        "🇮🇹 Scegli quale scaricare:\n"
        "🇸🇦 اختر الكتاب للتنزيل:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def download_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("📥 Download...")
    product_id = query.data[3:]  # strip "dl_"
    user_id = update.effective_user.id
    if await has_purchased(user_id, product_id):
        await send_book(context.bot, user_id, product_id)
        await query.edit_message_text(
            "📤 Libro inviato! | تم إرسال الكتاب!\n\n"
            "🇮🇹 Controlla i tuoi messaggi.\n🇸🇦 تحقق من رسائلك.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Menu | القائمة", callback_data="menu")]]
            ),
        )
    else:
        await query.answer("❌ Accesso non autorizzato!", show_alert=True)


async def orders_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("⛔ Admin only.")
        return
    orders = await get_pending_orders()
    if not orders:
        await update.message.reply_text("✅ No pending orders.")
        return
    lines = [f"📋 *Pending Orders ({len(orders)}):*\n"]
    for o in orders:
        pid = o.get("product_id", "?")
        p = PRODUCTS.get(pid, {})
        lines.append(
            f"• #{o['id']} — {o['full_name']} | `{o['user_id']}`\n"
            f"  📚 {p.get('title', pid)} | {o.get('payment_method', '?')} | {o['created_at'][:16]}"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def admin_send_book_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("⛔ Admin only.")
        return

    index_map = {
        "1": "it_magiche",
        "2": "it_inganno",
        "3": "ar_khidaa",
    }

    if not context.args or context.args[0] not in index_map:
        lines = ["📚 *Uso: /send_book \\<numero\\>*\n"]
        for num, pid in index_map.items():
            p = PRODUCTS[pid]
            lines.append(f"`{num}` — {p['emoji']} {p['title']} ({p['price_ton']} TON)")
        await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")
        return

    product_id = index_map[context.args[0]]
    p = PRODUCTS[product_id]
    await update.message.reply_text(
        f"📤 Invio *{p['title']}* in corso...", parse_mode="Markdown"
    )
    success = await send_book(context.bot, ADMIN_ID, product_id)
    if success:
        await update.message.reply_text(
            f"✅ *{p['title']}* inviato con successo!", parse_mode="Markdown"
        )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *Bilal Sayeh Books Bot* 📖\n\n"
        "🇮🇹 *Comandi:*\n"
        "/start — Menu principale\n"
        "/getbook — Scarica i tuoi libri\n"
        "/help — Questo messaggio\n\n"
        "🇸🇦 *الأوامر:*\n"
        "/start — القائمة الرئيسية\n"
        "/getbook — نزّل كتبك\n"
        "/help — هذه الرسالة\n\n"
        "_© 2026 Bilal Sayeh_",
        parse_mode="Markdown",
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def post_init(application: Application) -> None:
    start_health_server()
    await init_db()
    logger.info("Bot initialized.")


def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("getbook", getbook_command))
    app.add_handler(CommandHandler("orders", orders_command))
    app.add_handler(CommandHandler("send_book", admin_send_book_command))

    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu$"))
    app.add_handler(CallbackQueryHandler(section_it_callback, pattern="^sec_it$"))
    app.add_handler(CallbackQueryHandler(section_ar_callback, pattern="^sec_ar$"))
    app.add_handler(CallbackQueryHandler(product_callback, pattern=r"^prod_"))
    app.add_handler(CallbackQueryHandler(pay_ton_callback, pattern=r"^ton_"))
    app.add_handler(CallbackQueryHandler(pay_usdt_callback, pattern=r"^usdt_"))
    app.add_handler(CallbackQueryHandler(paid_ton_callback, pattern=r"^ptd_"))
    app.add_handler(CallbackQueryHandler(paid_usdt_callback, pattern=r"^pud_"))
    app.add_handler(CallbackQueryHandler(admin_confirm_callback, pattern=r"^confirm_"))
    app.add_handler(CallbackQueryHandler(admin_reject_callback, pattern=r"^reject_"))
    app.add_handler(CallbackQueryHandler(daily_callback, pattern="^daily$"))
    app.add_handler(CallbackQueryHandler(points_callback, pattern="^points$"))
    app.add_handler(CallbackQueryHandler(invite_callback, pattern="^invite$"))
    app.add_handler(CallbackQueryHandler(download_callback, pattern=r"^dl_"))

    logger.info("Bot starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
