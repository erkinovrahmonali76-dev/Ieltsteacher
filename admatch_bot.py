import asyncio
import html
import logging
import os
import re
from datetime import datetime, timezone

import asyncpg
from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, Message, ReplyKeyboardMarkup, ReplyKeyboardRemove,
)

# -------------------- ENVIRONMENT --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS = {int(x) for x in re.findall(r"\d+", os.getenv("ADMIN_IDS", ""))}
CHANNEL_ID = os.getenv("CHANNEL_ID", os.getenv("CHANNEL", "")).strip()
PORT = int(os.getenv("PORT", "10000"))
MAX_POST_LENGTH = 1200

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("akosh_community")

router = Router()
admin_router = Router()
pool: asyncpg.Pool | None = None

CATEGORIES = {
    "study": "📚 Ta'lim",
    "tech": "💻 Texnologiya",
    "games": "🎮 O'yinlar",
    "creative": "🎨 Ijod",
    "sport": "⚽ Sport",
    "other": "💬 Boshqa",
}
MAIN_MENU = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📝 Post yuborish"), KeyboardButton(text="📖 Qoidalar")],
        [KeyboardButton(text="ℹ️ Yordam")],
    ],
    resize_keyboard=True,
)

class PostState(StatesGroup):
    category = State()
    content = State()

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def category_keyboard() -> InlineKeyboardMarkup:
    rows = []
    items = list(CATEGORIES.items())
    for i in range(0, len(items), 2):
        rows.append([
            InlineKeyboardButton(text=items[i][1], callback_data=f"cat:{items[i][0]}"),
            *(
                [InlineKeyboardButton(text=items[i + 1][1], callback_data=f"cat:{items[i + 1][0]}")]
                if i + 1 < len(items) else []
            ),
        ])
    rows.append([InlineKeyboardButton(text="❌ Bekor qilish", callback_data="post:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def moderation_keyboard(post_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Tasdiqlash", callback_data=f"mod:approve:{post_id}"),
        InlineKeyboardButton(text="❌ Rad etish", callback_data=f"mod:reject:{post_id}"),
    ]])

def clean_text(value: str) -> str:
    return value.strip()

def looks_unsafe_for_public_post(value: str) -> str | None:
    """Basic guard against publishing direct contact details or links."""
    if re.search(r"https?://|t\.me/|www\.", value, re.IGNORECASE):
        return "Post ichida havola yoki Telegram manzili bo'lmasin."
    if re.search(r"(?<!\d)(?:\+?\d[\d\s().-]{7,}\d)(?!\d)", value):
        return "Post ichida telefon raqami bo'lmasin."
    if re.search(r"@[A-Za-z0-9_]{4,}", value):
        return "Post ichida shaxsiy akkaunt nomi bo'lmasin."
    return None

async def init_db() -> None:
    global pool
    if not DATABASE_URL:
        raise RuntimeError("Render Environment'da DATABASE_URL kiritilmagan.")
    dsn = DATABASE_URL
    if dsn.startswith("postgres://"):
        dsn = "postgresql://" + dsn[len("postgres://"):]
    # Render PostgreSQL accepts TLS; local development can set DB_SSL=disable.
    ssl_setting = os.getenv("DB_SSL", "require").lower()
    ssl_value = False if ssl_setting in {"0", "false", "disable", "off"} else "require"

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=5,
        command_timeout=30,
        timeout=30,
        ssl=ssl_value,
    )
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS community_posts (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                category TEXT NOT NULL,
                body TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','approved','rejected')),
                channel_message_id BIGINT,
                moderator_id BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                moderated_at TIMESTAMPTZ
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS community_posts_pending_idx
            ON community_posts(status, created_at)
        """)
    log.info("PostgreSQL bazasi tayyor.")

async def health(_: web.Request) -> web.Response:
    if pool is None:
        return web.json_response({"ok": False, "database": "not_initialized"}, status=503)
    try:
        await pool.fetchval("SELECT 1")
        return web.json_response({"ok": True, "service": "AKOSH COMMUNITY BOT"})
    except Exception:
        log.exception("Health-check database xatosi")
        return web.json_response({"ok": False, "database": "error"}, status=503)

async def start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("Health server port %s da ishga tushdi.", PORT)
    return runner

@router.message(CommandStart())
async def start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "👋 <b>AKOSH COMMUNITY</b>ga xush kelibsiz!\n\n"
        "Bu yerda ta'lim, texnologiya, o'yinlar, ijod va sport haqida "
        "foydali post yuborishingiz mumkin.\n\n"
        "📌 Har bir post kanalga chiqishidan oldin admin tomonidan tekshiriladi.",
        reply_markup=MAIN_MENU,
    )

@router.message(F.text == "📖 Qoidalar")
@router.message(Command("rules"))
async def rules(message: Message):
    await message.answer(
        "<b>📖 Hamjamiyat qoidalari</b>\n\n"
        "1. Hurmat bilan muloqot qiling.\n"
        "2. Haqorat, tahdid va kamsitish taqiqlanadi.\n"
        "3. Shaxsiy telefon raqami, manzil va akkauntlarni joylamang.\n"
        "4. Reklama, spam va firibgarlik taqiqlanadi.\n"
        "5. Boshqalarning shaxsiy ma'lumotlari yoki suratlarini ruxsatsiz yubormang.\n"
        "6. Mualliflik huquqini hurmat qiling.\n\n"
        "Qoidaga zid postlar rad etiladi."
    )

@router.message(F.text == "ℹ️ Yordam")
@router.message(Command("help"))
async def help_message(message: Message):
    await message.answer(
        "📝 Post yuborish tugmasini bosing, mavzu tanlang va matnni jo'nating.\n"
        "Post admin tekshiruvidan o'tgach kanalga chiqariladi.\n\n"
        "/start — bosh menyu\n/cancel — joriy amalni bekor qilish"
    )

@router.message(Command("cancel"))
async def cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Amal bekor qilindi.", reply_markup=MAIN_MENU)

@router.message(F.text == "📝 Post yuborish")
async def begin_post(message: Message, state: FSMContext):
    await state.set_state(PostState.category)
    await message.answer("Post mavzusini tanlang:", reply_markup=category_keyboard())

@router.callback_query(F.data == "post:cancel")
async def cancel_post(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.answer("Bekor qilindi.")
    await callback.message.edit_text("Post yuborish bekor qilindi.")

@router.callback_query(PostState.category, F.data.startswith("cat:"))
async def choose_category(callback: CallbackQuery, state: FSMContext):
    key = callback.data.split(":", 1)[1]
    if key not in CATEGORIES:
        return await callback.answer("Mavzu topilmadi.", show_alert=True)
    await state.update_data(category=key)
    await state.set_state(PostState.content)
    await callback.answer()
    await callback.message.edit_text(
        f"Tanlangan mavzu: <b>{CATEGORIES[key]}</b>\n\n"
        f"Endi postingizni yuboring (1–{MAX_POST_LENGTH} belgi).\n"
        "Telefon raqami, havola yoki shaxsiy akkaunt nomini kiritmang.\n\n"
        "Bekor qilish: /cancel"
    )

@router.message(PostState.content, F.text)
async def receive_post(message: Message, state: FSMContext):
    body = clean_text(message.text)
    if not body:
        return await message.answer("Matn bo'sh bo'lmasin.")
    if len(body) > MAX_POST_LENGTH:
        return await message.answer(f"Post {MAX_POST_LENGTH} belgidan oshmasin.")
    if body.startswith("/"):
        return await message.answer("Buyruq emas, post matnini yuboring. Bekor qilish: /cancel")
    issue = looks_unsafe_for_public_post(body)
    if issue:
        return await message.answer(f"⚠️ {issue}\nIltimos, postni tahrirlab qayta yuboring.")
    data = await state.get_data()
    category = data.get("category")
    if category not in CATEGORIES:
        await state.clear()
        return await message.answer("Holat tugagan. Qaytadan boshlang.", reply_markup=MAIN_MENU)

    try:
        post_id = await pool.fetchval(
            "INSERT INTO community_posts(user_id, category, body) "
            "VALUES($1,$2,$3) RETURNING id",
            message.from_user.id, category, body,
        )
    except Exception:
        log.exception("Postni bazaga saqlashda xato")
        return await message.answer("Post saqlanmadi. Birozdan keyin qayta urinib ko'ring.")

    await state.clear()
    await message.answer(
        f"✅ Post qabul qilindi!\nRaqami: <code>#{post_id}</code>\n"
        "Admin tekshiruvidan keyin natija haqida xabar beramiz.",
        reply_markup=MAIN_MENU,
    )

    preview = (
        f"🆕 <b>Yangi post #{post_id}</b>\n"
        f"Mavzu: {CATEGORIES[category]}\n\n"
        f"{html.escape(body)}\n\n"
        f"<i>Yuboruvchi ID: {message.from_user.id}</i>"
    )
    for admin_id in ADMIN_IDS:
        try:
            await message.bot.send_message(
                admin_id, preview, reply_markup=moderation_keyboard(post_id)
            )
        except TelegramForbiddenError:
            log.warning("Admin %s botni hali /start qilmagan.", admin_id)
        except Exception:
            log.exception("Adminga post yuborilmadi: %s", admin_id)

@router.message(PostState.content)
async def receive_non_text(message: Message):
    await message.answer("Iltimos, postni matn ko'rinishida yuboring.")

@admin_router.message(Command("admin"))
async def admin_home(message: Message):
    if not is_admin(message.from_user.id):
        return
    pending = await pool.fetchval(
        "SELECT count(*) FROM community_posts WHERE status='pending'"
    )
    await message.answer(
        f"🛠 <b>Admin panel</b>\nKutilayotgan postlar: <b>{pending}</b>\n\n"
        "/pending — navbatdagi postlarni ko'rish\n/stats — statistika"
    )

@admin_router.message(Command("pending"))
async def pending_posts(message: Message):
    if not is_admin(message.from_user.id):
        return
    rows = await pool.fetch(
        "SELECT id,user_id,category,body,created_at FROM community_posts "
        "WHERE status='pending' ORDER BY created_at ASC LIMIT 10"
    )
    if not rows:
        return await message.answer("⏳ Kutilayotgan post yo'q.")
    for row in rows:
        text = (
            f"🆕 <b>Post #{row['id']}</b>\n"
            f"Mavzu: {CATEGORIES.get(row['category'], 'Boshqa')}\n"
            f"Yuboruvchi ID: <code>{row['user_id']}</code>\n\n"
            f"{html.escape(row['body'])}"
        )
        await message.answer(text, reply_markup=moderation_keyboard(row["id"]))

@admin_router.message(Command("stats"))
async def stats(message: Message):
    if not is_admin(message.from_user.id):
        return
    row = await pool.fetchrow("""
        SELECT count(*) AS total,
               count(*) FILTER (WHERE status='pending') AS pending,
               count(*) FILTER (WHERE status='approved') AS approved,
               count(*) FILTER (WHERE status='rejected') AS rejected
        FROM community_posts
    """)
    await message.answer(
        "📊 <b>Bot statistikasi</b>\n"
        f"Jami: {row['total']}\nKutilmoqda: {row['pending']}\n"
        f"Tasdiqlangan: {row['approved']}\nRad etilgan: {row['rejected']}"
    )

@admin_router.callback_query(F.data.startswith("mod:"))
async def moderate_post(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer("Ruxsat yo'q.", show_alert=True)
    try:
        _, action, raw_id = callback.data.split(":")
        post_id = int(raw_id)
    except (ValueError, TypeError):
        return await callback.answer("Noto'g'ri so'rov.", show_alert=True)
    if action not in {"approve", "reject"}:
        return await callback.answer("Noma'lum amal.", show_alert=True)

    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM community_posts WHERE id=$1 FOR UPDATE", post_id
            )
            if not row:
                return await callback.answer("Post topilmadi.", show_alert=True)
            if row["status"] != "pending":
                return await callback.answer("Bu post avval ko'rib chiqilgan.", show_alert=True)

            if action == "reject":
                await conn.execute(
                    "UPDATE community_posts SET status='rejected', moderator_id=$2, "
                    "moderated_at=now() WHERE id=$1",
                    post_id, callback.from_user.id,
                )
            else:
                if not CHANNEL_ID:
                    return await callback.answer(
                        "CHANNEL_ID sozlanmagan. Render Environment'ni tekshiring.",
                        show_alert=True,
                    )
                category = CATEGORIES.get(row["category"], "💬 Boshqa")
                published = (
                    f"{category}\n\n{html.escape(row['body'])}\n\n"
                    f"<i>AKOSH COMMUNITY</i>"
                )
                try:
                    sent = await callback.bot.send_message(CHANNEL_ID, published)
                except TelegramBadRequest:
                    log.exception("Kanalga post chiqarilmadi.")
                    return await callback.answer(
                        "Kanalga yuborilmadi. Bot kanal admini ekanini tekshiring.",
                        show_alert=True,
                    )
                await conn.execute(
                    "UPDATE community_posts SET status='approved', "
                    "channel_message_id=$2, moderator_id=$3, moderated_at=now() WHERE id=$1",
                    post_id, sent.message_id, callback.from_user.id,
                )

    new_status = "approved" if action == "approve" else "rejected"
    try:
        await callback.bot.send_message(
            row["user_id"],
            f"{'✅ Postingiz kanalga chiqarildi!' if action == 'approve' else '❌ Postingiz admin tomonidan rad etildi.'}\n"
            f"Post raqami: #{post_id}",
        )
    except Exception:
        log.info("Foydalanuvchiga natija xabari yuborilmadi: %s", row["user_id"])

    await callback.answer("Bajarildi.")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer(f"Post #{post_id}: {new_status}")
    except Exception:
        pass

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN kiritilmagan.")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL kiritilmagan.")
    if not ADMIN_IDS:
        raise RuntimeError("ADMIN_IDS kiritilmagan.")
    if not CHANNEL_ID:
        log.warning("CHANNEL_ID yo'q: postlar tasdiqlanmaydi, kanalga chiqmaydi.")

    await init_db()
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(admin_router)
    dp.include_router(router)
    runner = await start_health_server()

    try:
        me = await bot.get_me()
        log.info("Bot ishga tushdi: @%s (id=%s)", me.username, me.id)
        await bot.set_my_commands([
            BotCommand(command="start", description="Botni boshlash"),
            BotCommand(command="help", description="Yordam"),
            BotCommand(command="rules", description="Hamjamiyat qoidalari"),
            BotCommand(command="cancel", description="Amalni bekor qilish"),
            BotCommand(command="admin", description="Admin paneli"),
            BotCommand(command="pending", description="Kutilayotgan postlar (admin)"),
            BotCommand(command="stats", description="Statistika (admin)"),
        ])
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await runner.cleanup()
        await bot.session.close()
        if pool is not None:
            await pool.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot to'xtatildi.")
    except Exception:
        log.exception("Bot ishga tushishida jiddiy xato.")
        raise
