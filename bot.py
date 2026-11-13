import asyncio
import logging
import os
from datetime import datetime, timezone

import asyncpg
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
ADMIN_IDS = {
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}
PORT = int(os.getenv("PORT", "10000"))

CATEGORIES = {
    "education": "📚 Ta’lim",
    "tech": "💻 Texnologiya",
    "games": "🎮 O‘yinlar",
    "creative": "🎨 Ijod",
    "sport": "⚽ Sport",
    "other": "💬 Boshqa mavzular",
}
CATEGORY_LABELS = {v: k for k, v in CATEGORIES.items()}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("akosh-community")

bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
pool = None


class SubmitPost(StatesGroup):
    category = State()
    content = State()


def main_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✍️ Post yuborish")],
            [KeyboardButton(text="📖 Qoidalar"), KeyboardButton(text="ℹ️ Yordam")],
        ],
        resize_keyboard=True
    )


def category_keyboard():
    kb = InlineKeyboardBuilder()
    for key, label in CATEGORIES.items():
        kb.button(text=label, callback_data=f"cat:{key}")
    kb.adjust(2)
    return kb.as_markup()


async def init_db():
    global pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL Render Environment’da kiritilmagan.")
    db_url = DATABASE_URL
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    pool = await asyncpg.create_pool(
        db_url,
        min_size=1,
        max_size=5,
        command_timeout=30,
        ssl="require" if os.getenv("DB_SSL", "require").lower() == "require" else None,
    )
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS community_posts (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                category TEXT NOT NULL,
                content TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                reviewed_at TIMESTAMPTZ,
                reviewed_by BIGINT
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS community_posts_status_idx
            ON community_posts(status, created_at)
        """)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


@dp.message(CommandStart())
async def start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Assalomu alaykum! 👋\n\n"
        "Bu — AKOSH COMMUNITY BOT. Bu yerda ta’lim, texnologiya, "
        "o‘yinlar, ijod va sport mavzularidagi foydali postlar "
        "admin tekshiruvidan keyin kanalga chiqariladi.\n\n"
        "Shaxsiy tanishuv yoki romantik match xizmati mavjud emas.",
        reply_markup=main_menu()
    )


@dp.message(F.text == "✍️ Post yuborish")
async def begin_post(message: Message, state: FSMContext):
    await state.set_state(SubmitPost.category)
    await message.answer(
        "Postingiz uchun mavzuni tanlang:",
        reply_markup=category_keyboard()
    )


@dp.callback_query(F.data.startswith("cat:"))
async def choose_category(call: CallbackQuery, state: FSMContext):
    key = call.data.split(":", 1)[1]
    if key not in CATEGORIES:
        await call.answer("Noto‘g‘ri mavzu.", show_alert=True)
        return
    await state.update_data(category=key)
    await state.set_state(SubmitPost.content)
    await call.message.edit_reply_markup(reply_markup=None)
    await call.message.answer(
        f"Tanlangan mavzu: {CATEGORIES[key]}\n\n"
        "Endi postingiz matnini yuboring. Iltimos, telefon raqami, "
        "uy manzili, parol yoki boshqa shaxsiy ma’lumotlarni yozmang."
    )
    await call.answer()


@dp.message(SubmitPost.content, F.text)
async def receive_post(message: Message, state: FSMContext):
    content = (message.text or "").strip()
    if len(content) < 10:
        await message.answer("Post juda qisqa. Kamida 10 ta belgi yozing.")
        return
    if len(content) > 3500:
        await message.answer("Post 3500 belgidan oshmasin. Qisqartirib yuboring.")
        return

    # Avoid publishing obvious personal contact details.
    import re
    if re.search(r'(\+?\d[\d\s().-]{7,}\d)|(@[A-Za-z0-9_]{4,})|(t\.me/)', content, re.I):
        await message.answer(
            "Postda telefon raqami yoki shaxsiy aloqa manzili bo‘lishi mumkin. "
            "Ularni olib tashlab, qayta yuboring."
        )
        return

    data = await state.get_data()
    category = data.get("category")
    if category not in CATEGORIES:
        await state.clear()
        await message.answer("Jarayon boshidan boshlansin.", reply_markup=main_menu())
        return

    username = message.from_user.username
    async with pool.acquire() as conn:
        post_id = await conn.fetchval(
            """INSERT INTO community_posts(user_id, username, category, content)
               VALUES($1, $2, $3, $4) RETURNING id""",
            message.from_user.id, username, category, content
        )

    await state.clear()
    await message.answer(
        f"✅ Postingiz qabul qilindi. Buyurtma raqami: #{post_id}\n"
        "Admin tekshirganidan keyin kanalga chiqariladi.",
        reply_markup=main_menu()
    )

    if ADMIN_IDS:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Tasdiqlash", callback_data=f"approve:{post_id}"),
            InlineKeyboardButton(text="❌ Rad etish", callback_data=f"reject:{post_id}"),
        ]])
        admin_text = (
            f"📝 Yangi post #{post_id}\n"
            f"Mavzu: {CATEGORIES[category]}\n"
            f"Foydalanuvchi ID: {message.from_user.id}\n"
            f"Username: @{username if username else 'yo‘q'}\n\n"
            f"{content}"
        )
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, admin_text, reply_markup=kb)
            except Exception:
                log.exception("Admin xabarini yuborib bo‘lmadi: %s", admin_id)


@dp.message(SubmitPost.content)
async def non_text_post(message: Message):
    await message.answer("Hozircha faqat matnli post qabul qilinadi.")


async def review_post(call: CallbackQuery, approve: bool):
    if not is_admin(call.from_user.id):
        await call.answer("Bu amal faqat admin uchun.", show_alert=True)
        return
    try:
        post_id = int(call.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await call.answer("Post raqami xato.", show_alert=True)
        return

    async with pool.acquire() as conn:
        post = await conn.fetchrow(
            "SELECT * FROM community_posts WHERE id=$1 AND status='pending'",
            post_id
        )
        if not post:
            await call.answer("Post topilmadi yoki oldin ko‘rib chiqilgan.", show_alert=True)
            return

        if approve:
            if not CHANNEL_ID:
                await call.answer("CHANNEL_ID sozlanmagan.", show_alert=True)
                return
            category_label = CATEGORIES.get(post["category"], "💬 Boshqa")
            published = (
                f"{category_label}\n\n{post['content']}\n\n"
                "— AKOSH COMMUNITY"
            )
            try:
                await bot.send_message(CHANNEL_ID, published)
            except Exception:
                log.exception("Kanalga post chiqarilmadi")
                await call.answer(
                    "Kanalga yuborilmadi. Bot kanal admini ekanini tekshiring.",
                    show_alert=True
                )
                return
            new_status = "approved"
        else:
            new_status = "rejected"

        await conn.execute(
            """UPDATE community_posts
               SET status=$2, reviewed_at=NOW(), reviewed_by=$3
               WHERE id=$1""",
            post_id, new_status, call.from_user.id
        )

    await call.message.edit_reply_markup(reply_markup=None)
    await call.answer("Bajarildi.")
    try:
        await bot.send_message(
            post["user_id"],
            "✅ Postingiz kanalga chiqarildi." if approve
            else "ℹ️ Postingiz admin tomonidan rad etildi."
        )
    except Exception:
        pass


@dp.callback_query(F.data.startswith("approve:"))
async def approve_callback(call: CallbackQuery):
    await review_post(call, True)


@dp.callback_query(F.data.startswith("reject:"))
async def reject_callback(call: CallbackQuery):
    await review_post(call, False)


@dp.message(Command("admin"))
async def admin_help(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "🛠 Admin buyruqlari:\n"
        "/pending — tekshirilmagan postlar\n"
        "/stats — statistika"
    )


@dp.message(Command("pending"))
async def pending_posts(message: Message):
    if not is_admin(message.from_user.id):
        return
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, user_id, category, content, created_at
               FROM community_posts WHERE status='pending'
               ORDER BY created_at ASC LIMIT 20"""
        )
    if not rows:
        await message.answer("Tekshirilmagan postlar yo‘q.")
        return
    for row in rows:
        text = (
            f"📝 Post #{row['id']}\n"
            f"Mavzu: {CATEGORIES.get(row['category'], row['category'])}\n"
            f"User ID: {row['user_id']}\n\n{row['content']}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Tasdiqlash", callback_data=f"approve:{row['id']}"),
            InlineKeyboardButton(text="❌ Rad etish", callback_data=f"reject:{row['id']}"),
        ]])
        await message.answer(text, reply_markup=kb)


@dp.message(Command("stats"))
async def stats(message: Message):
    if not is_admin(message.from_user.id):
        return
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM community_posts")
        pending = await conn.fetchval("SELECT COUNT(*) FROM community_posts WHERE status='pending'")
        approved = await conn.fetchval("SELECT COUNT(*) FROM community_posts WHERE status='approved'")
        rejected = await conn.fetchval("SELECT COUNT(*) FROM community_posts WHERE status='rejected'")
    await message.answer(
        f"📊 Statistika\nJami: {total}\nKutilmoqda: {pending}\n"
        f"Chiqarilgan: {approved}\nRad etilgan: {rejected}"
    )


@dp.message(F.text == "📖 Qoidalar")
async def rules(message: Message):
    await message.answer(
        "📖 AKOSH COMMUNITY qoidalari:\n"
        "1. Hurmat bilan muloqot qiling.\n"
        "2. Haqorat, tahdid va spam taqiqlanadi.\n"
        "3. Telefon raqami, uy manzili, parol va shaxsiy ma’lumotlarni yubormang.\n"
        "4. Reklama va shubhali havolalar admin ruxsatisiz chiqarilmaydi.\n"
        "5. Tanishuv/match yoki romantik xizmat yo‘q.\n"
        "6. Har bir post avval admin tomonidan tekshiriladi."
    )


@dp.message(F.text == "ℹ️ Yordam")
async def help_message(message: Message):
    await message.answer(
        "Post yuborish uchun «✍️ Post yuborish» tugmasini bosing.\n"
        "Post matn ko‘rinishida bo‘lishi kerak va admin tasdiqlagandan "
        "keyingina kanalga chiqariladi."
    )


async def health(request):
    return web.json_response({"status": "ok", "service": "AKOSH COMMUNITY BOT"})


async def start_health_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    return runner


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN Render Environment’da kiritilmagan.")
    await init_db()
    runner = await start_health_server()
    log.info("AKOSH COMMUNITY BOT ishga tushmoqda.")
    try:
        await dp.start_polling(bot)
    finally:
        await runner.cleanup()
        if pool:
            await pool.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
