"""
AdMatch Bot PRO  -  Telegram tanishuv boti
Stack: aiogram 3 + PostgreSQL (asyncpg) + aiohttp (Render Web Service uchun)

Muhit o'zgaruvchilari (Render -> Environment):
  BOT_TOKEN     - BotFather tokeni                       (majburiy)
  DATABASE_URL  - Postgres manzili                       (majburiy)
  ADMIN_IDS     - admin Telegram ID lari, vergul bilan   (masalan 12345,67890)
  CHANNEL       - majburiy obuna kanali, masalan @kanal  (ixtiyoriy)
  DAILY_LIKES   - bepul foydalanuvchi uchun kunlik layk  (standart 30)
  VIP_CONTACT   - VIP uchun yoziladigan akkaunt          (masalan @admin)
"""
import asyncio
import csv
import html
import io
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone

import asyncpg
from aiohttp import ClientSession, web
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.filters import BaseFilter, Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand, BufferedInputFile, CallbackQuery, InlineKeyboardButton,
    InlineKeyboardMarkup, KeyboardButton, Message, ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

# ====================== SOZLAMALAR ======================
TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
CHANNEL = os.getenv("CHANNEL", "").strip()
DAILY_LIKES = int(os.getenv("DAILY_LIKES", "30"))
VIP_CONTACT = os.getenv("VIP_CONTACT", "@admin")
PORT = int(os.getenv("PORT", "10000"))

pool: asyncpg.Pool = None
router = Router()
admin = Router()
esc = html.escape

SCHEMA = """
CREATE TABLE IF NOT EXISTS users(
    id BIGINT PRIMARY KEY,
    username TEXT,
    name TEXT, gender TEXT, age INT, city TEXT,
    photo TEXT, interests TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    is_banned BOOLEAN DEFAULT FALSE,
    vip_until TIMESTAMPTZ,
    min_age INT DEFAULT 16,
    max_age INT DEFAULT 70,
    created_at TIMESTAMPTZ DEFAULT now(),
    last_seen TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS reactions(
    from_id BIGINT, to_id BIGINT, value SMALLINT,
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY(from_id, to_id)
);
CREATE INDEX IF NOT EXISTS idx_reactions_to ON reactions(to_id, value);
CREATE TABLE IF NOT EXISTS matches(
    a BIGINT, b BIGINT, created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY(a, b)
);
CREATE TABLE IF NOT EXISTS reports(
    id SERIAL PRIMARY KEY, from_id BIGINT, to_id BIGINT,
    created_at TIMESTAMPTZ DEFAULT now(), handled BOOLEAN DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS daily_likes(
    user_id BIGINT, day DATE, cnt INT DEFAULT 0,
    PRIMARY KEY(user_id, day)
);
CREATE INDEX IF NOT EXISTS idx_users_gender ON users(gender, is_active);
"""


# ====================== DB YORDAMCHILARI ======================
async def get_user(uid):
    return await pool.fetchrow("SELECT * FROM users WHERE id=$1", uid)


def is_vip(u):
    return bool(u and u["vip_until"] and u["vip_until"] > datetime.now(timezone.utc))


async def set_reaction(a, b, v):
    await pool.execute(
        "INSERT INTO reactions(from_id,to_id,value) VALUES($1,$2,$3) "
        "ON CONFLICT (from_id,to_id) DO UPDATE SET value=$3, created_at=now()",
        a, b, v,
    )


async def like_allowed(uid, me):
    """VIP cheklovsiz. Boshqalarga kuniga DAILY_LIKES ta."""
    if is_vip(me):
        return True
    cnt = await pool.fetchval(
        "SELECT cnt FROM daily_likes WHERE user_id=$1 AND day=CURRENT_DATE", uid
    ) or 0
    if cnt >= DAILY_LIKES:
        return False
    await pool.execute(
        "INSERT INTO daily_likes(user_id,day,cnt) VALUES($1,CURRENT_DATE,1) "
        "ON CONFLICT (user_id,day) DO UPDATE SET cnt=daily_likes.cnt+1",
        uid,
    )
    return True


async def next_profile(me):
    return await pool.fetchrow(
        """
        SELECT x.* FROM users x
        WHERE x.gender<>$1 AND x.id<>$2 AND x.is_active AND NOT x.is_banned
          AND x.age BETWEEN $3 AND $4
          AND NOT EXISTS (SELECT 1 FROM reactions r WHERE r.from_id=$2 AND r.to_id=x.id)
        ORDER BY (x.vip_until IS NOT NULL AND x.vip_until>now()) DESC,
                 (lower(x.city)=lower($5)) DESC,
                 random()
        LIMIT 1
        """,
        me["gender"], me["id"], me["min_age"], me["max_age"], me["city"],
    )


async def next_liker(uid):
    return await pool.fetchrow(
        """
        SELECT u.* FROM reactions r JOIN users u ON u.id=r.from_id
        WHERE r.to_id=$1 AND r.value=1 AND NOT u.is_banned
          AND NOT EXISTS (SELECT 1 FROM reactions q WHERE q.from_id=$1 AND q.to_id=r.from_id)
        ORDER BY r.created_at DESC LIMIT 1
        """,
        uid,
    )


async def delete_user_data(uid):
    await pool.execute("DELETE FROM reactions WHERE from_id=$1 OR to_id=$1", uid)
    await pool.execute("DELETE FROM matches WHERE a=$1 OR b=$1", uid)
    await pool.execute("DELETE FROM daily_likes WHERE user_id=$1", uid)
    await pool.execute("DELETE FROM users WHERE id=$1", uid)


# ====================== KLAVIATURALAR / MATN ======================
def main_menu(uid):
    rows = [
        [KeyboardButton(text="🔍 Qidirish"), KeyboardButton(text="💌 Menga kim layk bosdi")],
        [KeyboardButton(text="👤 Mening profilim"), KeyboardButton(text="⚙️ Sozlamalar")],
        [KeyboardButton(text="👑 VIP PREMIUM")],
    ]
    if uid in ADMIN_IDS:
        rows.append([KeyboardButton(text="🛠 Admin panel")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def gender_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="👨 Yigit"), KeyboardButton(text="👩 Qiz")]],
        resize_keyboard=True, one_time_keyboard=True,
    )


def react_kb(target_id, src):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="❤️", callback_data=f"lk:{target_id}:{src}"),
            InlineKeyboardButton(text="👎", callback_data=f"ds:{target_id}:{src}"),
        ],
        [
            InlineKeyboardButton(text="🚩 Shikoyat", callback_data=f"rp:{target_id}:{src}"),
            InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back"),
        ],
    ])


def caption(u):
    icon = "👨" if u["gender"] == "Yigit" else "👩"
    vip = " 👑" if is_vip(u) else ""
    return (
        f"<b>{esc(u['name'])}</b>{vip}, {u['age']}, {esc(u['city'])}, {icon} {u['gender']}\n\n"
        f"🎯 <b>Qiziqishlar:</b> {esc(u['interests'])}"
    )


def sub_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Kanalga o'tish", url=f"https://t.me/{CHANNEL.lstrip('@')}")],
        [InlineKeyboardButton(text="✅ Tekshirish", callback_data="chk")],
    ])


async def subscribed(bot: Bot, uid):
    if not CHANNEL or uid in ADMIN_IDS:
        return True
    try:
        mem = await bot.get_chat_member(CHANNEL, uid)
        return mem.status in ("member", "administrator", "creator")
    except Exception:
        return True  # kanal noto'g'ri sozlangan bo'lsa botni to'xtatmaymiz


async def gate(bot: Bot, uid, chat_id):
    me = await get_user(uid)
    if not me:
        await bot.send_message(chat_id, "Avval /start bosib anketa to'ldiring.")
        return None
    if not await subscribed(bot, uid):
        await bot.send_message(
            chat_id, "📢 Botdan foydalanish uchun kanalimizga obuna bo'ling:", reply_markup=sub_kb()
        )
        return None
    return me


# ====================== MIDDLEWARE (ban + flood) ======================
class Guard(BaseMiddleware):
    def __init__(self):
        self.last = {}

    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if user:
            now = time.monotonic()
            if now - self.last.get(user.id, 0) < 0.3:
                if isinstance(event, CallbackQuery):
                    await event.answer()
                return
            self.last[user.id] = now
            row = await pool.fetchrow(
                "UPDATE users SET last_seen=now() WHERE id=$1 RETURNING is_banned", user.id
            )
            if row and row["is_banned"] and user.id not in ADMIN_IDS:
                if isinstance(event, CallbackQuery):
                    await event.answer("🚫 Siz bloklangansiz.", show_alert=True)
                else:
                    await event.answer("🚫 Siz bloklangansiz.")
                return
        return await handler(event, data)


class IsAdmin(BaseFilter):
    async def __call__(self, event) -> bool:
        return bool(event.from_user and event.from_user.id in ADMIN_IDS)


admin.message.filter(IsAdmin())
admin.callback_query.filter(IsAdmin())


# ====================== RO'YXATDAN O'TISH ======================
class Reg(StatesGroup):
    name = State()
    gender = State()
    age = State()
    city = State()
    photo = State()
    interests = State()


class SetAge(StatesGroup):
    rng = State()


@router.message(CommandStart())
async def start(m: Message, state: FSMContext, bot: Bot):
    await state.clear()
    await m.answer(
        "👋 <b>AdMatch Bot</b>ga xush kelibsiz!\n\n"
        "Bu yerda yangi insonlar bilan tanishishingiz mumkin.\n\n"
        "❤️ Layk bosing: agar u ham yoqtirsa, match bo'ladi\n"
        "👎 Yoqmasa: o'tkazib yuboring\n\n"
        "Tanishuvni hoziroq boshlang!"
    )
    if await get_user(m.from_user.id):
        if not await subscribed(bot, m.from_user.id):
            return await m.answer("📢 Avval kanalga obuna bo'ling:", reply_markup=sub_kb())
        await m.answer("Menyudan tanlang 👇", reply_markup=main_menu(m.from_user.id))
    else:
        await begin_reg(m, state)


async def begin_reg(m: Message, state: FSMContext):
    await m.answer("Anketa to'ldiramiz.\n\n✏️ Ismingiz?", reply_markup=ReplyKeyboardRemove())
    await state.set_state(Reg.name)


@router.message(Reg.name, F.text)
async def reg_name(m: Message, state: FSMContext):
    name = m.text.strip()
    if not 2 <= len(name) <= 30:
        return await m.answer("Ism 2 dan 30 ta belgigacha bo'lsin.")
    await state.update_data(name=name)
    await m.answer("Jinsingiz?", reply_markup=gender_kb())
    await state.set_state(Reg.gender)


@router.message(Reg.gender, F.text)
async def reg_gender(m: Message, state: FSMContext):
    t = m.text.lower()
    if "yigit" in t:
        g = "Yigit"
    elif "qiz" in t:
        g = "Qiz"
    else:
        return await m.answer("Tugmalardan birini tanlang 👇", reply_markup=gender_kb())
    await state.update_data(gender=g)
    await m.answer("Yoshingiz?", reply_markup=ReplyKeyboardRemove())
    await state.set_state(Reg.age)


@router.message(Reg.age, F.text)
async def reg_age(m: Message, state: FSMContext):
    if not m.text.isdigit() or not 16 <= int(m.text) <= 70:
        return await m.answer("Yoshni raqam bilan yozing (16-70).")
    await state.update_data(age=int(m.text))
    await m.answer("Qaysi shahardansiz?")
    await state.set_state(Reg.city)


@router.message(Reg.city, F.text)
async def reg_city(m: Message, state: FSMContext):
    city = m.text.strip()
    if not 2 <= len(city) <= 30:
        return await m.answer("Shahar nomini to'g'ri yozing.")
    await state.update_data(city=city)
    await m.answer("📸 Rasmingizni yuboring:")
    await state.set_state(Reg.photo)


@router.message(Reg.photo, F.photo)
async def reg_photo(m: Message, state: FSMContext):
    await state.update_data(photo=m.photo[-1].file_id)
    await m.answer("🎯 Qiziqishlaringiz? (masalan: Biznes, Avtomobillar, Sport)")
    await state.set_state(Reg.interests)


@router.message(Reg.photo)
async def reg_photo_bad(m: Message):
    await m.answer("Iltimos, rasm yuboring 📸")


@router.message(Reg.interests, F.text)
async def reg_interests(m: Message, state: FSMContext):
    d = await state.get_data()
    await pool.execute(
        """
        INSERT INTO users(id,username,name,gender,age,city,photo,interests)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (id) DO UPDATE SET username=$2,name=$3,gender=$4,age=$5,
            city=$6,photo=$7,interests=$8,is_active=TRUE
        """,
        m.from_user.id, m.from_user.username, d["name"], d["gender"], d["age"],
        d["city"], d["photo"], m.text.strip()[:100],
    )
    await state.clear()
    await m.answer("✅ Anketa saqlandi!", reply_markup=main_menu(m.from_user.id))
    await show_profile(m, m.from_user.id)


# ====================== PROFIL ======================
async def show_profile(m: Message, uid):
    u = await get_user(uid)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Qayta to'ldirish", callback_data="redo")]
    ])
    status = "" if u["is_active"] else "\n\n🙈 <i>Anketangiz yashirilgan</i>"
    await m.answer_photo(u["photo"], caption=caption(u) + status, reply_markup=kb)


@router.message(F.text == "👤 Mening profilim")
async def my_profile(m: Message, bot: Bot):
    if await gate(bot, m.from_user.id, m.chat.id):
        await show_profile(m, m.from_user.id)


@router.callback_query(F.data == "redo")
async def redo(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await begin_reg(c.message, state)


# ====================== SOZLAMALAR ======================
def settings_kb(u):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🎂 Qidiruv yoshi: {u['min_age']}-{u['max_age']}", callback_data="st:age")],
        [InlineKeyboardButton(
            text="🙈 Anketani yashirish" if u["is_active"] else "👁 Anketani ko'rsatish",
            callback_data="st:vis")],
        [InlineKeyboardButton(text="🗑 Akkauntni o'chirish", callback_data="st:del")],
    ])


@router.message(F.text == "⚙️ Sozlamalar")
async def settings(m: Message, bot: Bot):
    me = await gate(bot, m.from_user.id, m.chat.id)
    if me:
        await m.answer("⚙️ <b>Sozlamalar</b>", reply_markup=settings_kb(me))


@router.callback_query(F.data == "st:age")
async def st_age(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await c.message.answer("Qidiruv uchun yosh oralig'ini yozing. Masalan: <code>18-30</code>")
    await state.set_state(SetAge.rng)


@router.message(SetAge.rng, F.text)
async def st_age_set(m: Message, state: FSMContext):
    r = re.match(r"^(\d{2})\s*[-–]\s*(\d{2})$", m.text.strip())
    if not r or not (16 <= int(r[1]) <= int(r[2]) <= 70):
        return await m.answer("Format noto'g'ri. Masalan: <code>18-30</code> (16 dan 70 gacha).")
    await pool.execute("UPDATE users SET min_age=$2, max_age=$3 WHERE id=$1",
                       m.from_user.id, int(r[1]), int(r[2]))
    await state.clear()
    await m.answer(f"✅ Endi {r[1]}-{r[2]} yoshdagilar ko'rsatiladi.")


@router.callback_query(F.data == "st:vis")
async def st_vis(c: CallbackQuery):
    u = await pool.fetchrow(
        "UPDATE users SET is_active=NOT is_active WHERE id=$1 RETURNING *", c.from_user.id)
    if not u:
        return await c.answer("Avval /start bosing", show_alert=True)
    await c.answer("Anketa ko'rinadi" if u["is_active"] else "Anketa yashirildi")
    await c.message.edit_reply_markup(reply_markup=settings_kb(u))


@router.callback_query(F.data == "st:del")
async def st_del(c: CallbackQuery):
    await c.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Ha, o'chirish", callback_data="st:delok"),
        InlineKeyboardButton(text="❌ Yo'q", callback_data="back"),
    ]])
    await c.message.answer("⚠️ Anketa va barcha layklaringiz o'chadi. Rostdan o'chiraymi?", reply_markup=kb)


@router.callback_query(F.data == "st:delok")
async def st_delok(c: CallbackQuery):
    await c.answer()
    await delete_user_data(c.from_user.id)
    await c.message.answer("🗑 Akkaunt o'chirildi. Qaytish uchun /start bosing.",
                           reply_markup=ReplyKeyboardRemove())


# ====================== QIDIRUV ======================
async def send_next(bot: Bot, chat_id, uid):
    me = await get_user(uid)
    for _ in range(5):
        p = await next_profile(me)
        if not p:
            return await bot.send_message(
                chat_id, "😔 Hozircha yangi anketalar yo'q. Keyinroq urinib ko'ring.",
                reply_markup=main_menu(uid))
        try:
            return await bot.send_photo(chat_id, p["photo"], caption=caption(p),
                                        reply_markup=react_kb(p["id"], "s"))
        except TelegramBadRequest:
            await set_reaction(uid, p["id"], 0)  # rasmi buzilgan anketani o'tkazamiz


@router.message(F.text == "🔍 Qidirish")
async def search(m: Message, bot: Bot):
    if await gate(bot, m.from_user.id, m.chat.id):
        await send_next(bot, m.chat.id, m.from_user.id)


async def show_liker(bot: Bot, chat_id, uid):
    p = await next_liker(uid)
    if not p:
        return await bot.send_message(chat_id, "Hozircha yangi layklar yo'q.",
                                      reply_markup=main_menu(uid))
    await bot.send_photo(
        chat_id, p["photo"],
        caption="❤️ <b>Sizga shu odam layk bosdi:</b>\n\n" + caption(p),
        reply_markup=react_kb(p["id"], "l"))


@router.message(F.text == "💌 Menga kim layk bosdi")
async def who_liked(m: Message, bot: Bot):
    if await gate(bot, m.from_user.id, m.chat.id):
        await show_liker(bot, m.chat.id, m.from_user.id)


async def notify_match(bot: Bot, to_user, other):
    contact = (f"@{other['username']}" if other["username"]
               else f"<a href='tg://user?id={other['id']}'>{esc(other['name'])}</a>")
    try:
        await bot.send_photo(
            to_user["id"], other["photo"],
            caption=f"🎉 <b>MATCH!</b>\nSiz va {esc(other['name'])} bir-biringizni yoqtirdingiz.\n\n"
                    f"💬 Yozing: {contact}")
    except Exception:
        pass


async def handle_like(bot: Bot, me, tid):
    uid = me["id"]
    await set_reaction(uid, tid, 1)
    target = await get_user(tid)
    if not target:
        return
    prev = await pool.fetchval(
        "SELECT value FROM reactions WHERE from_id=$1 AND to_id=$2", tid, uid)
    if prev == 1:  # o'zaro layk -> MATCH
        await pool.execute(
            "INSERT INTO matches(a,b) VALUES(LEAST($1::bigint,$2::bigint),GREATEST($1::bigint,$2::bigint)) "
            "ON CONFLICT DO NOTHING", uid, tid)
        await notify_match(bot, me, target)
        await notify_match(bot, target, me)
    elif prev is None:  # birinchi layk -> "sizga shu odam layk bosdi"
        try:
            await bot.send_photo(
                tid, me["photo"],
                caption="❤️ <b>Sizga shu odam layk bosdi:</b>\n\n" + caption(me),
                reply_markup=react_kb(uid, "n"))
        except Exception:
            pass
    # prev == 0 bo'lsa (u yoqtirmagan) hech narsa yuborilmaydi


@router.callback_query(F.data.regexp(r"^(lk|ds|rp):\d+:[snl]$"))
async def react(c: CallbackQuery, bot: Bot):
    action, tid, src = c.data.split(":")
    tid, uid = int(tid), c.from_user.id
    me = await get_user(uid)
    if not me:
        return await c.answer("Avval /start bosing", show_alert=True)
    if action == "lk" and not await like_allowed(uid, me):
        return await c.answer(
            f"❌ Bugungi layk limiti ({DAILY_LIKES} ta) tugadi.\n👑 VIP bilan cheklovsiz!",
            show_alert=True)
    await c.answer()
    try:
        await c.message.delete()
    except Exception:
        pass

    if action == "ds":
        await set_reaction(uid, tid, 0)  # yoqmasa hech narsa yuborilmaydi
    elif action == "rp":
        await pool.execute("INSERT INTO reports(from_id,to_id) VALUES($1,$2)", uid, tid)
        await set_reaction(uid, tid, 0)
        for a in ADMIN_IDS:
            try:
                await bot.send_message(a, f"🚩 Yangi shikoyat. ID <code>{tid}</code> ustidan. /admin → Shikoyatlar")
            except Exception:
                pass
        await bot.send_message(c.message.chat.id, "🚩 Shikoyat yuborildi, rahmat.")
    else:
        await handle_like(bot, me, tid)

    if src == "s":
        await send_next(bot, c.message.chat.id, uid)
    elif src == "l":
        await show_liker(bot, c.message.chat.id, uid)


@router.callback_query(F.data == "back")
async def back(c: CallbackQuery):
    await c.answer()
    try:
        await c.message.delete()
    except Exception:
        pass
    await c.message.answer("Menyudan tanlang 👇", reply_markup=main_menu(c.from_user.id))


@router.callback_query(F.data == "chk")
async def check_sub(c: CallbackQuery, bot: Bot):
    if await subscribed(bot, c.from_user.id):
        await c.answer("✅ Rahmat!")
        try:
            await c.message.delete()
        except Exception:
            pass
        await c.message.answer("Menyudan tanlang 👇", reply_markup=main_menu(c.from_user.id))
    else:
        await c.answer("Hali obuna bo'lmadingiz ❌", show_alert=True)


# ====================== VIP ======================
@router.message(F.text == "👑 VIP PREMIUM")
async def vip(m: Message, bot: Bot):
    me = await gate(bot, m.from_user.id, m.chat.id)
    if not me:
        return
    status = (f"✅ Faol: {me['vip_until']:%d.%m.%Y} gacha" if is_vip(me) else "❌ Faol emas")
    await m.answer(
        "👑 <b>VIP PREMIUM</b>\n\n"
        "✅ Cheklovsiz layk\n"
        "✅ Anketangiz qidiruvda birinchi ko'rsatiladi\n"
        "✅ Ismingiz yonida 👑 belgisi\n\n"
        f"Bepul rejada kuniga {DAILY_LIKES} ta layk.\n"
        f"Holat: {status}\n\n"
        f"VIP olish uchun {esc(VIP_CONTACT)} ga yozing va ID raqamingizni yuboring:\n"
        f"<code>{m.from_user.id}</code>")


# ====================== ADMIN PANEL ======================
class Bc(StatesGroup):
    content = State()
    confirm = State()


class AdmUser(StatesGroup):
    query = State()


def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Statistika", callback_data="adm:stats"),
         InlineKeyboardButton(text="📢 Xabar yuborish", callback_data="adm:bc")],
        [InlineKeyboardButton(text="🔎 Foydalanuvchi", callback_data="adm:user"),
         InlineKeyboardButton(text="🚩 Shikoyatlar", callback_data="adm:rep")],
        [InlineKeyboardButton(text="📥 CSV eksport", callback_data="adm:csv")],
    ])


@admin.message(Command("admin"))
@admin.message(F.text == "🛠 Admin panel")
async def admin_panel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("🛠 <b>Admin panel</b>", reply_markup=admin_kb())


# ---- statistika
@admin.callback_query(F.data == "adm:stats")
async def adm_stats(c: CallbackQuery):
    await c.answer()
    s = await pool.fetchrow("""
        SELECT count(*) total,
          count(*) FILTER (WHERE gender='Yigit') boys,
          count(*) FILTER (WHERE gender='Qiz') girls,
          count(*) FILTER (WHERE created_at >= date_trunc('day', now())) today,
          count(*) FILTER (WHERE last_seen >= now() - interval '24 hours') active24,
          count(*) FILTER (WHERE vip_until > now()) vip,
          count(*) FILTER (WHERE is_banned) banned,
          count(*) FILTER (WHERE NOT is_active) hidden
        FROM users""")
    r = await pool.fetchrow("""
        SELECT count(*) FILTER (WHERE value=1) likes,
               count(*) FILTER (WHERE value=0) dislikes FROM reactions""")
    matches = await pool.fetchval("SELECT count(*) FROM matches")
    reports = await pool.fetchval("SELECT count(*) FROM reports WHERE NOT handled")
    cities = await pool.fetch(
        "SELECT initcap(lower(city)) c, count(*) n FROM users GROUP BY 1 ORDER BY n DESC LIMIT 5")
    top = "\n".join(f"  • {esc(x['c'])}: {x['n']}" for x in cities) or "  —"
    await c.message.answer(
        "📊 <b>Statistika</b>\n\n"
        f"👥 Jami: <b>{s['total']}</b>\n"
        f"👨 Yigitlar: {s['boys']}   👩 Qizlar: {s['girls']}\n"
        f"🆕 Bugun qo'shilgan: {s['today']}\n"
        f"🟢 So'nggi 24 soatda faol: {s['active24']}\n"
        f"👑 VIP: {s['vip']}\n"
        f"🚫 Bloklangan: {s['banned']}   🙈 Yashirin: {s['hidden']}\n\n"
        f"❤️ Laykler: {r['likes']}   👎 Dislaykler: {r['dislikes']}\n"
        f"🎉 Matchlar: {matches}\n"
        f"🚩 Ko'rilmagan shikoyatlar: {reports}\n\n"
        f"🏙 Top shaharlar:\n{top}")


# ---- xabar yuborish (rassilka)
@admin.callback_query(F.data == "adm:bc")
async def adm_bc(c: CallbackQuery):
    await c.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Hammaga", callback_data="bc:t:all")],
        [InlineKeyboardButton(text="👩 Qizlarga", callback_data="bc:t:Qiz"),
         InlineKeyboardButton(text="👨 Yigitlarga", callback_data="bc:t:Yigit")],
    ])
    await c.message.answer("Kimga yuboramiz?", reply_markup=kb)


@admin.callback_query(F.data.startswith("bc:t:"))
async def bc_target(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.update_data(target=c.data.split(":")[2])
    await state.set_state(Bc.content)
    await c.message.answer("Yuboriladigan xabarni jo'nating (matn, rasm yoki video):")


@admin.message(Bc.content)
async def bc_content(m: Message, state: FSMContext, bot: Bot):
    await state.update_data(chat_id=m.chat.id, msg_id=m.message_id)
    await state.set_state(Bc.confirm)
    await m.answer("👆 Shu xabar yuboriladi. Tasdiqlaysizmi?", reply_markup=InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="✅ Yuborish", callback_data="bc:go"),
            InlineKeyboardButton(text="❌ Bekor", callback_data="bc:no"),
        ]]))


@admin.callback_query(Bc.confirm, F.data == "bc:no")
async def bc_no(c: CallbackQuery, state: FSMContext):
    await c.answer("Bekor qilindi")
    await state.clear()


@admin.callback_query(Bc.confirm, F.data == "bc:go")
async def bc_go(c: CallbackQuery, state: FSMContext, bot: Bot):
    d = await state.get_data()
    await state.clear()
    await c.answer("Yuborish boshlandi")
    await c.message.answer("📤 Yuborilmoqda... tugagach xabar beraman.")
    asyncio.create_task(run_broadcast(bot, c.from_user.id, d["target"], d["chat_id"], d["msg_id"]))


async def run_broadcast(bot: Bot, admin_id, target, chat_id, msg_id):
    if target == "all":
        rows = await pool.fetch("SELECT id FROM users WHERE NOT is_banned")
    else:
        rows = await pool.fetch("SELECT id FROM users WHERE NOT is_banned AND gender=$1", target)
    ok = fail = 0
    for r in rows:
        for _ in range(2):
            try:
                await bot.copy_message(r["id"], chat_id, msg_id)
                ok += 1
                break
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after)
            except Exception:
                fail += 1
                break
        await asyncio.sleep(0.05)
    await bot.send_message(admin_id, f"✅ Xabar yuborildi.\nYetkazildi: {ok}\nYetmadi: {fail}")


# ---- foydalanuvchi qidirish va boshqarish
def user_kb(u):
    uid = u["id"]
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="✅ Blokdan chiqarish" if u["is_banned"] else "🚫 Bloklash",
            callback_data=f"au:{'unban' if u['is_banned'] else 'ban'}:{uid}")],
        [InlineKeyboardButton(text="👑 +30 kun VIP", callback_data=f"au:vip30:{uid}"),
         InlineKeyboardButton(text="👑 +90 kun", callback_data=f"au:vip90:{uid}")],
        [InlineKeyboardButton(text="❎ VIP ni olish", callback_data=f"au:vipoff:{uid}"),
         InlineKeyboardButton(text="🗑 O'chirish", callback_data=f"au:del:{uid}")],
    ])


def admin_card(u):
    vip = f"✅ {u['vip_until']:%d.%m.%Y} gacha" if is_vip(u) else "❌"
    return (
        f"🆔 <code>{u['id']}</code>  @{esc(u['username'] or '—')}\n\n"
        f"{caption(u)}\n\n"
        f"👑 VIP: {vip}\n"
        f"🚫 Bloklangan: {'ha' if u['is_banned'] else 'yoq'}\n"
        f"👁 Ko'rinadi: {'ha' if u['is_active'] else 'yoq'}\n"
        f"📅 Qo'shilgan: {u['created_at']:%d.%m.%Y}")


@admin.callback_query(F.data == "adm:user")
async def adm_user(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.set_state(AdmUser.query)
    await c.message.answer("Foydalanuvchi ID raqami yoki @username ini yozing:")


@admin.message(AdmUser.query, F.text)
async def adm_user_find(m: Message, state: FSMContext):
    q = m.text.strip().lstrip("@")
    if q.isdigit():
        u = await get_user(int(q))
    else:
        u = await pool.fetchrow("SELECT * FROM users WHERE lower(username)=lower($1)", q)
    await state.clear()
    if not u:
        return await m.answer("❌ Topilmadi.")
    await m.answer_photo(u["photo"], caption=admin_card(u), reply_markup=user_kb(u))


@admin.callback_query(F.data.regexp(r"^au:(ban|unban|vip30|vip90|vipoff|del):\d+$"))
async def adm_user_action(c: CallbackQuery, bot: Bot):
    _, act, uid = c.data.split(":")
    uid = int(uid)
    if act == "ban":
        await pool.execute("UPDATE users SET is_banned=TRUE WHERE id=$1", uid)
    elif act == "unban":
        await pool.execute("UPDATE users SET is_banned=FALSE WHERE id=$1", uid)
    elif act in ("vip30", "vip90"):
        days = 30 if act == "vip30" else 90
        await pool.execute(
            "UPDATE users SET vip_until=GREATEST(COALESCE(vip_until, now()), now()) + $2 * interval '1 day' "
            "WHERE id=$1", uid, days)
        try:
            await bot.send_message(uid, f"👑 Tabriklaymiz! Sizga {days} kunlik VIP berildi.")
        except Exception:
            pass
    elif act == "vipoff":
        await pool.execute("UPDATE users SET vip_until=NULL WHERE id=$1", uid)
    elif act == "del":
        await delete_user_data(uid)
        await c.answer("O'chirildi")
        return await c.message.edit_caption(caption="🗑 Foydalanuvchi o'chirildi.")
    await c.answer("Bajarildi ✅")
    u = await get_user(uid)
    if u:
        await c.message.edit_caption(caption=admin_card(u), reply_markup=user_kb(u))


# ---- shikoyatlar
async def show_report(bot: Bot, chat_id):
    r = await pool.fetchrow("SELECT * FROM reports WHERE NOT handled ORDER BY id LIMIT 1")
    if not r:
        return await bot.send_message(chat_id, "✅ Yangi shikoyatlar yo'q.")
    u = await get_user(r["to_id"])
    if not u:
        await pool.execute("UPDATE reports SET handled=TRUE WHERE to_id=$1", r["to_id"])
        return await show_report(bot, chat_id)
    cnt = await pool.fetchval("SELECT count(*) FROM reports WHERE to_id=$1", u["id"])
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🚫 Bloklash", callback_data=f"ar:ban:{u['id']}"),
        InlineKeyboardButton(text="✅ Rad etish", callback_data=f"ar:ok:{u['id']}"),
    ]])
    await bot.send_photo(
        chat_id, u["photo"],
        caption=f"🚩 <b>Shikoyat</b> (shu odamga jami {cnt} ta)\n"
                f"Shikoyatchi: <code>{r['from_id']}</code>\n\n" + admin_card(u),
        reply_markup=kb)


@admin.callback_query(F.data == "adm:rep")
async def adm_rep(c: CallbackQuery, bot: Bot):
    await c.answer()
    await show_report(bot, c.message.chat.id)


@admin.callback_query(F.data.regexp(r"^ar:(ban|ok):\d+$"))
async def adm_rep_action(c: CallbackQuery, bot: Bot):
    _, act, uid = c.data.split(":")
    uid = int(uid)
    if act == "ban":
        await pool.execute("UPDATE users SET is_banned=TRUE WHERE id=$1", uid)
    await pool.execute("UPDATE reports SET handled=TRUE WHERE to_id=$1", uid)
    await c.answer("Bajarildi ✅")
    try:
        await c.message.delete()
    except Exception:
        pass
    await show_report(bot, c.message.chat.id)


# ---- CSV eksport
@admin.callback_query(F.data == "adm:csv")
async def adm_csv(c: CallbackQuery):
    await c.answer()
    rows = await pool.fetch("SELECT * FROM users ORDER BY created_at")
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["id", "username", "name", "gender", "age", "city", "interests",
                "vip_until", "banned", "active", "created_at"])
    for u in rows:
        w.writerow([u["id"], u["username"], u["name"], u["gender"], u["age"], u["city"],
                    u["interests"], u["vip_until"], u["is_banned"], u["is_active"], u["created_at"]])
    data = buf.getvalue().encode("utf-8-sig")
    await c.message.answer_document(
        BufferedInputFile(data, filename=f"users_{datetime.now():%Y%m%d}.csv"),
        caption=f"👥 Jami: {len(rows)}")


# ====================== ISHGA TUSHIRISH ======================
async def health(request):
    return web.Response(text="AdMatch Bot ishlayapti")


async def keep_alive():
    """Render bepul rejada uxlab qolmasligi uchun o'zini ping qiladi."""
    url = os.getenv("RENDER_EXTERNAL_URL")
    if not url:
        return
    async with ClientSession() as s:
        while True:
            await asyncio.sleep(600)
            try:
                await s.get(url)
            except Exception:
                pass


async def main():
    global pool
    logging.basicConfig(level=logging.INFO)
    if not TOKEN or not DATABASE_URL:
        raise SystemExit("BOT_TOKEN va DATABASE_URL o'rnatilmagan!")

    ssl = "require" if "render.com" in DATABASE_URL else None
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10, ssl=ssl)
    await pool.execute(SCHEMA)

    bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    guard = Guard()
    dp.message.outer_middleware(guard)
    dp.callback_query.outer_middleware(guard)
    dp.include_router(admin)   # admin birinchi
    dp.include_router(router)

    # Render Web Service port talab qiladi
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    asyncio.create_task(keep_alive())

    await bot.set_my_commands([BotCommand(command="start", description="Botni boshlash")])
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
