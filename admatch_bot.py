"""
AdMatch Bot PRO v5 - Telegram tanishuv boti (VIP PREMIUM to'lov tizimi bilan)
Stack: aiogram 3 + PostgreSQL (asyncpg) + aiohttp (Render Web Service uchun)

Muhit o'zgaruvchilari (Render -> Environment):
  BOT_TOKEN     - BotFather tokeni (majburiy)
  DATABASE_URL  - Postgres manzili (majburiy)
  ADMIN_IDS     - admin Telegram ID lari, vergul bilan (12345,67890)  (majburiy: cheklar adminlarga boradi)
  CHANNEL       - majburiy obuna kanali (@kanal), ixtiyoriy
  DAILY_LIKES   - bepul foydalanuvchi uchun kunlik layk (standart 30)
  VIP_CONTACT   - yordam uchun akkaunt (@admin)

Karta raqami, karta egasi, narxlar, referal va promokod bonuslari admin paneldan
(/admin -> Sozlamalar) o'zgartiriladi.

YANGI (v5):
  * Standart karta: 6262 5701 3419 2691 (Boratova Zarnigor)
  * Promokod bilan to'langanda bonus VIP kunlar: 1 hafta +3, 1 oy +4, 3 oy +5
"""
import asyncio
import csv
import html
import io
import logging
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import quote

import asyncpg
from aiohttp import ClientSession, web
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter,
)
from aiogram.filters import BaseFilter, Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand, BufferedInputFile, CallbackQuery, ErrorEvent,
    InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, Message,
    ReplyKeyboardMarkup, ReplyKeyboardRemove,
)

# ====================== SOZLAMALAR ======================
TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_IDS = {int(x) for x in re.findall(r"\d+", os.getenv("ADMIN_IDS", ""))}
PORT = int(os.getenv("PORT", "10000"))
DB_SCHEMA = "admatch"

DEFAULTS = {
    "daily_likes": os.getenv("DAILY_LIKES", "30"),
    "vip_contact": os.getenv("VIP_CONTACT", "@admin"),
    "channel": os.getenv("CHANNEL", "").strip(),
    "ref_bonus": "10",
    "ref_vip_days": "3",       # do'sti VIP sotib olsa, taklif qiluvchiga beriladigan VIP kunlar
    "super_daily": "5",        # VIP uchun kunlik super layk limiti
    "maintenance": "0",
    "welcome": "Bu yerda yangi insonlar bilan tanishishingiz mumkin.",
    "price_week": "35000",
    "price_month": "65000",
    "price_quarter": "170000",
    "card_number": "6262 5701 3419 2691",
    "card_owner": "Boratova Zarnigor",
    # promokod bilan to'lansa, qo'shimcha sovg'a VIP kunlar
    "promo_bonus_week": "3",
    "promo_bonus_month": "4",
    "promo_bonus_quarter": "5",
}
CFG = dict(DEFAULTS)  # DB dagi qiymatlar bilan almashtiriladi

# tarif kaliti: (kunlar, tugma matni)
PLANS = {
    "week": (7, "⏳ 1 hafta"),
    "month": (30, "📅 1 oy"),
    "quarter": (90, "💎 3 oy"),
}
PLAN_RE = "(week|month|quarter)"

REJECT_REASONS = {
    "1": "Chek aniq emas yoki noto'g'ri",
    "2": "To'lov summasi mos kelmadi",
    "3": "To'lov hisobimizga tushmadi",
    "4": "Chek takroriy yoki haqiqiy emas",
}
STATUS_TXT = {"pending": "⏳ kutilmoqda", "approved": "✅ tasdiqlangan", "rejected": "❌ rad etilgan"}
MAX_PAY_TRIES_24H = 5

pool: asyncpg.Pool = None
BOT_USERNAME = ""
BC_BUSY = False
router = Router()
admin = Router()
esc = html.escape
log = logging.getLogger("admatch")


def cfg_int(key):
    try:
        return int(CFG[key])
    except (ValueError, KeyError):
        return int(DEFAULTS[key])


def money(v):
    try:
        return f"{int(v):,}".replace(",", " ")
    except (ValueError, TypeError):
        return str(v)


def plan_price(plan, percent=0):
    base = cfg_int(f"price_{plan}")
    return max(base * (100 - percent) // 100, 0)


def promo_bonus(plan):
    """Promokod bilan to'langanda beriladigan qo'shimcha kunlar."""
    return cfg_int(f"promo_bonus_{plan}")


def contact_url():
    return f"https://t.me/{CFG['vip_contact'].lstrip('@')}"


# Har bir statement alohida ishga tushiriladi (pgbouncer bilan xavfsizroq)
SCHEMA_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS users(
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
    )""",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS bio TEXT DEFAULT ''",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS looking_for TEXT",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS bonus_likes INT DEFAULT 0",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by BIGINT",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS blocked_bot BOOLEAN DEFAULT FALSE",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS vip_warned BOOLEAN DEFAULT FALSE",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_reminded TIMESTAMPTZ",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS vip_expired_sent BOOLEAN DEFAULT FALSE",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS promo_applied TEXT",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_bonus DATE",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS streak INT DEFAULT 0",
    """CREATE TABLE IF NOT EXISTS reactions(
        from_id BIGINT, to_id BIGINT, value SMALLINT,
        created_at TIMESTAMPTZ DEFAULT now(),
        PRIMARY KEY(from_id, to_id)
    )""",
    """CREATE TABLE IF NOT EXISTS matches(
        a BIGINT, b BIGINT, created_at TIMESTAMPTZ DEFAULT now(),
        PRIMARY KEY(a, b)
    )""",
    """CREATE TABLE IF NOT EXISTS reports(
        id SERIAL PRIMARY KEY, from_id BIGINT, to_id BIGINT,
        created_at TIMESTAMPTZ DEFAULT now(), handled BOOLEAN DEFAULT FALSE
    )""",
    """CREATE TABLE IF NOT EXISTS daily_likes(
        user_id BIGINT, day DATE, cnt INT DEFAULT 0,
        PRIMARY KEY(user_id, day)
    )""",
    """CREATE TABLE IF NOT EXISTS daily_super(
        user_id BIGINT, day DATE, cnt INT DEFAULT 0,
        PRIMARY KEY(user_id, day)
    )""",
    "CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT)",
    """CREATE TABLE IF NOT EXISTS admin_logs(
        id SERIAL PRIMARY KEY, admin_id BIGINT, action TEXT, target_id BIGINT,
        details TEXT, created_at TIMESTAMPTZ DEFAULT now()
    )""",
    """CREATE TABLE IF NOT EXISTS vip_log(
        id SERIAL PRIMARY KEY, user_id BIGINT, days INT, admin_id BIGINT,
        created_at TIMESTAMPTZ DEFAULT now()
    )""",
    # ---- to'lovlar
    """CREATE TABLE IF NOT EXISTS payments(
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        plan TEXT NOT NULL,
        days INT NOT NULL,
        amount BIGINT NOT NULL,
        promo_code TEXT,
        discount INT DEFAULT 0,
        file_id TEXT NOT NULL,
        file_uid TEXT NOT NULL,
        file_type TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        admin_id BIGINT,
        reason TEXT,
        admin_reminded BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMPTZ DEFAULT now(),
        decided_at TIMESTAMPTZ
    )""",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_pay_pending ON payments(user_id) WHERE status='pending'",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_pay_file ON payments(file_uid) WHERE status IN ('pending','approved')",
    "CREATE INDEX IF NOT EXISTS idx_pay_status ON payments(status, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_pay_user ON payments(user_id)",
    """CREATE TABLE IF NOT EXISTS payment_msgs(
        payment_id INT, admin_id BIGINT, message_id BIGINT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_paymsgs ON payment_msgs(payment_id)",
    # ---- promokodlar
    """CREATE TABLE IF NOT EXISTS promos(
        code TEXT PRIMARY KEY,
        percent INT NOT NULL,
        max_uses INT DEFAULT 0,
        used INT DEFAULT 0,
        expires_at TIMESTAMPTZ,
        active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMPTZ DEFAULT now()
    )""",
    """CREATE TABLE IF NOT EXISTS promo_uses(
        code TEXT, user_id BIGINT, created_at TIMESTAMPTZ DEFAULT now(),
        PRIMARY KEY(code, user_id)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_reactions_to ON reactions(to_id, value)",
    "CREATE INDEX IF NOT EXISTS idx_reactions_from ON reactions(from_id)",
    "CREATE INDEX IF NOT EXISTS idx_matches_b ON matches(b)",
    "CREATE INDEX IF NOT EXISTS idx_users_search ON users(is_active, is_banned, gender, age)",
    "CREATE INDEX IF NOT EXISTS idx_users_city ON users(lower(city))",
    "CREATE INDEX IF NOT EXISTS idx_users_vip ON users(vip_until)",
    "CREATE INDEX IF NOT EXISTS idx_users_seen ON users(last_seen)",
    "CREATE INDEX IF NOT EXISTS idx_users_username ON users(lower(username))",
    "CREATE INDEX IF NOT EXISTS idx_users_ref ON users(referred_by)",
    "CREATE INDEX IF NOT EXISTS idx_reports_open ON reports(handled, id)",
]


# ====================== DB YORDAMCHILARI ======================
async def get_user(uid):
    return await pool.fetchrow("SELECT * FROM users WHERE id=$1", uid)


def is_vip(u):
    return bool(u and u["vip_until"] and u["vip_until"] > datetime.now(timezone.utc))


def opposite(g):
    return "Qiz" if g == "Yigit" else "Yigit"


async def load_cfg():
    for r in await pool.fetch("SELECT key, value FROM settings"):
        CFG[r["key"]] = r["value"]


async def set_cfg(key, value):
    CFG[key] = value
    await pool.execute(
        "INSERT INTO settings(key,value) VALUES($1,$2) "
        "ON CONFLICT (key) DO UPDATE SET value=$2", key, value)


async def log_admin(admin_id, action, target=None, details=""):
    await pool.execute(
        "INSERT INTO admin_logs(admin_id,action,target_id,details) VALUES($1,$2,$3,$4)",
        admin_id, action, target, details)


async def set_reaction(a, b, v):
    await pool.execute(
        "INSERT INTO reactions(from_id,to_id,value) VALUES($1,$2,$3) "
        "ON CONFLICT (from_id,to_id) DO UPDATE SET value=$3, created_at=now()", a, b, v)


async def like_allowed(me):
    """VIP cheklovsiz. Boshqalarga: kunlik limit, keyin bonus layklar (atomik)."""
    if is_vip(me):
        return True
    uid, limit = me["id"], cfg_int("daily_likes")
    if limit > 0:
        row = await pool.fetchrow(
            "INSERT INTO daily_likes(user_id,day,cnt) VALUES($1,CURRENT_DATE,1) "
            "ON CONFLICT (user_id,day) DO UPDATE SET cnt=daily_likes.cnt+1 "
            "WHERE daily_likes.cnt<$2 RETURNING cnt", uid, limit)
        if row:
            return True
    ok = await pool.fetchval(
        "UPDATE users SET bonus_likes=bonus_likes-1 WHERE id=$1 AND bonus_likes>0 RETURNING TRUE", uid)
    return bool(ok)


async def super_allowed(uid):
    """VIP uchun kunlik super layk limiti (atomik)."""
    limit = cfg_int("super_daily")
    if limit <= 0:
        return False
    row = await pool.fetchrow(
        "INSERT INTO daily_super(user_id,day,cnt) VALUES($1,CURRENT_DATE,1) "
        "ON CONFLICT (user_id,day) DO UPDATE SET cnt=daily_super.cnt+1 "
        "WHERE daily_super.cnt<$2 RETURNING cnt", uid, limit)
    return bool(row)


async def likes_left(me):
    if is_vip(me):
        return "cheksiz"
    used = await pool.fetchval(
        "SELECT cnt FROM daily_likes WHERE user_id=$1 AND day=CURRENT_DATE", me["id"]) or 0
    bonus = await pool.fetchval("SELECT bonus_likes FROM users WHERE id=$1", me["id"]) or 0
    return str(max(cfg_int("daily_likes") - used, 0) + bonus)


async def next_profile(me):
    want = me["looking_for"] or opposite(me["gender"])
    return await pool.fetchrow(
        """
        SELECT x.* FROM users x
        WHERE ($1='all' OR x.gender=$1) AND x.id<>$2 AND x.is_active AND NOT x.is_banned
          AND x.age BETWEEN $3 AND $4
          AND COALESCE(x.looking_for, CASE WHEN x.gender='Yigit' THEN 'Qiz' ELSE 'Yigit' END)
              IN ('all', $6)
          AND NOT EXISTS (SELECT 1 FROM reactions r WHERE r.from_id=$2 AND r.to_id=x.id)
        ORDER BY (x.vip_until IS NOT NULL AND x.vip_until>now()) DESC,
                 (lower(x.city)=lower($5)) DESC,
                 random()
        LIMIT 1
        """,
        want, me["id"], me["min_age"], me["max_age"], me["city"], me["gender"],
    )


async def next_liker(uid):
    return await pool.fetchrow(
        """
        SELECT u.* FROM reactions r JOIN users u ON u.id=r.from_id
        WHERE r.to_id=$1 AND r.value=1 AND NOT u.is_banned
          AND NOT EXISTS (SELECT 1 FROM reactions q WHERE q.from_id=$1 AND q.to_id=r.from_id)
        ORDER BY r.created_at DESC LIMIT 1
        """, uid)


async def delete_user_data(uid):
    async with pool.acquire() as con:
        async with con.transaction():
            await con.execute("DELETE FROM reactions WHERE from_id=$1 OR to_id=$1", uid)
            await con.execute("DELETE FROM matches WHERE a=$1 OR b=$1", uid)
            await con.execute("DELETE FROM daily_likes WHERE user_id=$1", uid)
            await con.execute("DELETE FROM daily_super WHERE user_id=$1", uid)
            await con.execute("UPDATE reports SET handled=TRUE WHERE to_id=$1 OR from_id=$1", uid)
            await con.execute("UPDATE users SET referred_by=NULL WHERE referred_by=$1", uid)
            await con.execute("DELETE FROM users WHERE id=$1", uid)
    # to'lovlar tarixi (moliyaviy hisobot uchun) saqlanib qoladi


async def apply_vip(con, uid, days, admin_id):
    """VIP muddatini uzaytiradi (tranzaksiya ichida ishlatish uchun)."""
    await con.execute(
        "UPDATE users SET vip_until=GREATEST(COALESCE(vip_until, now()), now()) + $2 * interval '1 day', "
        "vip_warned=FALSE, vip_expired_sent=FALSE WHERE id=$1", uid, days)
    await con.execute("INSERT INTO vip_log(user_id,days,admin_id) VALUES($1,$2,$3)", uid, days, admin_id)


async def grant_vip(bot, admin_id, uid, days, notify=True):
    async with pool.acquire() as con:
        async with con.transaction():
            await apply_vip(con, uid, days, admin_id)
    await log_admin(admin_id, "vip", uid, f"+{days} kun")
    if notify:
        try:
            await bot.send_message(uid, f"👑 Tabriklaymiz! Sizga {days} kunlik VIP berildi.")
        except Exception:
            pass


# ====================== PROMOKOD ======================
async def promo_check(code, uid):
    """(promo_row, None) yoki (None, xato_matni)"""
    p = await pool.fetchrow("SELECT * FROM promos WHERE code=$1", code)
    if not p or not p["active"]:
        return None, "Bunday promokod topilmadi"
    if p["expires_at"] and p["expires_at"] < datetime.now(timezone.utc):
        return None, "Promokod muddati tugagan"
    if p["max_uses"] and p["used"] >= p["max_uses"]:
        return None, "Promokod limiti tugagan"
    if await pool.fetchval("SELECT 1 FROM promo_uses WHERE code=$1 AND user_id=$2", code, uid):
        return None, "Siz bu promokoddan allaqachon foydalangansiz"
    return p, None


async def promo_percent(uid):
    """Foydalanuvchida qo'llangan promokod: (kod, foiz) yoki (None, 0)."""
    code = await pool.fetchval("SELECT promo_applied FROM users WHERE id=$1", uid)
    if not code:
        return None, 0
    p, err = await promo_check(code, uid)
    if err:
        await pool.execute("UPDATE users SET promo_applied=NULL WHERE id=$1", uid)
        return None, 0
    return code, p["percent"]


# ====================== KLAVIATURALAR / MATN ======================
B_SEARCH = "🔍 Qidirish"
B_LIKES = "💌 Menga kim layk bosdi"
B_MATCH = "💞 Matchlarim"
B_PROFILE = "👤 Mening profilim"
B_SET = "⚙️ Sozlamalar"
B_VIP = "👑 VIP PREMIUM"
B_INV = "🎁 Do'st taklif qilish"
B_ADMIN = "🛠 Admin panel"
MENU_TEXTS = {B_SEARCH, B_LIKES, B_MATCH, B_PROFILE, B_SET, B_VIP, B_INV, B_ADMIN}
B_SKIP = "⏭ O'tkazib yuborish"


def btn(text, data=None, url=None):
    return InlineKeyboardButton(text=text, callback_data=data, url=url)


def kb(*rows):
    return InlineKeyboardMarkup(inline_keyboard=[list(r) for r in rows])


def main_menu(uid):
    rows = [
        [KeyboardButton(text=B_SEARCH), KeyboardButton(text=B_LIKES)],
        [KeyboardButton(text=B_MATCH), KeyboardButton(text=B_PROFILE)],
        [KeyboardButton(text=B_SET), KeyboardButton(text=B_VIP)],
        [KeyboardButton(text=B_INV)],
    ]
    if uid in ADMIN_IDS:  # faqat adminlarga ko'rinadi
        rows.append([KeyboardButton(text=B_ADMIN)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def gender_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="👨 Yigit"), KeyboardButton(text="👩 Qiz")]],
        resize_keyboard=True, one_time_keyboard=True)


def skip_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=B_SKIP)]], resize_keyboard=True,
                               one_time_keyboard=True)


def react_kb(target_id, src):
    return kb(
        [btn("❤️", f"lk:{target_id}:{src}"), btn("⭐", f"sl:{target_id}:{src}"),
         btn("👎", f"ds:{target_id}:{src}")],
        [btn("💬 Lichkasiga yozish", f"dm:{target_id}")],
        [btn("🚩 Shikoyat", f"rp:{target_id}:{src}"), btn("⬅️ Orqaga", "back")],
    )


def caption(u):
    icon = "👨" if u["gender"] == "Yigit" else "👩"
    vip = " 👑" if is_vip(u) else ""
    bio = f"\n💬 {esc(u['bio'])}" if u["bio"] else ""
    return (
        f"<b>{esc(u['name'])}</b>{vip}, {u['age']}, {esc(u['city'])}, {icon} {u['gender']}\n"
        f"{bio}\n"
        f"🎯 <b>Qiziqishlar:</b> {esc(u['interests'])}"
    )


def contact_of(u):
    if u["username"]:
        return f"@{esc(u['username'])}"
    return f"<a href='tg://user?id={u['id']}'>{esc(u['name'])}</a>"


def sub_kb():
    ch = CFG["channel"].lstrip("@")
    return kb([btn("📢 Kanalga o'tish", url=f"https://t.me/{ch}")], [btn("✅ Tekshirish", "chk")])


async def subscribed(bot: Bot, uid):
    if not CFG["channel"] or uid in ADMIN_IDS:
        return True
    try:
        mem = await bot.get_chat_member(CFG["channel"], uid)
        return mem.status in ("member", "administrator", "creator")
    except Exception:
        return True  # kanal noto'g'ri sozlangan bo'lsa botni to'xtatmaymiz


async def gate(bot: Bot, uid, chat_id):
    me = await get_user(uid)
    if not me:
        await bot.send_message(chat_id, "Avval /start bosib anketa to'ldiring.")
        return None
    if not await subscribed(bot, uid):
        await bot.send_message(chat_id, "📢 Botdan foydalanish uchun kanalimizga obuna bo'ling:",
                               reply_markup=sub_kb())
        return None
    return me


# ====================== MIDDLEWARE (ban + flood + texnik rejim) ======================
class Guard(BaseMiddleware):
    def __init__(self):
        self.last = {}

    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if not user:
            return await handler(event, data)
        now = time.monotonic()
        if now - self.last.get(user.id, 0) < 0.3:
            if isinstance(event, CallbackQuery):
                await event.answer()
            return
        if len(self.last) > 20000:
            self.last.clear()
        self.last[user.id] = now

        is_adm = user.id in ADMIN_IDS
        if CFG["maintenance"] == "1" and not is_adm:
            txt = "🛠 Botda texnik ishlar ketmoqda. Birozdan so'ng urinib ko'ring."
            if isinstance(event, CallbackQuery):
                await event.answer(txt, show_alert=True)
            else:
                await event.answer(txt)
            return

        row = await pool.fetchrow(
            "UPDATE users SET last_seen=now(), blocked_bot=FALSE WHERE id=$1 RETURNING is_banned", user.id)
        if row and row["is_banned"] and not is_adm:
            if isinstance(event, CallbackQuery):
                await event.answer("🚫 Siz bloklangansiz.", show_alert=True)
            else:
                await event.answer("🚫 Siz bloklangansiz.")
            return

        # menyu tugmasi bosilsa, yarim qolgan holatni bekor qilamiz
        st = data.get("state")
        if isinstance(event, Message) and event.text in MENU_TEXTS and st and await st.get_state():
            await st.clear()
        return await handler(event, data)


class IsAdmin(BaseFilter):
    async def __call__(self, event) -> bool:
        return bool(event.from_user and event.from_user.id in ADMIN_IDS)


admin.message.filter(IsAdmin())
admin.callback_query.filter(IsAdmin())


# ====================== HOLATLAR ======================
class Reg(StatesGroup):
    name = State()
    gender = State()
    age = State()
    city = State()
    photo = State()
    interests = State()
    bio = State()


class Edit(StatesGroup):
    value = State()


class SetAge(StatesGroup):
    rng = State()


class VipSearch(StatesGroup):
    q = State()


class Pay(StatesGroup):
    receipt = State()


class PromoS(StatesGroup):
    code = State()


def validate(field, v):
    v = v.strip()
    if field in ("name", "city") and 2 <= len(v) <= 30:
        return v
    if field == "age" and v.isdigit() and 16 <= int(v) <= 70:
        return int(v)
    if field == "interests" and 2 <= len(v) <= 100:
        return v
    if field == "bio" and len(v) <= 150:
        return "" if v in ("-", B_SKIP) else v
    return None


# ====================== RO'YXATDAN O'TISH ======================
@router.message(CommandStart())
async def start(m: Message, state: FSMContext, bot: Bot, command: CommandObject):
    await state.clear()
    ref = None
    if command.args and command.args.startswith("ref_") and command.args[4:].isdigit():
        ref = int(command.args[4:])
    await m.answer(
        "👋 <b>AdMatch Bot</b>ga xush kelibsiz!\n\n"
        f"{esc(CFG['welcome'])}\n\n"
        "❤️ Layk bosing: agar u ham yoqtirsa, match bo'ladi\n"
        "👎 Yoqmasa: o'tkazib yuboring\n\n"
        "Tanishuvni hoziroq boshlang!")
    if await get_user(m.from_user.id):
        if not await subscribed(bot, m.from_user.id):
            return await m.answer("📢 Avval kanalga obuna bo'ling:", reply_markup=sub_kb())
        await m.answer("Menyudan tanlang 👇", reply_markup=main_menu(m.from_user.id))
    else:
        if m.from_user.id in ADMIN_IDS:
            await m.answer("🛠 Siz adminsiz: /admin buyrug'i yoki menyudagi tugma orqali panelga kiring.",
                           reply_markup=main_menu(m.from_user.id))
        await begin_reg(m, state, keep_kb=m.from_user.id in ADMIN_IDS)
        if ref and ref != m.from_user.id:
            await state.update_data(ref=ref)


@router.message(Command("cancel"))
async def cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Bekor qilindi.", reply_markup=main_menu(m.from_user.id))


@router.message(Command("help"))
async def help_cmd(m: Message):
    await m.answer(
        "ℹ️ <b>Yordam</b>\n\n/start - botni boshlash\n/cancel - amalni bekor qilish\n"
        f"👑 VIP olish: menyudagi «{B_VIP}» tugmasi\n"
        f"Savollar bo'lsa: {esc(CFG['vip_contact'])}")


@router.message(Command("myid"))
async def myid(m: Message):
    await m.answer(
        f"ID: <code>{m.from_user.id}</code>\n"
        f"Admin: {'ha ✅' if m.from_user.id in ADMIN_IDS else 'yoq ❌'}")


async def begin_reg(m: Message, state: FSMContext, keep_kb=False):
    await m.answer("Anketa to'ldiramiz.\n\n✏️ Ismingiz?",
                   reply_markup=None if keep_kb else ReplyKeyboardRemove())
    await state.set_state(Reg.name)


@router.message(Reg.name, F.text)
async def reg_name(m: Message, state: FSMContext):
    v = validate("name", m.text)
    if v is None:
        return await m.answer("Ism 2 dan 30 ta belgigacha bo'lsin.")
    await state.update_data(name=v)
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
    v = validate("age", m.text)
    if v is None:
        return await m.answer("Yoshni raqam bilan yozing (16-70).")
    await state.update_data(age=v)
    await m.answer("Qaysi shahardansiz?")
    await state.set_state(Reg.city)


@router.message(Reg.city, F.text)
async def reg_city(m: Message, state: FSMContext):
    v = validate("city", m.text)
    if v is None:
        return await m.answer("Shahar nomini to'g'ri yozing.")
    await state.update_data(city=v)
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
    v = validate("interests", m.text)
    if v is None:
        return await m.answer("Qiziqishlar 2 dan 100 ta belgigacha bo'lsin.")
    await state.update_data(interests=v)
    await m.answer("💬 O'zingiz haqingizda qisqacha yozing (150 belgigacha) yoki o'tkazib yuboring:",
                   reply_markup=skip_kb())
    await state.set_state(Reg.bio)


@router.message(Reg.bio, F.text)
async def reg_bio(m: Message, state: FSMContext):
    v = validate("bio", m.text)
    if v is None:
        return await m.answer("Juda uzun, 150 belgidan oshmasin.")
    d = await state.get_data()
    uid = m.from_user.id
    existed = await get_user(uid)
    ref = None if existed else d.get("ref")
    if ref and not await get_user(ref):
        ref = None
    await pool.execute(
        """
        INSERT INTO users(id,username,name,gender,age,city,photo,interests,bio,looking_for,referred_by)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT (id) DO UPDATE SET username=$2,name=$3,
            looking_for=CASE WHEN users.gender<>$4 THEN $10 ELSE users.looking_for END,
            gender=$4,age=$5,city=$6,photo=$7,interests=$8,bio=$9,is_active=TRUE
        """,
        uid, m.from_user.username, d["name"], d["gender"], d["age"], d["city"], d["photo"],
        d["interests"], v, opposite(d["gender"]), ref)
    await state.clear()
    await m.answer("✅ Anketa saqlandi!", reply_markup=main_menu(uid))
    await show_profile(m, uid)
    if ref:
        bonus = cfg_int("ref_bonus")
        await pool.execute("UPDATE users SET bonus_likes=bonus_likes+$2 WHERE id=$1", ref, bonus)
        try:
            await m.bot.send_message(ref, f"🎁 Do'stingiz qo'shildi! Sizga +{bonus} bonus layk berildi.")
        except Exception:
            pass


# ====================== PROFIL ======================
def profile_kb():
    return kb(
        [btn("✏️ Ism", "ed:name"), btn("🎂 Yosh", "ed:age"), btn("🏙 Shahar", "ed:city")],
        [btn("📸 Rasm", "ed:photo"), btn("🎯 Qiziqish", "ed:interests"), btn("💬 Bio", "ed:bio")],
        [btn("🔄 Qayta to'ldirish", "redo")],
    )


async def show_profile(m: Message, uid):
    u = await get_user(uid)
    status = "" if u["is_active"] else "\n\n🙈 <i>Anketangiz yashirilgan</i>"
    if is_vip(u):
        status += f"\n\n👑 <b>VIP faol:</b> {u['vip_until']:%d.%m.%Y} gacha"
    await m.answer_photo(u["photo"], caption=caption(u) + status, reply_markup=profile_kb())


@router.message(F.text == B_PROFILE)
async def my_profile(m: Message, bot: Bot):
    if await gate(bot, m.from_user.id, m.chat.id):
        await show_profile(m, m.from_user.id)


@router.callback_query(F.data == "redo")
async def redo(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await begin_reg(c.message, state)


ED_PROMPTS = {
    "name": "✏️ Yangi ismingizni yozing:",
    "age": "🎂 Yangi yoshingizni yozing:",
    "city": "🏙 Yangi shahringizni yozing:",
    "photo": "📸 Yangi rasmingizni yuboring:",
    "interests": "🎯 Yangi qiziqishlaringizni yozing:",
    "bio": "💬 Yangi bio yozing (o'chirish uchun - yozing):",
}


@router.callback_query(F.data.startswith("ed:"))
async def ed_start(c: CallbackQuery, state: FSMContext):
    field = c.data[3:]
    if field not in ED_PROMPTS:
        return await c.answer()
    await c.answer()
    await state.set_state(Edit.value)
    await state.update_data(field=field)
    await c.message.answer(ED_PROMPTS[field] + "\n\nBekor qilish: /cancel")


@router.message(Edit.value, F.photo)
async def ed_photo(m: Message, state: FSMContext):
    if (await state.get_data()).get("field") != "photo":
        return await m.answer("Matn yuboring.")
    await pool.execute("UPDATE users SET photo=$2 WHERE id=$1", m.from_user.id, m.photo[-1].file_id)
    await state.clear()
    await m.answer("✅ Rasm yangilandi", reply_markup=main_menu(m.from_user.id))
    await show_profile(m, m.from_user.id)


@router.message(Edit.value, F.text)
async def ed_text(m: Message, state: FSMContext):
    field = (await state.get_data()).get("field")
    if field not in ED_PROMPTS or field == "photo":
        return await m.answer("Rasm yuboring 📸")
    v = validate(field, m.text)
    if v is None:
        return await m.answer("Noto'g'ri qiymat, qayta urinib ko'ring.")
    await pool.execute(f"UPDATE users SET {field}=$2 WHERE id=$1", m.from_user.id, v)  # field - ro'yxatdan
    await state.clear()
    await m.answer("✅ Yangilandi", reply_markup=main_menu(m.from_user.id))
    await show_profile(m, m.from_user.id)


# ====================== SOZLAMALAR ======================
LOOK_LABEL = {"Qiz": "👩 Qizlar", "Yigit": "👨 Yigitlar", "all": "👥 Hammasi"}


def settings_kb(u):
    lf = u["looking_for"] or opposite(u["gender"])
    vis = "🙈 Anketani yashirish" if u["is_active"] else "👁 Anketani ko'rsatish"
    return kb(
        [btn(f"🎂 Qidiruv yoshi: {u['min_age']}-{u['max_age']}", "st:age")],
        [btn(f"🔎 Kimni qidiraman: {LOOK_LABEL[lf]}", "st:look")],
        [btn(vis, "st:vis")],
        [btn("🗑 Akkauntni o'chirish", "st:del")],
    )


@router.message(F.text == B_SET)
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


@router.callback_query(F.data == "st:look")
async def st_look(c: CallbackQuery):
    me = await get_user(c.from_user.id)
    if not me:
        return await c.answer("Avval /start bosing", show_alert=True)
    order = ["Qiz", "Yigit", "all"]
    cur = me["looking_for"] or opposite(me["gender"])
    nxt = order[(order.index(cur) + 1) % 3]
    u = await pool.fetchrow("UPDATE users SET looking_for=$2 WHERE id=$1 RETURNING *", me["id"], nxt)
    await c.answer(LOOK_LABEL[nxt])
    await c.message.edit_reply_markup(reply_markup=settings_kb(u))


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
    await c.message.answer("⚠️ Anketa va barcha layklaringiz o'chadi. Rostdan o'chiraymi?",
                           reply_markup=kb([btn("✅ Ha, o'chirish", "st:delok"), btn("❌ Yo'q", "back")]))


@router.callback_query(F.data == "st:delok")
async def st_delok(c: CallbackQuery):
    await c.answer()
    await delete_user_data(c.from_user.id)
    await c.message.answer("🗑 Akkaunt o'chirildi. Qaytish uchun /start bosing.",
                           reply_markup=ReplyKeyboardRemove())


# ====================== QIDIRUV / LAYK / MATCH ======================
async def send_next(bot: Bot, chat_id, uid):
    me = await get_user(uid)
    if not me:
        return
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


@router.message(F.text == B_SEARCH)
async def search(m: Message, bot: Bot):
    if await gate(bot, m.from_user.id, m.chat.id):
        await send_next(bot, m.chat.id, m.from_user.id)


async def show_liker(bot: Bot, chat_id, uid):
    p = await next_liker(uid)
    if not p:
        return await bot.send_message(chat_id, "Hozircha yangi layklar yo'q.",
                                      reply_markup=main_menu(uid))
    await bot.send_photo(chat_id, p["photo"],
                         caption="❤️ <b>Sizga shu odam layk bosdi:</b>\n\n" + caption(p),
                         reply_markup=react_kb(p["id"], "l"))


@router.message(F.text == B_LIKES)
async def who_liked(m: Message, bot: Bot):
    if await gate(bot, m.from_user.id, m.chat.id):
        await show_liker(bot, m.chat.id, m.from_user.id)


@router.message(F.text == B_MATCH)
async def my_matches(m: Message, bot: Bot):
    if not await gate(bot, m.from_user.id, m.chat.id):
        return
    rows = await pool.fetch(
        """
        SELECT u.* FROM matches mt
        JOIN users u ON u.id = CASE WHEN mt.a=$1 THEN mt.b ELSE mt.a END
        WHERE (mt.a=$1 OR mt.b=$1) AND NOT u.is_banned
        ORDER BY mt.created_at DESC LIMIT 20
        """, m.from_user.id)
    if not rows:
        return await m.answer("💞 Hozircha matchlar yo'q. Qidirishni davom eting!")
    lines = [f"{i}. <b>{esc(u['name'])}</b>, {u['age']}, {esc(u['city'])} — {contact_of(u)}"
             for i, u in enumerate(rows, 1)]
    await m.answer("💞 <b>Matchlaringiz</b> (oxirgi 20 ta):\n\n" + "\n".join(lines))


async def notify_match(bot: Bot, to_user, other):
    try:
        await bot.send_photo(
            to_user["id"], other["photo"],
            caption=f"🎉 <b>MATCH!</b>\nSiz va {esc(other['name'])} bir-biringizni yoqtirdingiz.\n\n"
                    f"💬 Yozing: {contact_of(other)}")
    except Exception:
        pass


async def handle_like(bot: Bot, me, tid, superlike=False):
    uid = me["id"]
    await set_reaction(uid, tid, 1)
    target = await get_user(tid)
    if not target or target["is_banned"]:
        return
    prev = await pool.fetchval(
        "SELECT value FROM reactions WHERE from_id=$1 AND to_id=$2", tid, uid)
    if prev == 1:  # o'zaro layk -> MATCH
        await pool.execute(
            "INSERT INTO matches(a,b) VALUES(LEAST($1::bigint,$2::bigint),GREATEST($1::bigint,$2::bigint)) "
            "ON CONFLICT DO NOTHING", uid, tid)
        await notify_match(bot, me, target)
        await notify_match(bot, target, me)
    elif prev is None or superlike:  # birinchi layk yoki super layk -> bildirishnoma
        head = ("⭐ <b>SUPER LAYK!</b> Sizga alohida qiziqish bildirdi:" if superlike
                else "❤️ <b>Sizga shu odam layk bosdi:</b>")
        try:
            await bot.send_photo(
                tid, me["photo"],
                caption=head + "\n\n" + caption(me),
                reply_markup=react_kb(uid, "n"))
        except TelegramForbiddenError:
            await pool.execute("UPDATE users SET blocked_bot=TRUE WHERE id=$1", tid)
        except Exception:
            pass


@router.callback_query(F.data.regexp(r"^(lk|ds|rp|sl):\d+:[snlv]$"))
async def react(c: CallbackQuery, bot: Bot):
    action, tid, src = c.data.split(":")
    tid, uid = int(tid), c.from_user.id
    me = await get_user(uid)
    if not me:
        return await c.answer("Avval /start bosing", show_alert=True)
    if action == "sl":
        if not is_vip(me):
            await c.answer()
            return await c.message.answer(
                "⭐ <b>Super layk</b> faqat VIP uchun!\n\n" + vip_offer_text(),
                reply_markup=kb(*await buy_rows(uid)))
        if not await super_allowed(uid):
            return await c.answer(
                f"⭐ Bugungi super layk limiti ({cfg_int('super_daily')} ta) tugadi.", show_alert=True)
    if action == "lk" and not await like_allowed(me):
        return await c.answer(
            f"❌ Bugungi layk limiti ({cfg_int('daily_likes')} ta) tugadi.\n"
            "👑 VIP bilan cheksiz yoki do'st taklif qilib bonus oling!",
            show_alert=True)
    await c.answer("⭐ Super layk yuborildi!" if action == "sl" else None)
    try:
        await c.message.delete()
    except Exception:
        pass

    if action == "ds":
        await set_reaction(uid, tid, 0)
    elif action == "rp":
        await pool.execute("INSERT INTO reports(from_id,to_id) VALUES($1,$2)", uid, tid)
        await set_reaction(uid, tid, 0)
        for a in ADMIN_IDS:
            try:
                await bot.send_message(a, f"🚩 Yangi shikoyat. ID <code>{tid}</code> ustidan. "
                                          "/admin → Shikoyatlar")
            except Exception:
                pass
        await bot.send_message(c.message.chat.id, "🚩 Shikoyat yuborildi, rahmat.")
    else:
        await handle_like(bot, me, tid, superlike=(action == "sl"))

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


# ====================== VIP PREMIUM: TARIFLAR VA TO'LOV ======================
async def buy_rows(uid=None):
    """Tarif tugmalari (promokod bo'lsa chegirma va bonus kun bilan)."""
    code, pct = (None, 0)
    if uid:
        code, pct = await promo_percent(uid)
    rows = []
    for key, (days, label) in PLANS.items():
        price = plan_price(key, pct)
        extra = f" (-{pct}% +{promo_bonus(key)} kun🎁)" if code else ""
        rows.append([btn(f"{label} — {money(price)} so'm{extra}", f"buy:{key}")])
    return rows


def vip_offer_text():
    prices = "\n".join(f"{label} — <b>{money(plan_price(k))} so'm</b>" for k, (d, label) in PLANS.items())
    return ("👑 <b>Bu funksiya faqat VIP PREMIUM uchun</b>\n\n"
            "VIP bilan istalgan odamning lichkasiga yozasiz, layk cheksiz, ⭐ super layk, "
            "anketangiz birinchi ko'rsatiladi.\n\n" + prices)


@router.message(F.text == B_VIP)
async def vip(m: Message, bot: Bot):
    me = await gate(bot, m.from_user.id, m.chat.id)
    if not me:
        return
    code, pct = await promo_percent(me["id"])
    rows = []
    if is_vip(me):
        status = f"✅ Faol: {me['vip_until']:%d.%m.%Y} gacha"
        rows.append([btn("🔎 Odam qidirish va lichkasiga yozish", "vs")])
    else:
        status = "❌ Faol emas"
    rows += await buy_rows(me["id"])
    rows.append([btn("🏷 Promokod", "promo"), btn("🧾 To'lovlarim", "mypays")])
    prices = "\n".join(
        f"{label} — <b>{money(plan_price(k, pct))} so'm</b>" for k, (d, label) in PLANS.items())
    promo_line = (f"\n🏷 Promokod: <b>{esc(code)}</b> (-{pct}%)\n"
                  f"🎁 Bonus: 1 hafta +{promo_bonus('week')}, 1 oy +{promo_bonus('month')}, "
                  f"3 oy +{promo_bonus('quarter')} kun VIP") if code else ""
    await m.answer(
        "👑 <b>VIP PREMIUM</b>\n\n"
        "✅ Istalgan odamning lichkasiga yozish\n"
        "✅ Cheksiz layk\n"
        f"✅ ⭐ Super layk (kuniga {cfg_int('super_daily')} ta)\n"
        "✅ Anketangiz qidiruvda birinchi ko'rsatiladi\n"
        "✅ Ismingiz yonida 👑 belgisi\n\n"
        f"{prices}{promo_line}\n\n"
        f"Qolgan layklar: <b>{await likes_left(me)}</b>\n"
        f"Holat: {status}\n\n"
        "Tarifni tanlang, kartaga to'lang va chekni yuboring. "
        "Admin tasdiqlashi bilan VIP <b>avtomatik</b> yoqiladi ⚡️",
        reply_markup=kb(*rows))


@router.callback_query(F.data.regexp(rf"^buy:{PLAN_RE}$"))
async def buy_plan(c: CallbackQuery):
    plan = c.data[4:]
    me = await get_user(c.from_user.id)
    if not me:
        return await c.answer("Avval /start bosing", show_alert=True)
    await c.answer()
    days, label = PLANS[plan]
    if not CFG["card_number"]:
        return await c.message.answer(
            "⚠️ To'lov rekvizitlari hali sozlanmagan. Iltimos, admin bilan bog'laning:",
            reply_markup=kb([btn("🆘 Admin", url=contact_url())]))
    code, pct = await promo_percent(me["id"])
    amount = plan_price(plan, pct)
    disc = f" (promokod -{pct}%)" if pct else ""
    bonus_line = f"🎁 Promokod bonusi: <b>+{promo_bonus(plan)} kun</b>\n" if code else ""
    owner = f"👤 Karta egasi: <b>{esc(CFG['card_owner'])}</b>\n" if CFG["card_owner"] else ""
    await c.message.answer(
        f"💳 <b>To'lov: {label}</b>\n\n"
        f"📦 Muddat: {days} kun\n{bonus_line}"
        f"💰 To'lov summasi: <b>{money(amount)} so'm</b>{disc}\n\n"
        f"🏦 Karta raqami:\n<code>{esc(CFG['card_number'])}</code>\n{owner}\n"
        "<b>Qadamlar:</b>\n"
        "1️⃣ Yuqoridagi kartaga <b>aniq shu summani</b> o'tkazing\n"
        "2️⃣ «📤 Chek yuborish» tugmasini bosing\n"
        "3️⃣ To'lov chekini (skrinshot yoki PDF) yuboring\n"
        "4️⃣ Admin tekshiradi va VIP avtomatik yoqiladi ⚡️",
        reply_markup=kb(
            [btn("📤 Chek yuborish", f"pay:{plan}")],
            [btn("🏷 Promokod kiritish", "promo")],
            [btn("🆘 Admin bilan bog'lanish", url=contact_url())],
            [btn("⬅️ Orqaga", "back")]))


@router.callback_query(F.data.regexp(rf"^pay:{PLAN_RE}$"))
async def pay_start(c: CallbackQuery, state: FSMContext):
    plan = c.data[4:]
    me = await get_user(c.from_user.id)
    if not me:
        return await c.answer("Avval /start bosing", show_alert=True)
    await c.answer()
    pend = await pool.fetchval("SELECT id FROM payments WHERE user_id=$1 AND status='pending'", me["id"])
    if pend:
        return await c.message.answer(
            f"⏳ Sizning #{pend} raqamli to'lovingiz hali ko'rib chiqilmoqda. Iltimos, kuting.")
    await state.set_state(Pay.receipt)
    await state.update_data(plan=plan)
    await c.message.answer(
        "📤 To'lov chekini <b>rasm</b> (skrinshot) yoki <b>PDF</b> ko'rinishida yuboring.\n\n"
        "Bekor qilish: /cancel")


@router.message(Pay.receipt, F.photo | F.document)
async def pay_receipt(m: Message, state: FSMContext, bot: Bot):
    d = await state.get_data()
    plan = d.get("plan")
    uid = m.from_user.id
    me = await get_user(uid)
    if plan not in PLANS or not me:
        await state.clear()
        return await m.answer("Xatolik. Qaytadan urinib ko'ring.", reply_markup=main_menu(uid))

    if m.photo:
        fid, fuid, ftype = m.photo[-1].file_id, m.photo[-1].file_unique_id, "photo"
    else:
        doc = m.document
        mime = doc.mime_type or ""
        if not (mime.startswith("image/") or mime == "application/pdf"):
            return await m.answer("❌ Faqat rasm yoki PDF fayl yuboring.")
        if doc.file_size and doc.file_size > 10 * 1024 * 1024:
            return await m.answer("❌ Fayl juda katta (10 MB gacha).")
        fid, fuid, ftype = doc.file_id, doc.file_unique_id, "document"

    tries = await pool.fetchval(
        "SELECT count(*) FROM payments WHERE user_id=$1 AND created_at>now()-interval '24 hours'", uid)
    if tries >= MAX_PAY_TRIES_24H:
        await state.clear()
        return await m.answer("⛔ Bugun juda ko'p urinish bo'ldi. Ertaga qayta urinib ko'ring yoki "
                              f"admin bilan bog'laning: {esc(CFG['vip_contact'])}")
    if await pool.fetchval(
            "SELECT 1 FROM payments WHERE file_uid=$1 AND status IN ('pending','approved')", fuid):
        return await m.answer("❌ Bu chek avval yuborilgan. Iltimos, o'z to'lov chekingizni yuboring.")

    code, pct = await promo_percent(uid)
    amount = plan_price(plan, pct)
    days = PLANS[plan][0]
    try:
        pid = await pool.fetchval(
            "INSERT INTO payments(user_id,plan,days,amount,promo_code,discount,file_id,file_uid,file_type) "
            "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id",
            uid, plan, days, amount, code, pct, fid, fuid, ftype)
    except asyncpg.UniqueViolationError:
        await state.clear()
        return await m.answer("⏳ Sizda ko'rib chiqilayotgan to'lov bor yoki bu chek takroriy.",
                              reply_markup=main_menu(uid))
    await state.clear()
    await pool.execute("UPDATE users SET promo_applied=NULL WHERE id=$1", uid)

    p = await pool.fetchrow("SELECT * FROM payments WHERE id=$1", pid)
    sent = 0
    for a in ADMIN_IDS:
        if await send_payment_card(bot, a, p):
            sent += 1
    if not sent:
        log.warning("To'lov #%s hech bir adminga yetib bormadi (ADMIN_IDS tekshiring)", pid)
    bonus_txt = f"\n🎁 Promokod bonusi: +{promo_bonus(plan)} kun" if code else ""
    await m.answer(
        f"✅ <b>Chekingiz qabul qilindi!</b> (#{pid})\n\n"
        f"📦 {PLANS[plan][1]} — {money(amount)} so'm{bonus_txt}\n"
        "⏳ Admin tekshiradi, odatda 5–30 daqiqa ichida. Natija haqida shu yerda xabar beramiz.",
        reply_markup=main_menu(uid))


@router.message(Pay.receipt)
async def pay_receipt_bad(m: Message):
    await m.answer("📎 Iltimos, to'lov chekini rasm yoki PDF qilib yuboring.\nBekor qilish: /cancel")


@router.callback_query(F.data == "mypays")
async def my_pays(c: CallbackQuery):
    await c.answer()
    rows = await pool.fetch(
        "SELECT * FROM payments WHERE user_id=$1 ORDER BY id DESC LIMIT 10", c.from_user.id)
    if not rows:
        return await c.message.answer("🧾 Hozircha to'lovlaringiz yo'q.")
    lines = []
    for p in rows:
        line = (f"#{p['id']} · {p['created_at']:%d.%m.%Y} · {PLANS.get(p['plan'], ('', p['plan']))[1]} · "
                f"{money(p['amount'])} so'm · {STATUS_TXT[p['status']]}")
        if p["status"] == "rejected" and p["reason"]:
            line += f"\n    ↳ {esc(p['reason'])}"
        lines.append(line)
    await c.message.answer("🧾 <b>To'lovlarim</b> (oxirgi 10 ta):\n\n" + "\n".join(lines))


# ---- promokod kiritish
@router.callback_query(F.data == "promo")
async def promo_ask(c: CallbackQuery, state: FSMContext):
    if not await get_user(c.from_user.id):
        return await c.answer("Avval /start bosing", show_alert=True)
    await c.answer()
    await state.set_state(PromoS.code)
    await c.message.answer("🏷 Promokodni yozing:\n\nBekor qilish: /cancel")


@router.message(PromoS.code, F.text)
async def promo_set(m: Message, state: FSMContext):
    code = m.text.strip().upper()
    p, err = await promo_check(code, m.from_user.id)
    if err:
        return await m.answer(f"❌ {err}. Boshqa kod yozing yoki /cancel.")
    await pool.execute("UPDATE users SET promo_applied=$2 WHERE id=$1", m.from_user.id, code)
    await state.clear()
    await m.answer(f"✅ Promokod qabul qilindi: <b>-{p['percent']}%</b> chegirma va bonus VIP kunlar!\n"
                   "Endi tarifni tanlang 👇",
                   reply_markup=kb(*await buy_rows(m.from_user.id)))


# ---- VIP: lichkaga yozish / qidirish
@router.callback_query(F.data.regexp(r"^dm:\d+$"))
async def dm_open(c: CallbackQuery):
    me = await get_user(c.from_user.id)
    if not me:
        return await c.answer("Avval /start bosing", show_alert=True)
    await c.answer()
    if not is_vip(me):
        return await c.message.answer(vip_offer_text(), reply_markup=kb(*await buy_rows(me["id"])))
    t = await get_user(int(c.data[3:]))
    if not t or t["is_banned"]:
        return await c.message.answer("Foydalanuvchi topilmadi.")
    url = f"https://t.me/{t['username']}" if t["username"] else f"tg://user?id={t['id']}"
    await c.message.answer(f"💬 <b>{esc(t['name'])}</b> lichkasiga o'tish:",
                           reply_markup=kb([btn("💬 Lichkaga o'tish", url=url)]))


@router.callback_query(F.data == "vs")
async def vs_start(c: CallbackQuery, state: FSMContext):
    me = await get_user(c.from_user.id)
    await c.answer()
    if not me or not is_vip(me):
        return await c.message.answer(vip_offer_text(), reply_markup=kb(*await buy_rows(c.from_user.id)))
    await state.set_state(VipSearch.q)
    await c.message.answer("🔎 Ism, @username yoki shahar yozing:\n/cancel - bekor")


@router.message(VipSearch.q, F.text)
async def vs_find(m: Message, state: FSMContext, bot: Bot):
    me = await get_user(m.from_user.id)
    await state.clear()
    if not me or not is_vip(me):
        return await m.answer(vip_offer_text(), reply_markup=kb(*await buy_rows(m.from_user.id)))
    q = m.text.strip().lstrip("@")
    rows = await pool.fetch(
        "SELECT * FROM users WHERE id<>$1 AND is_active AND NOT is_banned "
        "AND (lower(username)=lower($2) OR name ILIKE $3 OR city ILIKE $3) "
        "ORDER BY (lower(city)=lower($4)) DESC, random() LIMIT 10",
        me["id"], q, f"%{q}%", me["city"])
    if not rows:
        return await m.answer("❌ Hech kim topilmadi.")
    for u in rows:
        try:
            await bot.send_photo(m.chat.id, u["photo"], caption=caption(u),
                                 reply_markup=react_kb(u["id"], "v"))
        except TelegramBadRequest:
            pass


# ====================== REFERAL / KUNLIK BONUS ======================
@router.message(F.text == B_INV)
async def invite(m: Message, bot: Bot):
    me = await gate(bot, m.from_user.id, m.chat.id)
    if not me:
        return
    n = await pool.fetchval("SELECT count(*) FROM users WHERE referred_by=$1", me["id"])
    link = f"https://t.me/{BOT_USERNAME}?start=ref_{me['id']}"
    share = f"https://t.me/share/url?url={quote(link)}&text={quote('AdMatch botida tanishing! ❤️')}"
    vip_days = cfg_int("ref_vip_days")
    vip_line = (f"👑 Do'stingiz VIP sotib olsa, sizga yana <b>+{vip_days} kun VIP</b>!\n"
                if vip_days > 0 else "")
    await m.answer(
        "🎁 <b>Do'st taklif qiling</b>\n\n"
        f"Har bir do'stingiz anketa to'ldirsa, sizga <b>+{cfg_int('ref_bonus')}</b> bonus layk beriladi.\n"
        f"{vip_line}\n"
        f"Sizning havolangiz:\n<code>{link}</code>\n\n"
        f"👥 Taklif qilganlar: <b>{n}</b>\n"
        f"💌 Qolgan layklar: <b>{await likes_left(me)}</b>\n"
        f"🔥 Kunlik bonus seriyasi: <b>{me['streak'] or 0}</b> kun",
        reply_markup=kb([btn("🎁 Kunlik bonus olish", "daily")],
                        [btn("📤 Do'stlarga ulashish", url=share)]))


@router.callback_query(F.data == "daily")
async def daily_bonus(c: CallbackQuery):
    me = await get_user(c.from_user.id)
    if not me:
        return await c.answer("Avval /start bosing", show_alert=True)
    row = await pool.fetchrow(
        """
        UPDATE users SET
            streak = CASE WHEN last_bonus = CURRENT_DATE - 1 THEN COALESCE(streak,0) + 1 ELSE 1 END,
            last_bonus = CURRENT_DATE
        WHERE id=$1 AND (last_bonus IS NULL OR last_bonus < CURRENT_DATE)
        RETURNING streak
        """, me["id"])
    if not row:
        return await c.answer("Bugungi bonusni olgansiz ✅\nErtaga qaytib keling!", show_alert=True)
    bonus = 2 + min(row["streak"], 8)  # 3 dan 10 gacha, har kuni ko'payadi
    await pool.execute("UPDATE users SET bonus_likes=bonus_likes+$2 WHERE id=$1", me["id"], bonus)
    await c.answer(f"🎁 +{bonus} bonus layk!\n🔥 Ketma-ket: {row['streak']} kun", show_alert=True)


# ====================== ADMIN PANEL ======================
class Bc(StatesGroup):
    content = State()
    confirm = State()


class AdmUser(StatesGroup):
    query = State()
    msg = State()


class CfgS(StatesGroup):
    value = State()


class AdmReject(StatesGroup):
    reason = State()


AUD = {
    "all": ("👥 Hammaga", "TRUE"),
    "Qiz": ("👩 Qizlarga", "gender='Qiz'"),
    "Yigit": ("👨 Yigitlarga", "gender='Yigit'"),
    "vip": ("👑 VIP larga", "vip_until>now()"),
    "free": ("🆓 Bepullarga", "(vip_until IS NULL OR vip_until<=now())"),
    "act7": ("🔥 7 kunda faollarga", "last_seen>now()-interval '7 days'"),
}


def admin_kb():
    return kb(
        [btn("📊 Statistika", "adm:stats"), btn("📢 Xabar yuborish", "adm:bc")],
        [btn("💳 To'lovlar", "adm:pays"), btn("💰 Daromad", "adm:rev")],
        [btn("🔎 Foydalanuvchi", "adm:user"), btn("🚩 Shikoyatlar", "adm:rep")],
        [btn("👑 VIP ro'yxati", "adm:vips"), btn("🚫 Bloklanganlar", "adm:bans")],
        [btn("🏷 Promokodlar", "adm:promos"), btn("⚙️ Sozlamalar", "adm:cfg")],
        [btn("📜 Loglar", "adm:logs"), btn("📥 CSV eksport", "adm:csv")],
    )


@admin.message(Command("admin"))
@admin.message(F.text == B_ADMIN)
async def admin_panel(m: Message, state: FSMContext):
    await state.clear()
    pend = await pool.fetchval("SELECT count(*) FROM payments WHERE status='pending'")
    await m.answer("🛠 <b>Admin panel</b>\n\n"
                   f"💳 Kutilayotgan to'lovlar: <b>{pend}</b>\n\n"
                   "Tezkor buyruqlar:\n"
                   "<code>/vip ID KUN</code>  <code>/ban ID</code>  <code>/unban ID</code>\n"
                   "<code>/payments</code> — kutilayotgan to'lovlar\n"
                   "<code>/promo KOD FOIZ [LIMIT] [KUN]</code>  <code>/delpromo KOD</code>",
                   reply_markup=admin_kb())


@admin.message(Command("vip"))
async def cmd_vip(m: Message, command: CommandObject, bot: Bot):
    a = (command.args or "").split()
    if len(a) != 2 or not all(x.isdigit() for x in a) or not await get_user(int(a[0])):
        return await m.answer("Format: <code>/vip ID KUN</code> (foydalanuvchi bazada bo'lishi kerak)")
    await grant_vip(bot, m.from_user.id, int(a[0]), int(a[1]))
    await m.answer("✅ VIP berildi")


@admin.message(Command("ban", "unban"))
async def cmd_ban(m: Message, command: CommandObject):
    if not (command.args or "").strip().isdigit():
        return await m.answer("Format: <code>/ban ID</code>")
    uid, ban = int(command.args.strip()), command.command == "ban"
    await pool.execute("UPDATE users SET is_banned=$2 WHERE id=$1", uid, ban)
    await log_admin(m.from_user.id, "ban" if ban else "unban", uid)
    await m.answer("✅ Bajarildi")


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
          count(*) FILTER (WHERE last_seen >= now() - interval '7 days') active7,
          count(*) FILTER (WHERE vip_until > now()) vip,
          count(*) FILTER (WHERE is_banned) banned,
          count(*) FILTER (WHERE NOT is_active) hidden,
          count(*) FILTER (WHERE blocked_bot) blocked,
          count(*) FILTER (WHERE referred_by IS NOT NULL) refs
        FROM users""")
    r = await pool.fetchrow("""
        SELECT count(*) FILTER (WHERE value=1) likes,
               count(*) FILTER (WHERE value=0) dislikes FROM reactions""")
    matches = await pool.fetchval("SELECT count(*) FROM matches")
    reports = await pool.fetchval("SELECT count(*) FROM reports WHERE NOT handled")
    cities = await pool.fetch(
        "SELECT initcap(lower(city)) c, count(*) n FROM users GROUP BY 1 ORDER BY n DESC LIMIT 5")
    days = await pool.fetch("""
        SELECT d::date AS day, count(u.id) n
        FROM generate_series(current_date-6, current_date, interval '1 day') d
        LEFT JOIN users u ON u.created_at::date = d::date
        GROUP BY 1 ORDER BY 1""")
    top_ref = await pool.fetch(
        "SELECT referred_by id, count(*) n FROM users WHERE referred_by IS NOT NULL "
        "GROUP BY 1 ORDER BY n DESC LIMIT 3")
    mx = max([x["n"] for x in days] + [1])
    chart = "\n".join(f"{x['day']:%d.%m} {'█' * round(x['n'] / mx * 10) or '▏'} {x['n']}" for x in days)
    top = "\n".join(f" • {esc(x['c'])}: {x['n']}" for x in cities) or " —"
    tref = "\n".join(f" • <code>{x['id']}</code>: {x['n']}" for x in top_ref) or " —"
    pct = round(matches * 2 / s["total"] * 100) if s["total"] else 0
    conv = round(s["vip"] / s["total"] * 100, 1) if s["total"] else 0
    await c.message.answer(
        "📊 <b>Statistika</b>\n\n"
        f"👥 Jami: <b>{s['total']}</b>\n"
        f"👨 Yigitlar: {s['boys']}   👩 Qizlar: {s['girls']}\n"
        f"🆕 Bugun qo'shilgan: {s['today']}\n"
        f"🟢 24 soatda faol: {s['active24']}   7 kunda: {s['active7']}\n"
        f"👑 VIP: {s['vip']} ({conv}%)\n"
        f"🚫 Bloklangan: {s['banned']}   🙈 Yashirin: {s['hidden']}\n"
        f"⛔ Botni bloklaganlar: {s['blocked']}\n\n"
        f"❤️ Laykler: {r['likes']}   👎 Dislaykler: {r['dislikes']}\n"
        f"🎉 Matchlar: {matches} (foydalanuvchilarning ~{pct}%)\n"
        f"🚩 Ko'rilmagan shikoyatlar: {reports}\n"
        f"🎁 Referal orqali kelganlar: {s['refs']}\n\n"
        f"📈 <b>So'nggi 7 kun (yangi)</b>\n<code>{chart}</code>\n\n"
        f"🏙 Top shaharlar:\n{top}\n\n🏆 Top taklif qiluvchilar:\n{tref}")


# ---- xabar yuborish (rassilka)
@admin.callback_query(F.data == "adm:bc")
async def adm_bc(c: CallbackQuery):
    await c.answer()
    items = list(AUD.items())
    rows = [[btn(items[i][1][0], f"bc:t:{items[i][0]}"),
             btn(items[i + 1][1][0], f"bc:t:{items[i + 1][0]}")] for i in range(0, len(items), 2)]
    await c.message.answer("Kimga yuboramiz?", reply_markup=kb(*rows))


@admin.callback_query(F.data.startswith("bc:t:"))
async def bc_target(c: CallbackQuery, state: FSMContext):
    key = c.data.split(":")[2]
    if key not in AUD:
        return await c.answer()
    await c.answer()
    await state.update_data(target=key)
    await state.set_state(Bc.content)
    await c.message.answer("Yuboriladigan xabarni jo'nating (matn, rasm yoki video):\n/cancel - bekor")


@admin.message(Command("cancel"))
async def adm_cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Bekor qilindi.", reply_markup=main_menu(m.from_user.id))


@admin.message(Bc.content)
async def bc_content(m: Message, state: FSMContext):
    d = await state.get_data()
    label, cond = AUD[d["target"]]
    n = await pool.fetchval(f"SELECT count(*) FROM users WHERE NOT is_banned AND NOT blocked_bot AND {cond}")
    await state.update_data(chat_id=m.chat.id, msg_id=m.message_id)
    await state.set_state(Bc.confirm)
    await m.answer(f"👆 Shu xabar <b>{label}</b> ({n} kishi) yuboriladi. Tasdiqlaysizmi?",
                   reply_markup=kb([btn("✅ Yuborish", "bc:go"), btn("❌ Bekor", "bc:no")]))


@admin.callback_query(Bc.confirm, F.data == "bc:no")
async def bc_no(c: CallbackQuery, state: FSMContext):
    await c.answer("Bekor qilindi")
    await state.clear()


@admin.callback_query(Bc.confirm, F.data == "bc:go")
async def bc_go(c: CallbackQuery, state: FSMContext, bot: Bot):
    global BC_BUSY
    if BC_BUSY:
        return await c.answer("Boshqa xabar yuborilmoqda, kuting.", show_alert=True)
    d = await state.get_data()
    await state.clear()
    await c.answer("Yuborish boshlandi")
    status = await c.message.answer("📤 Yuborilmoqda...")
    await log_admin(c.from_user.id, "broadcast", None, d["target"])
    asyncio.create_task(run_broadcast(bot, c.from_user.id, status, d["target"], d["chat_id"], d["msg_id"]))


async def run_broadcast(bot: Bot, admin_id, status, target, chat_id, msg_id):
    global BC_BUSY
    BC_BUSY = True
    try:
        cond = AUD[target][1]
        rows = await pool.fetch(f"SELECT id FROM users WHERE NOT is_banned AND NOT blocked_bot AND {cond}")
        ok = fail = 0
        for i, r in enumerate(rows, 1):
            for _ in range(3):
                try:
                    await bot.copy_message(r["id"], chat_id, msg_id)
                    ok += 1
                    break
                except TelegramRetryAfter as e:
                    await asyncio.sleep(e.retry_after)
                except TelegramForbiddenError:
                    await pool.execute("UPDATE users SET blocked_bot=TRUE WHERE id=$1", r["id"])
                    fail += 1
                    break
                except Exception:
                    fail += 1
                    break
            if i % 100 == 0:
                try:
                    await status.edit_text(f"📤 Yuborilmoqda... {i}/{len(rows)}")
                except Exception:
                    pass
            await asyncio.sleep(0.05)
        await bot.send_message(admin_id, f"✅ Xabar yuborildi.\nYetkazildi: {ok}\nYetmadi: {fail}")
    except Exception:
        log.exception("Broadcast xatosi")
    finally:
        BC_BUSY = False


# ---- foydalanuvchini boshqarish
def user_kb(u):
    i = u["id"]
    ban = ("✅ Blokdan chiqarish", "unban") if u["is_banned"] else ("🚫 Bloklash", "ban")
    return kb(
        [btn(ban[0], f"au:{ban[1]}:{i}"), btn("✉️ Xabar", f"au:msg:{i}")],
        [btn("👑 +7 kun", f"au:vip7:{i}"), btn("👑 +30 kun", f"au:vip30:{i}"),
         btn("👑 +90 kun", f"au:vip90:{i}")],
        [btn("❌ VIP ni olish", f"au:vipoff:{i}"), btn("🎁 +10 layk", f"au:bonus:{i}")],
        [btn("🗑 O'chirish", f"au:del:{i}")],
    )


def admin_card(u):
    vip_s = f"✅ {u['vip_until']:%d.%m.%Y} gacha" if is_vip(u) else "❌"
    return (
        f"🆔 <code>{u['id']}</code> @{esc(u['username'] or '—')}\n\n"
        f"{caption(u)}\n\n"
        f"👑 VIP: {vip_s}\n"
        f"🚫 Bloklangan: {'ha' if u['is_banned'] else 'yoq'}\n"
        f"👁 Ko'rinadi: {'ha' if u['is_active'] else 'yoq'}\n"
        f"💌 Bonus layk: {u['bonus_likes'] or 0}\n"
        f"🕒 Oxirgi faollik: {u['last_seen']:%d.%m.%Y %H:%M}\n"
        f"📅 Qo'shilgan: {u['created_at']:%d.%m.%Y}")


async def send_card(bot: Bot, chat_id, u):
    try:
        await bot.send_photo(chat_id, u["photo"], caption=admin_card(u), reply_markup=user_kb(u))
    except TelegramBadRequest:
        await bot.send_message(chat_id, admin_card(u), reply_markup=user_kb(u))


async def refresh_card(c: CallbackQuery, u):
    try:
        await c.message.edit_caption(caption=admin_card(u), reply_markup=user_kb(u))
    except TelegramBadRequest:
        try:
            await c.message.edit_text(admin_card(u), reply_markup=user_kb(u))
        except TelegramBadRequest:
            pass


@admin.callback_query(F.data == "adm:user")
async def adm_user(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.set_state(AdmUser.query)
    await c.message.answer("Foydalanuvchi ID raqami, @username yoki ismini yozing:")


@admin.message(AdmUser.query, F.text)
async def adm_user_find(m: Message, state: FSMContext, bot: Bot):
    q = m.text.strip().lstrip("@")
    await state.clear()
    if q.isdigit():
        u = await get_user(int(q))
        rows = [u] if u else []
    else:
        rows = await pool.fetch(
            "SELECT * FROM users WHERE lower(username)=lower($1) OR name ILIKE $2 "
            "ORDER BY created_at DESC LIMIT 8", q, f"%{q}%")
    if not rows:
        return await m.answer("❌ Topilmadi.")
    if len(rows) == 1:
        return await send_card(bot, m.chat.id, rows[0])
    await m.answer("Bir nechta topildi:", reply_markup=kb(
        *[[btn(f"{u['name']} ({u['id']})", f"au:view:{u['id']}")] for u in rows]))


@admin.callback_query(F.data.regexp(r"^au:(ban|unban|vip7|vip30|vip90|vipoff|bonus|msg|del|delok|view):\d+$"))
async def adm_user_action(c: CallbackQuery, bot: Bot, state: FSMContext):
    _, act, uid = c.data.split(":")
    uid = int(uid)
    u = await get_user(uid)
    if not u:
        return await c.answer("Foydalanuvchi topilmadi", show_alert=True)
    aid = c.from_user.id
    if act == "view":
        await c.answer()
        return await send_card(bot, c.message.chat.id, u)
    if act == "msg":
        await c.answer()
        await state.set_state(AdmUser.msg)
        await state.update_data(target=uid)
        return await c.message.answer(f"✉️ <code>{uid}</code> ga yuboriladigan xabarni jo'nating:\n/cancel - bekor")
    if act == "del":
        await c.answer()
        return await c.message.answer(
            f"⚠️ <code>{uid}</code> ni butunlay o'chiraymi?",
            reply_markup=kb([btn("✅ Ha", f"au:delok:{uid}"), btn("❌ Yo'q", f"au:view:{uid}")]))
    if act == "delok":
        await delete_user_data(uid)
        await log_admin(aid, "delete", uid)
        await c.answer("O'chirildi")
        return await c.message.edit_text("🗑 Foydalanuvchi o'chirildi.")
    if act in ("ban", "unban"):
        await pool.execute("UPDATE users SET is_banned=$2 WHERE id=$1", uid, act == "ban")
        await log_admin(aid, act, uid)
    elif act in ("vip7", "vip30", "vip90"):
        await grant_vip(bot, aid, uid, int(act[3:]))
    elif act == "vipoff":
        await pool.execute("UPDATE users SET vip_until=NULL WHERE id=$1", uid)
        await log_admin(aid, "vipoff", uid)
    elif act == "bonus":
        await pool.execute("UPDATE users SET bonus_likes=bonus_likes+10 WHERE id=$1", uid)
        await log_admin(aid, "bonus", uid, "+10")
    await c.answer("Bajarildi ✅")
    u = await get_user(uid)
    if u:
        await refresh_card(c, u)


@admin.message(AdmUser.msg)
async def adm_user_msg(m: Message, state: FSMContext, bot: Bot):
    uid = (await state.get_data())["target"]
    await state.clear()
    try:
        await bot.copy_message(uid, m.chat.id, m.message_id)
        await log_admin(m.from_user.id, "message", uid)
        await m.answer("✅ Yuborildi")
    except Exception as e:
        await m.answer(f"❌ Yuborilmadi: {esc(str(e))[:200]}")


# ---- VIP va ban ro'yxatlari
@admin.callback_query(F.data == "adm:vips")
async def adm_vips(c: CallbackQuery):
    await c.answer()
    rows = await pool.fetch(
        "SELECT * FROM users WHERE vip_until>now() ORDER BY vip_until LIMIT 30")
    if not rows:
        return await c.message.answer("Faol VIP lar yo'q.")
    await c.message.answer("👑 <b>Faol VIP lar</b> (tugash sanasi bo'yicha):", reply_markup=kb(
        *[[btn(f"{u['name']} — {u['vip_until']:%d.%m.%Y}", f"au:view:{u['id']}")] for u in rows]))


@admin.callback_query(F.data == "adm:bans")
async def adm_bans(c: CallbackQuery):
    await c.answer()
    rows = await pool.fetch("SELECT * FROM users WHERE is_banned ORDER BY last_seen DESC LIMIT 30")
    if not rows:
        return await c.message.answer("Bloklanganlar yo'q.")
    await c.message.answer("🚫 <b>Bloklanganlar:</b>", reply_markup=kb(
        *[[btn(f"{u['name']} ({u['id']})", f"au:view:{u['id']}")] for u in rows]))


# ---- shikoyatlar
async def show_report(bot: Bot, chat_id):
    for _ in range(10):
        r = await pool.fetchrow("SELECT * FROM reports WHERE NOT handled ORDER BY id LIMIT 1")
        if not r:
            return await bot.send_message(chat_id, "✅ Yangi shikoyatlar yo'q.")
        u = await get_user(r["to_id"])
        if u:
            break
        await pool.execute("UPDATE reports SET handled=TRUE WHERE to_id=$1", r["to_id"])
    else:
        return
    cnt = await pool.fetchval("SELECT count(*) FROM reports WHERE to_id=$1", u["id"])
    text = (f"🚩 <b>Shikoyat</b> (shu odamga jami {cnt} ta)\n"
            f"Shikoyatchi: <code>{r['from_id']}</code>\n\n" + admin_card(u))
    markup = kb([btn("🚫 Bloklash", f"ar:ban:{u['id']}"), btn("✅ Rad etish", f"ar:ok:{u['id']}")])
    try:
        await bot.send_photo(chat_id, u["photo"], caption=text, reply_markup=markup)
    except TelegramBadRequest:
        await bot.send_message(chat_id, text, reply_markup=markup)


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
    await log_admin(c.from_user.id, "report_" + act, uid)
    await c.answer("Bajarildi ✅")
    try:
        await c.message.delete()
    except Exception:
        pass
    await show_report(bot, c.message.chat.id)

# ====================== ISHGA TUSHIRISH ======================
async def init_database() -> None:
    """Jadvallarni yaratadi va standart sozlamalarni bazaga yozadi."""
    global pool

    if not DATABASE_URL:
        raise RuntimeError("Render Environment'da DATABASE_URL kiritilmagan.")

    # Render Postgres URL'i asyncpg bilan ishlashi uchun normalizatsiya.
    dsn = DATABASE_URL.strip()
    if dsn.startswith("postgres://"):
        dsn = "postgresql://" + dsn[len("postgres://"):]

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=5,
        command_timeout=60,
        timeout=30,
    )

    async with pool.acquire() as conn:
        for statement in SCHEMA_STATEMENTS:
            await conn.execute(statement)
        for key, value in DEFAULTS.items():
            await conn.execute(
                "INSERT INTO settings(key, value) VALUES($1, $2) "
                "ON CONFLICT(key) DO NOTHING",
                key,
                str(value),
            )

    await load_cfg()
    log.info("PostgreSQL bazasi tayyor.")


async def health_check(_request: web.Request) -> web.Response:
    """Render health check endpoint."""
    if pool is None:
        return web.json_response({"ok": False, "database": "not_ready"}, status=503)
    try:
        await pool.fetchval("SELECT 1")
        return web.json_response({"ok": True, "service": "AdMatch Bot PRO"})
    except Exception:
        log.exception("Health check: database tekshiruvida xato")
        return web.json_response({"ok": False, "database": "error"}, status=503)


async def main() -> None:
    global BOT_USERNAME

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    if not TOKEN:
        raise RuntimeError("Render Environment'da BOT_TOKEN kiritilmagan.")
    if not DATABASE_URL:
        raise RuntimeError("Render Environment'da DATABASE_URL kiritilmagan.")
    if not ADMIN_IDS:
        log.warning("ADMIN_IDS kiritilmagan: admin funksiyalari ishlamaydi.")

    await init_database()

    bot = Bot(
        token=TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.update.middleware(Guard())
    dp.include_router(router)
    dp.include_router(admin)

    # Bot username'i referal havolalarini yaratish uchun kerak.
    me = await bot.get_me()
    BOT_USERNAME = me.username or ""
    log.info("Bot ulandi: @%s", BOT_USERNAME or "username yo'q")

    await bot.set_my_commands([
        BotCommand(command="start", description="Botni boshlash"),
        BotCommand(command="help", description="Yordam"),
        BotCommand(command="cancel", description="Amalni bekor qilish"),
        BotCommand(command="admin", description="Admin panel"),
    ])

    # Render Web Service PORT talab qiladi. Bu endpoint servisni sog'lom
    # deb belgilash uchun; Telegram yangilanishlari polling orqali olinadi.
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()
    log.info("Health server 0.0.0.0:%s da ishga tushdi.", PORT)

    try:
        # Oldingi webhook bo'lsa olib tashlaydi va pollingni boshlaydi.
        await bot.delete_webhook(drop_pending_updates=False)
        log.info("Telegram polling boshlandi.")
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            close_bot_session=False,
        )
    finally:
        await runner.cleanup()
        await bot.session.close()
        if pool is not None:
            await pool.close()
            pool = None
        log.info("Bot to'xtatildi.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception:
        logging.basicConfig(level=logging.INFO)
        logging.exception("AdMatch Bot PRO ishga tushmadi.")
        raise
