"""
AdMatch Bot PRO v2 - Telegram tanishuv boti
Stack: aiogram 3 + PostgreSQL (asyncpg) + aiohttp (Render Web Service uchun)

Muhit o'zgaruvchilari (Render -> Environment):
  BOT_TOKEN     - BotFather tokeni (majburiy)
  DATABASE_URL  - Postgres manzili (majburiy)
  ADMIN_IDS     - admin Telegram ID lari, vergul bilan (12345,67890)
  CHANNEL       - majburiy obuna kanali, masalan @kanal (ixtiyoriy)
  DAILY_LIKES   - bepul foydalanuvchi uchun kunlik layk (standart 30)
  VIP_CONTACT   - VIP uchun yoziladigan akkaunt (masalan @admin)
"""
import asyncio
import csv
import html
import io
import logging
import os
import re
import secrets
import time
from datetime import datetime, timezone

import asyncpg
from aiohttp import ClientSession, web
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import BaseFilter, Command, CommandObject, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand, BufferedInputFile, CallbackQuery, ErrorEvent, InlineKeyboardButton,
    InlineKeyboardMarkup, KeyboardButton, Message, ReplyKeyboardMarkup, ReplyKeyboardRemove,
)

# ====================== SOZLAMALAR ======================
TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
CHANNEL = os.getenv("CHANNEL", "").strip()
VIP_CONTACT = os.getenv("VIP_CONTACT", "@admin")
PORT = int(os.getenv("PORT", "10000"))

log = logging.getLogger("admatch")
pool: asyncpg.Pool = None
router = Router()
admin = Router()
esc = html.escape

# Admin paneldan o'zgartiriladigan sozlamalar (bazada saqlanadi)
DEFAULTS = {
    "daily_likes": int(os.getenv("DAILY_LIKES", "30")),
    "super_daily": 3,      # VIP uchun kunlik super layk
    "ref_bonus": 5,        # taklif qilgan do'st uchun bonus layk
    "report_limit": 5,     # shuncha shikoyatdan keyin anketa avtomatik yashiriladi
    "maintenance": 0,      # 1 = texnik rejim
}
_cfg = {}

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
ALTER TABLE users ADD COLUMN IF NOT EXISTS bio TEXT DEFAULT '';
ALTER TABLE users ADD COLUMN IF NOT EXISTS seeking TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS notify BOOLEAN DEFAULT TRUE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS bonus_likes INT DEFAULT 0;
ALTER TABLE users ADD COLUMN IF NOT EXISTS ref_by BIGINT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS bot_blocked BOOLEAN DEFAULT FALSE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS vip_notified BOOLEAN DEFAULT FALSE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS reminded_at TIMESTAMPTZ;

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
ALTER TABLE reports ADD COLUMN IF NOT EXISTS reason TEXT;
CREATE TABLE IF NOT EXISTS daily_likes(
    user_id BIGINT, day DATE, cnt INT DEFAULT 0,
    PRIMARY KEY(user_id, day)
);
ALTER TABLE daily_likes ADD COLUMN IF NOT EXISTS supers INT DEFAULT 0;
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS promo(
    code TEXT PRIMARY KEY, days INT, max_uses INT DEFAULT 0, used INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS promo_uses(
    code TEXT, user_id BIGINT, created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY(code, user_id)
);
CREATE TABLE IF NOT EXISTS admin_log(
    id SERIAL PRIMARY KEY, admin_id BIGINT, action TEXT, target BIGINT,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_users_search ON users(is_active, is_banned);
CREATE INDEX IF NOT EXISTS idx_users_created ON users(created_at);
"""


# ====================== SOZLAMALAR (runtime) ======================
async def load_cfg():
    for r in await pool.fetch("SELECT key, value FROM settings"):
        try:
            _cfg[r["key"]] = int(r["value"])
        except ValueError:
            pass


def cfg(k):
    return _cfg.get(k, DEFAULTS[k])


async def set_cfg(k, v):
    _cfg[k] = v
    await pool.execute(
        "INSERT INTO settings(key,value) VALUES($1,$2) "
        "ON CONFLICT (key) DO UPDATE SET value=$2", k, str(v))


# ====================== DB YORDAMCHILARI ======================
async def get_user(uid):
    return await pool.fetchrow("SELECT * FROM users WHERE id=$1", uid)


def is_vip(u):
    return bool(u and u["vip_until"] and u["vip_until"] > datetime.now(timezone.utc))


def seek_of(u):
    return u["seeking"] or ("Qiz" if u["gender"] == "Yigit" else "Yigit")


async def set_reaction(a, b, v):
    await pool.execute(
        "INSERT INTO reactions(from_id,to_id,value) VALUES($1,$2,$3) "
        "ON CONFLICT (from_id,to_id) DO UPDATE SET value=$3, created_at=now()", a, b, v)


async def spend_like(uid, vip):
    """VIP cheklovsiz. Boshqalarga: kunlik limit, keyin bonus layklar."""
    if vip:
        return True
    row = await pool.fetchrow(
        "INSERT INTO daily_likes(user_id,day,cnt) VALUES($1,CURRENT_DATE,1) "
        "ON CONFLICT (user_id,day) DO UPDATE SET cnt=daily_likes.cnt+1 "
        "WHERE daily_likes.cnt < $2 RETURNING cnt", uid, cfg("daily_likes"))
    if row:
        return True
    left = await pool.fetchval(
        "UPDATE users SET bonus_likes=bonus_likes-1 WHERE id=$1 AND bonus_likes>0 "
        "RETURNING bonus_likes", uid)
    return left is not None


async def spend_super(uid):
    row = await pool.fetchrow(
        "INSERT INTO daily_likes(user_id,day,cnt,supers) VALUES($1,CURRENT_DATE,0,1) "
        "ON CONFLICT (user_id,day) DO UPDATE SET supers=daily_likes.supers+1 "
        "WHERE daily_likes.supers < $2 RETURNING supers", uid, cfg("super_daily"))
    return row is not None


async def likes_left(me):
    if is_vip(me):
        return None
    cnt = await pool.fetchval(
        "SELECT cnt FROM daily_likes WHERE user_id=$1 AND day=CURRENT_DATE", me["id"]) or 0
    return max(cfg("daily_likes") - cnt, 0) + (me["bonus_likes"] or 0)


async def next_profile(me):
    return await pool.fetchrow(
        """
        SELECT x.* FROM users x
        WHERE x.id<>$1 AND x.is_active AND NOT x.is_banned AND NOT x.bot_blocked
          AND ($2::text='all' OR x.gender=$2::text)
          AND COALESCE(x.seeking, CASE WHEN x.gender='Yigit' THEN 'Qiz' ELSE 'Yigit' END)
              IN ('all', $3::text)
          AND x.age BETWEEN $4 AND $5
          AND NOT EXISTS (SELECT 1 FROM reactions r WHERE r.from_id=$1 AND r.to_id=x.id)
        ORDER BY (x.vip_until IS NOT NULL AND x.vip_until>now()) DESC,
                 (lower(x.city)=lower($6)) DESC,
                 random()
        LIMIT 1
        """,
        me["id"], seek_of(me), me["gender"], me["min_age"], me["max_age"], me["city"])


async def next_liker(uid):
    return await pool.fetchrow(
        """
        SELECT u.*, r.value AS lv FROM reactions r JOIN users u ON u.id=r.from_id
        WHERE r.to_id=$1 AND r.value>=1 AND NOT u.is_banned
          AND NOT EXISTS (SELECT 1 FROM reactions q WHERE q.from_id=$1 AND q.to_id=r.from_id)
        ORDER BY r.value DESC, r.created_at DESC LIMIT 1
        """, uid)


async def count_likers(uid):
    return await pool.fetchval(
        """
        SELECT count(*) FROM reactions r JOIN users u ON u.id=r.from_id
        WHERE r.to_id=$1 AND r.value>=1 AND NOT u.is_banned
          AND NOT EXISTS (SELECT 1 FROM reactions q WHERE q.from_id=$1 AND q.to_id=r.from_id)
        """, uid)


async def delete_user_data(uid):
    await pool.execute("DELETE FROM reactions WHERE from_id=$1 OR to_id=$1", uid)
    await pool.execute("DELETE FROM matches WHERE a=$1 OR b=$1", uid)
    await pool.execute("DELETE FROM daily_likes WHERE user_id=$1", uid)
    await pool.execute("DELETE FROM reports WHERE from_id=$1 OR to_id=$1", uid)
    await pool.execute("DELETE FROM users WHERE id=$1", uid)


async def grant_vip(uid, days):
    await pool.execute(
        "UPDATE users SET vip_until=GREATEST(COALESCE(vip_until, now()), now()) "
        "+ $2 * interval '1 day', vip_notified=FALSE WHERE id=$1", uid, days)


async def alog(admin_id, action, target=None):
    await pool.execute(
        "INSERT INTO admin_log(admin_id,action,target) VALUES($1,$2,$3)", admin_id, action, target)


# ====================== XAVFSIZ YUBORISH ======================
async def send_safe(bot: Bot, uid, text, **kw):
    try:
        await bot.send_message(uid, text, **kw)
        return True
    except TelegramForbiddenError:
        await pool.execute("UPDATE users SET bot_blocked=TRUE WHERE id=$1", uid)
    except Exception as e:
        log.warning("send_safe %s: %s", uid, e)
    return False


async def send_photo_safe(bot: Bot, uid, photo, caption, kb=None):
    try:
        await bot.send_photo(uid, photo, caption=caption, reply_markup=kb)
        return True
    except TelegramForbiddenError:
        await pool.execute("UPDATE users SET bot_blocked=TRUE WHERE id=$1", uid)
    except Exception as e:
        log.warning("send_photo_safe %s: %s", uid, e)
    return False


async def notify_admins(bot: Bot, text, kb=None):
    for a in ADMIN_IDS:
        await send_safe(bot, a, text, reply_markup=kb)


# ====================== KLAVIATURALAR / MATN ======================
BTN_SEARCH = "🔍 Qidirish"
BTN_LIKES = "💌 Menga kim layk bosdi"
BTN_MATCH = "💞 Matchlarim"
BTN_PROFILE = "👤 Mening profilim"
BTN_SET = "⚙️ Sozlamalar"
BTN_VIP = "👑 VIP PREMIUM"
BTN_REF = "🎁 Do'st taklif qilish"
BTN_ADMIN = "🛠 Admin panel"


def B(text, data=None, url=None):
    if url:
        return InlineKeyboardButton(text=text, url=url)
    return InlineKeyboardButton(text=text, callback_data=data)


def IK(*rows):
    return InlineKeyboardMarkup(inline_keyboard=[list(r) for r in rows])


def main_menu(uid):
    rows = [
        [KeyboardButton(text=BTN_SEARCH), KeyboardButton(text=BTN_LIKES)],
        [KeyboardButton(text=BTN_MATCH), KeyboardButton(text=BTN_PROFILE)],
        [KeyboardButton(text=BTN_SET), KeyboardButton(text=BTN_VIP)],
        [KeyboardButton(text=BTN_REF)],
    ]
    if uid in ADMIN_IDS:
        rows.append([KeyboardButton(text=BTN_ADMIN)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def gender_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="👨 Yigit"), KeyboardButton(text="👩 Qiz")]],
        resize_keyboard=True, one_time_keyboard=True)


def seeking_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="👨 Yigitlar"), KeyboardButton(text="👩 Qizlar"),
                   KeyboardButton(text="👥 Hammasi")]],
        resize_keyboard=True, one_time_keyboard=True)


def react_kb(tid, src):
    return IK(
        [B("❤️", f"lk:{tid}:{src}"), B("💫 Super", f"sl:{tid}:{src}"), B("👎", f"ds:{tid}:{src}")],
        [B("🚩 Shikoyat", f"rp:{tid}:{src}"), B("⬅️ Orqaga", "back")])


REPORT_REASONS = {"s": "Spam / reklama", "f": "Soxta anketa", "a": "Haqorat / odobsizlik", "o": "Boshqa"}


def report_kb(tid, src):
    rows = [[B(v, f"rr:{tid}:{src}:{k}")] for k, v in REPORT_REASONS.items()]
    rows.append([B("❌ Bekor qilish", f"rr:{tid}:{src}:x")])
    return IK(*rows)


def caption(u):
    icon = "👨" if u["gender"] == "Yigit" else "👩"
    vip = " 👑" if is_vip(u) else ""
    bio = f"\n💬 {esc(u['bio'])}" if u["bio"] else ""
    return (f"<b>{esc(u['name'])}</b>{vip}, {u['age']}, {esc(u['city'])}, {icon} {u['gender']}{bio}\n\n"
            f"🎯 <b>Qiziqishlar:</b> {esc(u['interests'])}")


def contact(u):
    if u["username"]:
        return f"@{esc(u['username'])}"
    return f"<a href='tg://user?id={u['id']}'>{esc(u['name'])}</a>"


def sub_kb():
    return IK([B("📢 Kanalga o'tish", url=f"https://t.me/{CHANNEL.lstrip('@')}")],
              [B("✅ Tekshirish", "chk")])


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
        await bot.send_message(chat_id, "📢 Botdan foydalanish uchun kanalimizga obuna bo'ling:",
                               reply_markup=sub_kb())
        return None
    return me


# ====================== VALIDATSIYA ======================
LINK = re.compile(r"(https?://|t\.me|www\.|@\w)", re.I)


def clean(text, lo, hi):
    t = " ".join((text or "").split())
    if not lo <= len(t) <= hi or LINK.search(t):
        return None
    return t


def parse_field(field, text):
    if field == "age":
        t = (text or "").strip()
        return int(t) if t.isdigit() and 16 <= int(t) <= 70 else None
    if field == "bio":
        t = (text or "").strip()
        return "" if t == "-" else clean(t, 1, 150)
    return clean(text, *{"name": (2, 30), "city": (2, 30), "interests": (2, 100)}[field])


FIELD_ERR = {
    "name": "Ism 2-30 belgi bo'lsin (havola va @ bo'lmasin).",
    "age": "Yoshni raqam bilan yozing (16-70).",
    "city": "Shahar nomini to'g'ri yozing (2-30 belgi).",
    "interests": "Qiziqishlar 2-100 belgi bo'lsin (havola va @ bo'lmasin).",
    "bio": "Matn 150 belgigacha bo'lsin (havola va @ bo'lmasin). O'tkazish uchun: -",
}
EDITABLE = {"name", "age", "city", "photo", "bio", "interests"}


# ====================== MIDDLEWARE ======================
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
        self.last[user.id] = now
        if len(self.last) > 5000:
            self.last = {k: v for k, v in self.last.items() if now - v < 60}
        is_admin = user.id in ADMIN_IDS
        row = await pool.fetchrow(
            "UPDATE users SET last_seen=now(), bot_blocked=FALSE WHERE id=$1 RETURNING is_banned",
            user.id)
        if row and row["is_banned"] and not is_admin:
            if isinstance(event, CallbackQuery):
                await event.answer("🚫 Siz bloklangansiz.", show_alert=True)
            else:
                await event.answer("🚫 Siz bloklangansiz.")
            return
        if cfg("maintenance") and not is_admin:
            txt = "🛠 Botda texnik ishlar ketmoqda. Birozdan keyin urinib ko'ring."
            if isinstance(event, CallbackQuery):
                await event.answer(txt, show_alert=True)
            else:
                await event.answer(txt)
            return
        return await handler(event, data)


class IsAdmin(BaseFilter):
    async def __call__(self, event) -> bool:
        return bool(event.from_user and event.from_user.id in ADMIN_IDS)


admin.message.filter(IsAdmin())
admin.callback_query.filter(IsAdmin())


async def on_error(event: ErrorEvent):
    log.exception("Handler xatosi: %s", event.exception)
    return True


# ====================== HOLATLAR ======================
class Reg(StatesGroup):
    name = State()
    gender = State()
    seeking = State()
    age = State()
    city = State()
    photo = State()
    interests = State()
    bio = State()


class Edit(StatesGroup):
    value = State()


class SetAge(StatesGroup):
    rng = State()


class PromoIn(StatesGroup):
    code = State()


# ====================== START / RO'YXATDAN O'TISH ======================
WELCOME = (
    "👋 <b>AdMatch Bot</b>ga xush kelibsiz!\n\n"
    "Bu yerda yangi insonlar bilan tanishishingiz mumkin.\n\n"
    "❤️ Layk bosing: agar u ham yoqtirsa, match bo'ladi\n"
    "💫 Super layk: sizni ajratib ko'rsatadi (VIP)\n"
    "👎 Yoqmasa: o'tkazib yuboring")


@router.message(CommandStart())
async def start(m: Message, state: FSMContext, bot: Bot, command: CommandObject):
    await state.clear()
    ref = None
    if command.args and command.args.startswith("ref_") and command.args[4:].isdigit():
        ref = int(command.args[4:])
    if await get_user(m.from_user.id):
        if not await subscribed(bot, m.from_user.id):
            return await m.answer("📢 Avval kanalga obuna bo'ling:", reply_markup=sub_kb())
        await m.answer("Menyudan tanlang 👇", reply_markup=main_menu(m.from_user.id))
    else:
        await m.answer(WELCOME)
        await state.update_data(ref=ref)
        await begin_reg(m, state)


@router.message(Command("menu"))
@router.message(Command("cancel"))
async def cmd_menu(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Menyudan tanlang 👇", reply_markup=main_menu(m.from_user.id))


@router.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer(
        "ℹ️ <b>Yordam</b>\n\n"
        "/start - botni boshlash\n/menu - asosiy menyu\n/cancel - amalni bekor qilish\n\n"
        f"Savollar uchun: {esc(VIP_CONTACT)}")


async def begin_reg(m: Message, state: FSMContext):
    await m.answer("Anketa to'ldiramiz.\n\n✏️ Ismingiz?", reply_markup=ReplyKeyboardRemove())
    await state.set_state(Reg.name)


@router.message(Reg.name, F.text)
async def reg_name(m: Message, state: FSMContext):
    v = parse_field("name", m.text)
    if v is None:
        return await m.answer(FIELD_ERR["name"])
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
    await m.answer("Kimni qidiryapsiz?", reply_markup=seeking_kb())
    await state.set_state(Reg.seeking)


@router.message(Reg.seeking, F.text)
async def reg_seeking(m: Message, state: FSMContext):
    t = m.text.lower()
    if "yigit" in t:
        s = "Yigit"
    elif "qiz" in t:
        s = "Qiz"
    elif "ham" in t:
        s = "all"
    else:
        return await m.answer("Tugmalardan birini tanlang 👇", reply_markup=seeking_kb())
    await state.update_data(seeking=s)
    await m.answer("Yoshingiz?", reply_markup=ReplyKeyboardRemove())
    await state.set_state(Reg.age)


@router.message(Reg.age, F.text)
async def reg_age(m: Message, state: FSMContext):
    v = parse_field("age", m.text)
    if v is None:
        return await m.answer(FIELD_ERR["age"])
    await state.update_data(age=v)
    await m.answer("Qaysi shahardansiz?")
    await state.set_state(Reg.city)


@router.message(Reg.city, F.text)
async def reg_city(m: Message, state: FSMContext):
    v = parse_field("city", m.text)
    if v is None:
        return await m.answer(FIELD_ERR["city"])
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
    v = parse_field("interests", m.text)
    if v is None:
        return await m.answer(FIELD_ERR["interests"])
    await state.update_data(interests=v)
    await m.answer("💬 O'zingiz haqingizda 1-2 jumla yozing (o'tkazish uchun: <code>-</code>)")
    await state.set_state(Reg.bio)


@router.message(Reg.bio, F.text)
async def reg_bio(m: Message, state: FSMContext, bot: Bot):
    bio = parse_field("bio", m.text)
    if bio is None:
        return await m.answer(FIELD_ERR["bio"])
    d = await state.get_data()
    uid = m.from_user.id
    existed = await get_user(uid)
    await pool.execute(
        """
        INSERT INTO users(id,username,name,gender,seeking,age,city,photo,interests,bio)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT (id) DO UPDATE SET username=$2,name=$3,gender=$4,seeking=$5,age=$6,
            city=$7,photo=$8,interests=$9,bio=$10,is_active=TRUE
        """,
        uid, m.from_user.username, d["name"], d["gender"], d["seeking"], d["age"],
        d["city"], d["photo"], d["interests"], bio)
    ref = d.get("ref")
    await state.clear()
    if not existed and ref and ref != uid:
        inv = await get_user(ref)
        if inv:
            bonus = cfg("ref_bonus")
            await pool.execute("UPDATE users SET ref_by=$2 WHERE id=$1", uid, ref)
            await pool.execute("UPDATE users SET bonus_likes=bonus_likes+$2 WHERE id=$1", ref, bonus)
            await send_safe(bot, ref, f"🎁 Do'stingiz qo'shildi! Sizga +{bonus} bonus layk berildi.")
    await m.answer("✅ Anketa saqlandi!", reply_markup=main_menu(uid))
    await show_profile(m, uid)


# ====================== PROFIL ======================
async def show_profile(m: Message, uid):
    u = await get_user(uid)
    status = "" if u["is_active"] else "\n\n🙈 <i>Anketangiz yashirilgan</i>"
    left = await likes_left(u)
    info = ("\n\n👑 <b>VIP faol:</b> " + f"{u['vip_until']:%d.%m.%Y} gacha" if is_vip(u)
            else f"\n\n❤️ Bugungi layklar: <b>{left}</b>")
    await m.answer_photo(u["photo"], caption=caption(u) + info + status,
                         reply_markup=IK([B("✏️ Tahrirlash", "st:edit")]))


@router.message(F.text == BTN_PROFILE)
async def my_profile(m: Message, bot: Bot):
    if await gate(bot, m.from_user.id, m.chat.id):
        await show_profile(m, m.from_user.id)


@router.callback_query(F.data == "redo")
async def redo(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await begin_reg(c.message, state)


# ====================== TAHRIRLASH ======================
EDIT_LABELS = {"name": "👤 Ism", "age": "🎂 Yosh", "city": "🏙 Shahar", "photo": "📸 Rasm",
               "bio": "💬 Haqimda", "interests": "🎯 Qiziqish"}
EDIT_PROMPT = {"name": "Yangi ismni yozing:", "age": "Yangi yoshni yozing:",
               "city": "Yangi shaharni yozing:", "photo": "📸 Yangi rasm yuboring:",
               "bio": "Yangi matnni yozing (o'chirish uchun: <code>-</code>):",
               "interests": "Yangi qiziqishlarni yozing:"}


@router.callback_query(F.data == "st:edit")
async def st_edit(c: CallbackQuery):
    await c.answer()
    keys = list(EDIT_LABELS)
    rows = [[B(EDIT_LABELS[k], f"ed:{k}") for k in keys[i:i + 2]] for i in range(0, len(keys), 2)]
    rows.append([B("🔄 Hammasini qayta to'ldirish", "redo")])
    await c.message.answer("Nimani o'zgartiramiz?", reply_markup=IK(*rows))


@router.callback_query(F.data.regexp(r"^ed:(name|age|city|photo|bio|interests)$"))
async def ed_pick(c: CallbackQuery, state: FSMContext):
    await c.answer()
    f = c.data.split(":")[1]
    await state.set_state(Edit.value)
    await state.update_data(field=f)
    await c.message.answer(EDIT_PROMPT[f] + "\n\nBekor qilish: /cancel")


@router.message(Edit.value)
async def ed_value(m: Message, state: FSMContext):
    f = (await state.get_data()).get("field")
    if f not in EDITABLE:
        await state.clear()
        return
    if f == "photo":
        if not m.photo:
            return await m.answer("Iltimos, rasm yuboring 📸")
        val = m.photo[-1].file_id
    else:
        val = parse_field(f, m.text) if m.text else None
        if val is None:
            return await m.answer(FIELD_ERR[f])
    await pool.execute(f"UPDATE users SET {f}=$2 WHERE id=$1", m.from_user.id, val)
    await state.clear()
    await m.answer("✅ Saqlandi!", reply_markup=main_menu(m.from_user.id))
    await show_profile(m, m.from_user.id)


# ====================== SOZLAMALAR ======================
SEEK_LABEL = {"Yigit": "👨 Yigitlar", "Qiz": "👩 Qizlar", "all": "👥 Hammasi"}


def settings_kb(u):
    return IK(
        [B(f"🎂 Qidiruv yoshi: {u['min_age']}-{u['max_age']}", "st:age")],
        [B(f"🎯 Qidiraman: {SEEK_LABEL[seek_of(u)]}", "st:seek")],
        [B(f"🔔 Bildirishnoma: {'yoqiq' if u['notify'] is not False else 'ochiq emas'}", "st:ntf")],
        [B("🙈 Anketani yashirish" if u["is_active"] else "👁 Anketani ko'rsatish", "st:vis")],
        [B("✏️ Anketani tahrirlash", "st:edit")],
        [B("🗑 Akkauntni o'chirish", "st:del")])


async def refresh_settings(c: CallbackQuery):
    u = await get_user(c.from_user.id)
    try:
        await c.message.edit_reply_markup(reply_markup=settings_kb(u))
    except TelegramBadRequest:
        pass


@router.message(F.text == BTN_SET)
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


@router.callback_query(F.data == "st:seek")
async def st_seek(c: CallbackQuery):
    u = await get_user(c.from_user.id)
    if not u:
        return await c.answer("Avval /start bosing", show_alert=True)
    order = ["Yigit", "Qiz", "all"]
    nxt = order[(order.index(seek_of(u)) + 1) % 3]
    await pool.execute("UPDATE users SET seeking=$2 WHERE id=$1", u["id"], nxt)
    await c.answer(SEEK_LABEL[nxt])
    await refresh_settings(c)


@router.callback_query(F.data == "st:ntf")
async def st_ntf(c: CallbackQuery):
    u = await pool.fetchrow(
        "UPDATE users SET notify=NOT COALESCE(notify, TRUE) WHERE id=$1 RETURNING notify", c.from_user.id)
    if not u:
        return await c.answer("Avval /start bosing", show_alert=True)
    await c.answer("Bildirishnoma yoqildi" if u["notify"] else "Bildirishnoma o'chirildi")
    await refresh_settings(c)


@router.callback_query(F.data == "st:vis")
async def st_vis(c: CallbackQuery):
    u = await pool.fetchrow(
        "UPDATE users SET is_active=NOT is_active WHERE id=$1 RETURNING *", c.from_user.id)
    if not u:
        return await c.answer("Avval /start bosing", show_alert=True)
    await c.answer("Anketa ko'rinadi" if u["is_active"] else "Anketa yashirildi")
    try:
        await c.message.edit_reply_markup(reply_markup=settings_kb(u))
    except TelegramBadRequest:
        pass


@router.callback_query(F.data == "st:del")
async def st_del(c: CallbackQuery):
    await c.answer()
    await c.message.answer("⚠️ Anketa va barcha layklaringiz o'chadi. Rostdan o'chiraymi?",
                           reply_markup=IK([B("✅ Ha, o'chirish", "st:delok"), B("❌ Yo'q", "back")]))


@router.callback_query(F.data == "st:delok")
async def st_delok(c: CallbackQuery):
    await c.answer()
    await delete_user_data(c.from_user.id)
    await c.message.answer("🗑 Akkaunt o'chirildi. Qaytish uchun /start bosing.",
                           reply_markup=ReplyKeyboardRemove())


# ====================== QIDIRUV ======================
async def send_next(bot: Bot, chat_id, uid):
    me = await get_user(uid)
    if not me:
        return
    for _ in range(5):
        p = await next_profile(me)
        if not p:
            return await bot.send_message(
                chat_id, "😔 Hozircha yangi anketalar yo'q.\nQidiruv yoshini kengaytiring yoki keyinroq urinib ko'ring.",
                reply_markup=main_menu(uid))
        try:
            return await bot.send_photo(chat_id, p["photo"], caption=caption(p),
                                        reply_markup=react_kb(p["id"], "s"))
        except TelegramBadRequest:
            await set_reaction(uid, p["id"], 0)  # rasmi buzilgan anketani o'tkazamiz


@router.message(F.text == BTN_SEARCH)
async def search(m: Message, bot: Bot):
    if await gate(bot, m.from_user.id, m.chat.id):
        await send_next(bot, m.chat.id, m.from_user.id)


async def show_liker(bot: Bot, chat_id, uid):
    p = await next_liker(uid)
    if not p:
        return await bot.send_message(chat_id, "Hozircha yangi layklar yo'q.",
                                      reply_markup=main_menu(uid))
    head = "💫 <b>Sizni SUPER layk qildi:</b>" if p["lv"] == 2 else "❤️ <b>Sizga shu odam layk bosdi:</b>"
    await bot.send_photo(chat_id, p["photo"], caption=f"{head}\n\n{caption(p)}",
                         reply_markup=react_kb(p["id"], "l"))


@router.message(F.text == BTN_LIKES)
async def who_liked(m: Message, bot: Bot):
    if await gate(bot, m.from_user.id, m.chat.id):
        n = await count_likers(m.from_user.id)
        if n:
            await m.answer(f"💌 Sizni <b>{n}</b> kishi yoqtirgan!")
        await show_liker(bot, m.chat.id, m.from_user.id)


@router.message(F.text == BTN_MATCH)
async def my_matches(m: Message, bot: Bot):
    if not await gate(bot, m.from_user.id, m.chat.id):
        return
    uid = m.from_user.id
    rows = await pool.fetch(
        """
        SELECT u.* FROM matches mt
        JOIN users u ON u.id = CASE WHEN mt.a=$1 THEN mt.b ELSE mt.a END
        WHERE (mt.a=$1 OR mt.b=$1) AND NOT u.is_banned
        ORDER BY mt.created_at DESC LIMIT 15
        """, uid)
    if not rows:
        return await m.answer("💞 Hozircha matchlar yo'q. Ko'proq layk bosing!")
    lines = [f"• <b>{esc(u['name'])}</b>, {u['age']}, {esc(u['city'])} - {contact(u)}" for u in rows]
    await m.answer("💞 <b>Matchlaringiz:</b>\n\n" + "\n".join(lines))


async def notify_match(bot: Bot, to_user, other):
    await send_photo_safe(
        bot, to_user["id"], other["photo"],
        f"🎉 <b>MATCH!</b>\nSiz va {esc(other['name'])} bir-biringizni yoqtirdingiz.\n\n"
        f"💬 Yozing: {contact(other)}")


async def handle_like(bot: Bot, me, tid, sup):
    uid = me["id"]
    await set_reaction(uid, tid, 2 if sup else 1)
    target = await get_user(tid)
    if not target:
        return
    prev = await pool.fetchval("SELECT value FROM reactions WHERE from_id=$1 AND to_id=$2", tid, uid)
    if prev is not None and prev >= 1:  # o'zaro layk -> MATCH
        row = await pool.fetchrow(
            "INSERT INTO matches(a,b) VALUES(LEAST($1::bigint,$2::bigint),GREATEST($1::bigint,$2::bigint)) "
            "ON CONFLICT DO NOTHING RETURNING a", uid, tid)
        if row:
            await notify_match(bot, me, target)
            await notify_match(bot, target, me)
    elif prev is None and target["notify"] is not False and not target["bot_blocked"]:
        head = "💫 <b>Sizni SUPER layk qildi:</b>" if sup else "❤️ <b>Sizga shu odam layk bosdi:</b>"
        await send_photo_safe(bot, tid, me["photo"], f"{head}\n\n{caption(me)}", react_kb(uid, "n"))
    # prev == 0 bo'lsa (u yoqtirmagan) hech narsa yuborilmaydi


async def continue_flow(bot: Bot, chat_id, uid, src):
    if src == "s":
        await send_next(bot, chat_id, uid)
    elif src == "l":
        await show_liker(bot, chat_id, uid)


@router.callback_query(F.data.regexp(r"^(lk|sl|ds|rp):\d+:[snl]$"))
async def react(c: CallbackQuery, bot: Bot):
    action, tid, src = c.data.split(":")
    tid, uid = int(tid), c.from_user.id
    me = await get_user(uid)
    if not me:
        return await c.answer("Avval /start bosing", show_alert=True)
    if tid == uid:
        return await c.answer()
    if action in ("lk", "sl"):
        mine = await pool.fetchval("SELECT value FROM reactions WHERE from_id=$1 AND to_id=$2", uid, tid)
        if mine is not None and mine >= 1:  # qayta bosish - ikki marta hisoblamaymiz
            await c.answer()
            return await continue_flow(bot, c.message.chat.id, uid, src)
    if action == "sl":
        if not is_vip(me):
            return await c.answer("💫 Super layk faqat VIP uchun.", show_alert=True)
        if not await spend_super(uid):
            return await c.answer(f"💫 Bugungi super layk limiti ({cfg('super_daily')} ta) tugadi.",
                                  show_alert=True)
    elif action == "lk" and not await spend_like(uid, is_vip(me)):
        return await c.answer(
            f"❌ Bugungi layk limiti ({cfg('daily_likes')} ta) tugadi.\n"
            "👑 VIP bilan cheklovsiz yoki do'st taklif qilib bonus oling!", show_alert=True)
    await c.answer()
    try:
        await c.message.delete()
    except Exception:
        pass
    if action == "rp":
        return await c.message.answer("🚩 Shikoyat sababini tanlang:", reply_markup=report_kb(tid, src))
    if action == "ds":
        await set_reaction(uid, tid, 0)
    else:
        await handle_like(bot, me, tid, action == "sl")
    await continue_flow(bot, c.message.chat.id, uid, src)


@router.callback_query(F.data.regexp(r"^rr:\d+:[snl]:[sfaox]$"))
async def report_reason(c: CallbackQuery, bot: Bot):
    _, tid, src, code = c.data.split(":")
    tid, uid = int(tid), c.from_user.id
    await c.answer()
    try:
        await c.message.delete()
    except Exception:
        pass
    if code != "x":
        already = await pool.fetchval("SELECT 1 FROM reports WHERE from_id=$1 AND to_id=$2", uid, tid)
        if not already:
            await pool.execute("INSERT INTO reports(from_id,to_id,reason) VALUES($1,$2,$3)",
                               uid, tid, REPORT_REASONS[code])
            cnt = await pool.fetchval(
                "SELECT count(DISTINCT from_id) FROM reports WHERE to_id=$1 AND NOT handled", tid)
            extra = ""
            if cnt >= cfg("report_limit"):
                await pool.execute("UPDATE users SET is_active=FALSE WHERE id=$1", tid)
                extra = f"\n⚠️ {cnt} ta shikoyat - anketa avtomatik yashirildi."
            await notify_admins(
                bot, f"🚩 Yangi shikoyat. ID <code>{tid}</code> ({esc(REPORT_REASONS[code])}){extra}",
                IK([B("🚩 Ko'rish", "adm:rep")]))
        await set_reaction(uid, tid, 0)
        await bot.send_message(c.message.chat.id, "🚩 Shikoyat yuborildi, rahmat.")
    await continue_flow(bot, c.message.chat.id, uid, src)


@router.callback_query(F.data == "back")
async def back(c: CallbackQuery):
    await c.answer()
    try:
        await c.message.delete()
    except Exception:
        pass
    await c.message.answer("Menyudan tanlang 👇", reply_markup=main_menu(c.from_user.id))


@router.callback_query(F.data == "noop")
async def noop(c: CallbackQuery):
    await c.answer()


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


# ====================== VIP / PROMOKOD / REFERAL ======================
@router.message(F.text == BTN_VIP)
async def vip(m: Message, bot: Bot):
    me = await gate(bot, m.from_user.id, m.chat.id)
    if not me:
        return
    status = f"✅ Faol: {me['vip_until']:%d.%m.%Y} gacha" if is_vip(me) else "❌ Faol emas"
    rows = [[B("🎟 Promokod kiritish", "pm:enter")]]
    if VIP_CONTACT.startswith("@"):
        rows.append([B("💬 VIP olish", url=f"https://t.me/{VIP_CONTACT.lstrip('@')}")])
    await m.answer(
        "👑 <b>VIP PREMIUM</b>\n\n"
        "✅ Cheklovsiz layk\n"
        f"✅ Kuniga {cfg('super_daily')} ta 💫 super layk\n"
        "✅ Anketangiz qidiruvda birinchi ko'rsatiladi\n"
        "✅ Ismingiz yonida 👑 belgisi\n\n"
        f"Bepul rejada kuniga {cfg('daily_likes')} ta layk.\n"
        f"Holat: {status}\n\n"
        f"VIP olish uchun {esc(VIP_CONTACT)} ga yozing va ID raqamingizni yuboring:\n"
        f"<code>{m.from_user.id}</code>",
        reply_markup=IK(*rows))


@router.callback_query(F.data == "pm:enter")
async def promo_enter(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.set_state(PromoIn.code)
    await c.message.answer("🎟 Promokodni yozing:\n\nBekor qilish: /cancel")


async def redeem(uid, code):
    async with pool.acquire() as con:
        async with con.transaction():
            p = await con.fetchrow("SELECT * FROM promo WHERE code=$1 FOR UPDATE", code)
            if not p:
                return False, "❌ Bunday promokod yo'q."
            if p["max_uses"] and p["used"] >= p["max_uses"]:
                return False, "❌ Promokod limiti tugagan."
            if await con.fetchval("SELECT 1 FROM promo_uses WHERE code=$1 AND user_id=$2", code, uid):
                return False, "❌ Siz bu promokoddan foydalangansiz."
            await con.execute("INSERT INTO promo_uses(code,user_id) VALUES($1,$2)", code, uid)
            await con.execute("UPDATE promo SET used=used+1 WHERE code=$1", code)
            await con.execute(
                "UPDATE users SET vip_until=GREATEST(COALESCE(vip_until, now()), now()) "
                "+ $2 * interval '1 day', vip_notified=FALSE WHERE id=$1", uid, p["days"])
            return True, f"🎉 Tabriklaymiz! Sizga {p['days']} kunlik 👑 VIP berildi."


@router.message(PromoIn.code, F.text)
async def promo_code(m: Message, state: FSMContext):
    code = m.text.strip().upper()
    if not re.match(r"^[A-Z0-9_-]{3,20}$", code):
        return await m.answer("Promokod noto'g'ri formatda. Qayta yozing yoki /cancel.")
    if not await get_user(m.from_user.id):
        await state.clear()
        return await m.answer("Avval /start bosib anketa to'ldiring.")
    ok, msg = await redeem(m.from_user.id, code)
    await state.clear()
    await m.answer(msg, reply_markup=main_menu(m.from_user.id))


@router.message(F.text == BTN_REF)
async def referral(m: Message, bot: Bot):
    if not await gate(bot, m.from_user.id, m.chat.id):
        return
    me = await bot.me()
    n = await pool.fetchval("SELECT count(*) FROM users WHERE ref_by=$1", m.from_user.id)
    await m.answer(
        "🎁 <b>Do'st taklif qiling - bonus oling!</b>\n\n"
        f"Har bir qo'shilgan do'stingiz uchun <b>+{cfg('ref_bonus')}</b> bonus layk.\n\n"
        f"👥 Taklif qilganlar: <b>{n}</b>\n\n"
        f"🔗 Sizning havolangiz:\nhttps://t.me/{me.username}?start=ref_{m.from_user.id}")


# ====================== ADMIN PANEL ======================
class Bc(StatesGroup):
    content = State()
    confirm = State()


class AdmUser(StatesGroup):
    query = State()


class AdmDm(StatesGroup):
    text = State()


class AdmPromo(StatesGroup):
    new = State()


def admin_kb():
    return IK(
        [B("📊 Statistika", "adm:stats"), B("📢 Xabar yuborish", "adm:bc")],
        [B("🔎 Foydalanuvchi", "adm:user"), B("🚩 Shikoyatlar", "adm:rep")],
        [B("🎟 Promokodlar", "adm:promo"), B("⚙️ Sozlamalar", "adm:cfg")],
        [B("📥 CSV eksport", "adm:csv"), B("🧾 Loglar", "adm:log")])


@admin.message(Command("admin"))
@admin.message(F.text == BTN_ADMIN)
async def admin_panel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("🛠 <b>Admin panel</b>", reply_markup=admin_kb())


# ---- tezkor komandalar
@admin.message(Command("ban"))
async def cmd_ban(m: Message, command: CommandObject):
    if not command.args or not command.args.strip().isdigit():
        return await m.answer("Foydalanish: <code>/ban ID</code>")
    uid = int(command.args)
    await pool.execute("UPDATE users SET is_banned=TRUE WHERE id=$1", uid)
    await alog(m.from_user.id, "ban", uid)
    await m.answer(f"🚫 {uid} bloklandi.")


@admin.message(Command("unban"))
async def cmd_unban(m: Message, command: CommandObject):
    if not command.args or not command.args.strip().isdigit():
        return await m.answer("Foydalanish: <code>/unban ID</code>")
    uid = int(command.args)
    await pool.execute("UPDATE users SET is_banned=FALSE WHERE id=$1", uid)
    await alog(m.from_user.id, "unban", uid)
    await m.answer(f"✅ {uid} blokdan chiqarildi.")


@admin.message(Command("vip"))
async def cmd_vip(m: Message, command: CommandObject, bot: Bot):
    p = (command.args or "").split()
    if len(p) != 2 or not p[0].isdigit() or not p[1].isdigit():
        return await m.answer("Foydalanish: <code>/vip ID KUN</code>")
    uid, days = int(p[0]), int(p[1])
    if not await get_user(uid):
        return await m.answer("❌ Foydalanuvchi topilmadi.")
    await grant_vip(uid, days)
    await alog(m.from_user.id, f"vip+{days}", uid)
    await send_safe(bot, uid, f"👑 Tabriklaymiz! Sizga {days} kunlik VIP berildi.")
    await m.answer(f"✅ {uid} ga {days} kun VIP berildi.")


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
               count(*) FILTER (WHERE bot_blocked) blocked,
               count(*) FILTER (WHERE ref_by IS NOT NULL) refs
        FROM users""")
    r = await pool.fetchrow("""
        SELECT count(*) FILTER (WHERE value>=1) likes,
               count(*) FILTER (WHERE value=2) supers,
               count(*) FILTER (WHERE value=0) dislikes FROM reactions""")
    matches = await pool.fetchval("SELECT count(*) FROM matches")
    reports = await pool.fetchval("SELECT count(*) FROM reports WHERE NOT handled")
    cities = await pool.fetch(
        "SELECT initcap(lower(city)) c, count(*) n FROM users GROUP BY 1 ORDER BY n DESC LIMIT 5")
    days = await pool.fetch(
        "SELECT created_at::date d, count(*) n FROM users "
        "WHERE created_at >= now() - interval '7 days' GROUP BY 1 ORDER BY 1")
    top = "\n".join(f"  • {esc(x['c'])}: {x['n']}" for x in cities) or "  —"
    mx = max([x["n"] for x in days] or [1])
    chart = "\n".join(f"<code>{x['d']:%d.%m} {'█' * max(1, round(x['n'] * 10 / mx))} {x['n']}</code>"
                      for x in days) or "  —"
    rate = f"{matches * 100 / r['likes']:.1f}%" if r["likes"] else "—"
    await c.message.answer(
        "📊 <b>Statistika</b>\n\n"
        f"👥 Jami: <b>{s['total']}</b>\n"
        f"👨 Yigitlar: {s['boys']}   👩 Qizlar: {s['girls']}\n"
        f"🆕 Bugun qo'shilgan: {s['today']}\n"
        f"🟢 24 soatda faol: {s['active24']}   7 kunda: {s['active7']}\n"
        f"👑 VIP: {s['vip']}\n"
        f"🚫 Bloklangan: {s['banned']}   🙈 Yashirin: {s['hidden']}\n"
        f"🤖 Botni bloklaganlar: {s['blocked']}\n"
        f"🎁 Referal orqali: {s['refs']}\n\n"
        f"❤️ Laykler: {r['likes']} (💫 {r['supers']})   👎 {r['dislikes']}\n"
        f"🎉 Matchlar: {matches} (konversiya {rate})\n"
        f"🚩 Ko'rilmagan shikoyatlar: {reports}\n\n"
        f"🏙 Top shaharlar:\n{top}\n\n"
        f"📈 So'nggi 7 kun (yangi a'zolar):\n{chart}")


# ---- xabar yuborish (rassilka)
BC_TARGETS = {
    "all": ("👥 Hammaga", "TRUE"),
    "Qiz": ("👩 Qizlarga", "gender='Qiz'"),
    "Yigit": ("👨 Yigitlarga", "gender='Yigit'"),
    "vip": ("👑 VIP larga", "vip_until>now()"),
    "free": ("🆓 Oddiylarga", "(vip_until IS NULL OR vip_until<=now())"),
}
BC = {"stop": False, "running": False}


@admin.callback_query(F.data == "adm:bc")
async def adm_bc(c: CallbackQuery):
    await c.answer()
    keys = list(BC_TARGETS)
    rows = [[B(BC_TARGETS[k][0], f"bc:t:{k}") for k in keys[i:i + 2]] for i in range(0, len(keys), 2)]
    await c.message.answer("Kimga yuboramiz?", reply_markup=IK(*rows))


@admin.callback_query(F.data.regexp(r"^bc:t:(all|Qiz|Yigit|vip|free)$"))
async def bc_target(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.update_data(target=c.data.split(":")[2])
    await state.set_state(Bc.content)
    await c.message.answer("Yuboriladigan xabarni jo'nating (matn, rasm yoki video):")


@admin.message(Bc.content)
async def bc_content(m: Message, state: FSMContext):
    await state.update_data(chat_id=m.chat.id, msg_id=m.message_id)
    await state.set_state(Bc.confirm)
    await m.answer("👆 Shu xabar yuboriladi. Tasdiqlaysizmi?",
                   reply_markup=IK([B("✅ Yuborish", "bc:go"), B("❌ Bekor", "bc:no")]))


@admin.callback_query(Bc.confirm, F.data == "bc:no")
async def bc_no(c: CallbackQuery, state: FSMContext):
    await c.answer("Bekor qilindi")
    await state.clear()


@admin.callback_query(Bc.confirm, F.data == "bc:go")
async def bc_go(c: CallbackQuery, state: FSMContext, bot: Bot):
    d = await state.get_data()
    await state.clear()
    if BC["running"]:
        return await c.answer("Boshqa xabar yuborilmoqda, kuting.", show_alert=True)
    await c.answer("Yuborish boshlandi")
    await alog(c.from_user.id, f"broadcast:{d['target']}")
    asyncio.create_task(run_broadcast(bot, c.from_user.id, d["target"], d["chat_id"], d["msg_id"]))


@admin.callback_query(F.data == "bc:stop")
async def bc_stop(c: CallbackQuery):
    BC["stop"] = True
    await c.answer("To'xtatilmoqda...")


async def run_broadcast(bot: Bot, admin_id, target, chat_id, msg_id):
    BC.update(stop=False, running=True)
    ok = fail = 0
    try:
        rows = await pool.fetch(
            f"SELECT id FROM users WHERE NOT is_banned AND NOT bot_blocked AND {BC_TARGETS[target][1]}")
        total = len(rows)
        status = await bot.send_message(admin_id, f"📤 Yuborilmoqda: 0/{total}",
                                        reply_markup=IK([B("⛔ To'xtatish", "bc:stop")]))
        for i, r in enumerate(rows, 1):
            if BC["stop"]:
                break
            for _ in range(3):
                try:
                    await bot.copy_message(r["id"], chat_id, msg_id)
                    ok += 1
                    break
                except TelegramRetryAfter as e:
                    await asyncio.sleep(e.retry_after)
                except TelegramForbiddenError:
                    await pool.execute("UPDATE users SET bot_blocked=TRUE WHERE id=$1", r["id"])
                    fail += 1
                    break
                except Exception:
                    fail += 1
                    break
            if i % 25 == 0:
                try:
                    await status.edit_text(f"📤 Yuborilmoqda: {i}/{total}",
                                           reply_markup=IK([B("⛔ To'xtatish", "bc:stop")]))
                except Exception:
                    pass
            await asyncio.sleep(0.05)
        stopped = "\n⛔ To'xtatildi." if BC["stop"] else ""
        await bot.send_message(admin_id, f"✅ Xabar yuborish tugadi.{stopped}\n"
                                         f"Yetkazildi: {ok}\nYetmadi: {fail}")
    except Exception:
        log.exception("Broadcast xatosi")
    finally:
        BC["running"] = False


# ---- foydalanuvchini boshqarish
def user_kb(u):
    uid = u["id"]
    return IK(
        [B("✅ Blokdan chiqarish" if u["is_banned"] else "🚫 Bloklash",
           f"au:{'unban' if u['is_banned'] else 'ban'}:{uid}"),
         B("💬 Xabar yozish", f"au:dm:{uid}")],
        [B("👑 +7", f"au:vip7:{uid}"), B("👑 +30", f"au:vip30:{uid}"), B("👑 +90", f"au:vip90:{uid}")],
        [B("❎ VIP ni olish", f"au:vipoff:{uid}"), B("❤️ +20 layk", f"au:bonus:{uid}")],
        [B("🗑 O'chirish", f"au:del:{uid}")])


def admin_card(u):
    vip = f"✅ {u['vip_until']:%d.%m.%Y} gacha" if is_vip(u) else "❌"
    return (
        f"🆔 <code>{u['id']}</code>  @{esc(u['username'] or '—')}\n\n"
        f"{caption(u)}\n\n"
        f"👑 VIP: {vip}\n"
        f"🚫 Bloklangan: {'ha' if u['is_banned'] else 'yoq'}\n"
        f"👁 Ko'rinadi: {'ha' if u['is_active'] else 'yoq'}\n"
        f"🤖 Botni bloklagan: {'ha' if u['bot_blocked'] else 'yoq'}\n"
        f"🎁 Bonus layk: {u['bonus_likes'] or 0}   Taklif qilgan: {u['ref_by'] or '—'}\n"
        f"📅 Qo'shilgan: {u['created_at']:%d.%m.%Y}   So'nggi: {u['last_seen']:%d.%m.%Y}")


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


@admin.callback_query(F.data.regexp(r"^au:(ban|unban|vip7|vip30|vip90|vipoff|bonus|dm|del|delok|cancel):\d+$"))
async def adm_user_action(c: CallbackQuery, bot: Bot, state: FSMContext):
    _, act, uid = c.data.split(":")
    uid = int(uid)
    if act == "dm":
        await c.answer()
        await state.set_state(AdmDm.text)
        await state.update_data(to=uid)
        return await c.message.answer(f"💬 {uid} ga yuboriladigan xabar matnini yozing:")
    if act == "del":
        await c.answer()
        return await c.message.edit_reply_markup(reply_markup=IK(
            [B("✅ Ha, o'chirish", f"au:delok:{uid}"), B("❌ Yo'q", f"au:cancel:{uid}")]))
    if act == "delok":
        await delete_user_data(uid)
        await alog(c.from_user.id, "delete", uid)
        await c.answer("O'chirildi")
        return await c.message.edit_caption(caption="🗑 Foydalanuvchi o'chirildi.")
    if act == "ban":
        await pool.execute("UPDATE users SET is_banned=TRUE WHERE id=$1", uid)
    elif act == "unban":
        await pool.execute("UPDATE users SET is_banned=FALSE WHERE id=$1", uid)
    elif act in ("vip7", "vip30", "vip90"):
        days = int(act[3:])
        await grant_vip(uid, days)
        await send_safe(bot, uid, f"👑 Tabriklaymiz! Sizga {days} kunlik VIP berildi.")
    elif act == "vipoff":
        await pool.execute("UPDATE users SET vip_until=NULL WHERE id=$1", uid)
    elif act == "bonus":
        await pool.execute("UPDATE users SET bonus_likes=bonus_likes+20 WHERE id=$1", uid)
        await send_safe(bot, uid, "🎁 Sizga +20 bonus layk berildi!")
    if act != "cancel":
        await alog(c.from_user.id, act, uid)
    await c.answer("Bajarildi ✅")
    u = await get_user(uid)
    if u:
        try:
            await c.message.edit_caption(caption=admin_card(u), reply_markup=user_kb(u))
        except TelegramBadRequest:
            pass


@admin.message(AdmDm.text, F.text)
async def adm_dm_send(m: Message, state: FSMContext, bot: Bot):
    to = (await state.get_data())["to"]
    await state.clear()
    ok = await send_safe(bot, to, f"📩 <b>Admin xabari:</b>\n\n{esc(m.text)}")
    await alog(m.from_user.id, "dm", to)
    await m.answer("✅ Yuborildi." if ok else "❌ Yuborib bo'lmadi.")


# ---- shikoyatlar
async def show_report(bot: Bot, chat_id):
    r = await pool.fetchrow("SELECT * FROM reports WHERE NOT handled ORDER BY id LIMIT 1")
    if not r:
        return await bot.send_message(chat_id, "✅ Yangi shikoyatlar yo'q.")
    u = await get_user(r["to_id"])
    if not u:
        await pool.execute("UPDATE reports SET handled=TRUE WHERE to_id=$1", r["to_id"])
        return await show_report(bot, chat_id)
    cnt = await pool.fetchval("SELECT count(*) FROM reports WHERE to_id=$1 AND NOT handled", u["id"])
    kb = IK([B("🚫 Bloklash", f"ar:ban:{u['id']}"), B("🙈 Yashirish", f"ar:hide:{u['id']}")],
            [B("✅ Rad etish", f"ar:ok:{u['id']}")])
    await bot.send_photo(
        chat_id, u["photo"],
        caption=(f"🚩 <b>Shikoyat</b> (shu odamga jami {cnt} ta)\n"
                 f"Sabab: {esc(r['reason'] or '—')}\n"
                 f"Shikoyatchi: <code>{r['from_id']}</code>\n\n" + admin_card(u)),
        reply_markup=kb)


@admin.callback_query(F.data == "adm:rep")
async def adm_rep(c: CallbackQuery, bot: Bot):
    await c.answer()
    await show_report(bot, c.message.chat.id)


@admin.callback_query(F.data.regexp(r"^ar:(ban|hide|ok):\d+$"))
async def adm_rep_action(c: CallbackQuery, bot: Bot):
    _, act, uid = c.data.split(":")
    uid = int(uid)
    if act == "ban":
        await pool.execute("UPDATE users SET is_banned=TRUE WHERE id=$1", uid)
    elif act == "hide":
        await pool.execute("UPDATE users SET is_active=FALSE WHERE id=$1", uid)
    elif act == "ok":
        await pool.execute("UPDATE users SET is_active=TRUE WHERE id=$1 AND NOT is_banned", uid)
    await pool.execute("UPDATE reports SET handled=TRUE WHERE to_id=$1", uid)
    await alog(c.from_user.id, f"report:{act}", uid)
    await c.answer("Bajarildi ✅")
    try:
        await c.message.delete()
    except Exception:
        pass
    await show_report(bot, c.message.chat.id)


# ---- promokodlar
async def promo_list_text():
    rows = await pool.fetch("SELECT * FROM promo ORDER BY created_at DESC LIMIT 20")
    if not rows:
        return "🎟 Promokodlar yo'q.", IK([B("➕ Yangi promokod", "pm:new")])
    lines = [f"<code>{r['code']}</code> - {r['days']} kun, ishlatilgan {r['used']}/"
             f"{r['max_uses'] or '∞'}" for r in rows]
    kb = [[B(f"🗑 {r['code']}", f"pm:del:{r['code']}")] for r in rows[:10]]
    kb.append([B("➕ Yangi promokod", "pm:new")])
    return "🎟 <b>Promokodlar</b>\n\n" + "\n".join(lines), IK(*kb)


@admin.callback_query(F.data == "adm:promo")
async def adm_promo(c: CallbackQuery):
    await c.answer()
    text, kb = await promo_list_text()
    await c.message.answer(text, reply_markup=kb)


@admin.callback_query(F.data == "pm:new")
async def pm_new(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.set_state(AdmPromo.new)
    await c.message.answer(
        "Format: <code>KUN LIMIT [KOD]</code>\n"
        "Masalan: <code>30 100 YANGIYIL</code>\n"
        "LIMIT 0 = cheksiz. KOD yozilmasa avtomatik yaratiladi.")


@admin.message(AdmPromo.new, F.text)
async def pm_create(m: Message, state: FSMContext):
    p = m.text.split()
    if len(p) < 2 or not p[0].isdigit() or not p[1].isdigit() or int(p[0]) < 1:
        return await m.answer("Format noto'g'ri. Masalan: <code>30 100 YANGIYIL</code>")
    code = p[2].upper() if len(p) > 2 else "VIP" + secrets.token_hex(3).upper()
    if not re.match(r"^[A-Z0-9_-]{3,20}$", code):
        return await m.answer("Kod faqat A-Z, 0-9, _ va - bo'lsin (3-20 belgi).")
    try:
        await pool.execute("INSERT INTO promo(code,days,max_uses) VALUES($1,$2,$3)",
                           code, int(p[0]), int(p[1]))
    except asyncpg.UniqueViolationError:
        return await m.answer("❌ Bunday kod allaqachon bor.")
    await state.clear()
    await alog(m.from_user.id, f"promo:{code}")
    await m.answer(f"✅ Yaratildi: <code>{code}</code> - {p[0]} kun, limit {p[1]}")


@admin.callback_query(F.data.regexp(r"^pm:del:[A-Z0-9_-]+$"))
async def pm_del(c: CallbackQuery):
    code = c.data.split(":")[2]
    await pool.execute("DELETE FROM promo WHERE code=$1", code)
    await alog(c.from_user.id, f"promo_del:{code}")
    await c.answer("O'chirildi")
    text, kb = await promo_list_text()
    try:
        await c.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        pass


# ---- bot sozlamalari
CFG_META = {
    "daily_likes": ("❤️ Kunlik layk", 5, 0),
    "super_daily": ("💫 Super layk (VIP)", 1, 0),
    "ref_bonus": ("🎁 Referal bonus", 1, 0),
    "report_limit": ("🚩 Avto-yashirish", 1, 1),
}


def cfg_kb():
    rows = []
    for k, (label, step, _) in CFG_META.items():
        rows.append([B("➖", f"cfg:{k}:-{step}"), B(f"{label}: {cfg(k)}", "noop"),
                     B("➕", f"cfg:{k}:{step}")])
    rows.append([B(f"🛠 Texnik rejim: {'YOQIQ' if cfg('maintenance') else 'o‘chiq'}", "cfg:maintenance:t")])
    return IK(*rows)


@admin.callback_query(F.data == "adm:cfg")
async def adm_cfg(c: CallbackQuery):
    await c.answer()
    await c.message.answer("⚙️ <b>Bot sozlamalari</b>", reply_markup=cfg_kb())


@admin.callback_query(F.data.regexp(r"^cfg:\w+:(-?\d+|t)$"))
async def adm_cfg_change(c: CallbackQuery):
    _, key, val = c.data.split(":")
    if key == "maintenance":
        await set_cfg(key, 0 if cfg(key) else 1)
    elif key in CFG_META:
        await set_cfg(key, max(CFG_META[key][2], cfg(key) + int(val)))
    else:
        return await c.answer()
    await alog(c.from_user.id, f"cfg:{key}={cfg(key)}")
    await c.answer("Saqlandi ✅")
    try:
        await c.message.edit_reply_markup(reply_markup=cfg_kb())
    except TelegramBadRequest:
        pass


# ---- loglar
@admin.callback_query(F.data == "adm:log")
async def adm_log(c: CallbackQuery):
    await c.answer()
    rows = await pool.fetch("SELECT * FROM admin_log ORDER BY id DESC LIMIT 15")
    if not rows:
        return await c.message.answer("🧾 Loglar bo'sh.")
    lines = [f"<code>{r['created_at']:%d.%m %H:%M}</code> {r['admin_id']}: {esc(r['action'])}"
             f"{' → ' + str(r['target']) if r['target'] else ''}" for r in rows]
    await c.message.answer("🧾 <b>So'nggi amallar</b>\n\n" + "\n".join(lines))


# ---- CSV eksport
@admin.callback_query(F.data == "adm:csv")
async def adm_csv(c: CallbackQuery):
    await c.answer()
    rows = await pool.fetch("SELECT * FROM users ORDER BY created_at")
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["id", "username", "name", "gender", "seeking", "age", "city", "interests", "bio",
                "vip_until", "banned", "active", "bot_blocked", "ref_by", "created_at", "last_seen"])
    for u in rows:
        w.writerow([u["id"], u["username"], u["name"], u["gender"], u["seeking"], u["age"], u["city"],
                    u["interests"], u["bio"], u["vip_until"], u["is_banned"], u["is_active"],
                    u["bot_blocked"], u["ref_by"], u["created_at"], u["last_seen"]])
    data = buf.getvalue().encode("utf-8-sig")
    await alog(c.from_user.id, "csv_export")
    await c.message.answer_document(
        BufferedInputFile(data, filename=f"users_{datetime.now():%Y%m%d}.csv"),
        caption=f"👥 Jami: {len(rows)}")


# ====================== FALLBACK (eng oxirida) ======================
@router.message(StateFilter(None))
async def fallback(m: Message):
    if await get_user(m.from_user.id):
        await m.answer("Menyudan tanlang 👇", reply_markup=main_menu(m.from_user.id))
    else:
        await m.answer("Boshlash uchun /start bosing.")


# ====================== FON VAZIFALARI ======================
async def background(bot: Bot):
    """VIP tugashi haqida xabar, layklar haqida eslatma, eski ma'lumotlarni tozalash."""
    await asyncio.sleep(30)
    while True:
        try:
            expired = await pool.fetch(
                "UPDATE users SET vip_notified=TRUE WHERE vip_until IS NOT NULL AND vip_until<=now() "
                "AND NOT COALESCE(vip_notified, FALSE) AND NOT bot_blocked RETURNING id")
            for r in expired:
                await send_safe(bot, r["id"], "👑 VIP muddatingiz tugadi. Yangilash uchun 👑 VIP PREMIUM bo'limiga kiring.")
                await asyncio.sleep(0.05)

            pending = await pool.fetch(
                """
                SELECT u.id, count(*) n FROM users u
                JOIN reactions r ON r.to_id=u.id AND r.value>=1
                WHERE u.last_seen < now() - interval '2 days'
                  AND COALESCE(u.notify, TRUE) AND NOT u.bot_blocked AND NOT u.is_banned
                  AND (u.reminded_at IS NULL OR u.reminded_at < now() - interval '3 days')
                  AND NOT EXISTS (SELECT 1 FROM reactions q WHERE q.from_id=u.id AND q.to_id=r.from_id)
                GROUP BY u.id LIMIT 200
                """)
            for r in pending:
                await pool.execute("UPDATE users SET reminded_at=now() WHERE id=$1", r["id"])
                await send_safe(bot, r["id"], f"💌 Sizni {r['n']} kishi yoqtirgan! Ko'rish uchun botga kiring.")
                await asyncio.sleep(0.05)

            await pool.execute("DELETE FROM daily_likes WHERE day < CURRENT_DATE - 7")
        except Exception:
            log.exception("background xatosi")
        await asyncio.sleep(3600)


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
    await load_cfg()

    bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    guard = Guard()
    dp.message.outer_middleware(guard)
    dp.callback_query.outer_middleware(guard)
    dp.errors.register(on_error)
    dp.include_router(admin)  # admin birinchi
    dp.include_router(router)

    # Render Web Service port talab qiladi
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()

    tasks = [asyncio.create_task(keep_alive()), asyncio.create_task(background(bot))]
    await bot.set_my_commands([
        BotCommand(command="start", description="Botni boshlash"),
        BotCommand(command="menu", description="Asosiy menyu"),
        BotCommand(command="cancel", description="Amalni bekor qilish"),
        BotCommand(command="help", description="Yordam"),
    ])
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        for t in tasks:
            t.cancel()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
