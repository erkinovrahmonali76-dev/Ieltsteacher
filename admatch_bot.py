"""
AdMatch Bot - Telegram tanishuv boti
O'rnatish:  pip install aiogram
Ishga tushirish:  BOT_TOKEN=123:ABC python admatch_bot.py
"""
import asyncio
import logging
import os
import sqlite3

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, Message, ReplyKeyboardMarkup, ReplyKeyboardRemove,
)

TOKEN = os.getenv("BOT_TOKEN", "9177446332:AAGy4J6bHjEIr7GS-gnAUFvJkKlOjboWRn4")
DB_PATH = "admatch.db"

router = Router()

# ---------------------- DATABASE ----------------------
db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row
db.executescript("""
CREATE TABLE IF NOT EXISTS users(
    id INTEGER PRIMARY KEY,
    username TEXT,
    name TEXT, gender TEXT, age INTEGER, city TEXT,
    photo TEXT, interests TEXT
);
CREATE TABLE IF NOT EXISTS reactions(
    from_id INTEGER, to_id INTEGER, value INTEGER,
    PRIMARY KEY(from_id, to_id)
);
""")
db.commit()


def get_user(uid):
    return db.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()


def save_user(uid, username, d):
    db.execute(
        "INSERT OR REPLACE INTO users(id,username,name,gender,age,city,photo,interests) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (uid, username, d["name"], d["gender"], d["age"], d["city"], d["photo"], d["interests"]),
    )
    db.commit()


def next_profile(uid, gender):
    """Qarama-qarshi jinsdagi, hali ko'rilmagan anketa."""
    return db.execute(
        "SELECT * FROM users WHERE gender!=? AND id!=? "
        "AND id NOT IN (SELECT to_id FROM reactions WHERE from_id=?) "
        "ORDER BY RANDOM() LIMIT 1",
        (gender, uid, uid),
    ).fetchone()


def next_liker(uid):
    """Menga layk bosgan, men hali javob bermagan odam."""
    return db.execute(
        "SELECT u.* FROM reactions r JOIN users u ON u.id=r.from_id "
        "WHERE r.to_id=? AND r.value=1 "
        "AND r.from_id NOT IN (SELECT to_id FROM reactions WHERE from_id=?) LIMIT 1",
        (uid, uid),
    ).fetchone()


def set_reaction(a, b, v):
    db.execute("INSERT OR REPLACE INTO reactions VALUES(?,?,?)", (a, b, v))
    db.commit()


def liked_me(a, b):
    """b meni (a ni) laykladimi?"""
    r = db.execute(
        "SELECT 1 FROM reactions WHERE from_id=? AND to_id=? AND value=1", (b, a)
    ).fetchone()
    return bool(r)


# ---------------------- KEYBOARDS ----------------------
def main_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔍 Qidirish")],
            [KeyboardButton(text="💌 Menga kim layk bosdi")],
            [KeyboardButton(text="👤 Mening profilim")],
        ],
        resize_keyboard=True,
    )


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
        [InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back")],
    ])


def caption(u):
    icon = "👨" if u["gender"] == "Yigit" else "👩"
    return (
        f"<b>{u['name']}</b>, {u['age']}, {u['city']}, {icon} {u['gender']}\n\n"
        f"🎯 <b>Qiziqishlar:</b> {u['interests']}"
    )


# ---------------------- REGISTRATION ----------------------
class Reg(StatesGroup):
    name = State()
    gender = State()
    age = State()
    city = State()
    photo = State()
    interests = State()


@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(
        "👋 <b>AdMatch Bot</b>ga xush kelibsiz!\n\n"
        "Bu yerda yangi insonlar bilan tanishishingiz mumkin.\n\n"
        "❤️ Layk bosing — agar u ham yoqtirsa, match bo'ladi\n"
        "👎 Yoqmasa — o'tkazib yuboring",
        parse_mode="HTML",
    )
    if get_user(m.from_user.id):
        await m.answer("Menyudan tanlang 👇", reply_markup=main_menu())
    else:
        await begin_reg(m, state)


async def begin_reg(m: Message, state: FSMContext):
    await m.answer("Anketa to'ldiramiz.\n\n✏️ Ismingiz?", reply_markup=ReplyKeyboardRemove())
    await state.set_state(Reg.name)


@router.message(Reg.name, F.text)
async def reg_name(m: Message, state: FSMContext):
    await state.update_data(name=m.text.strip()[:30])
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
    if not m.text.isdigit() or not (16 <= int(m.text) <= 70):
        return await m.answer("Yoshni raqam bilan yozing (16-70).")
    await state.update_data(age=int(m.text))
    await m.answer("Qaysi shahardansiz?")
    await state.set_state(Reg.city)


@router.message(Reg.city, F.text)
async def reg_city(m: Message, state: FSMContext):
    await state.update_data(city=m.text.strip()[:30])
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
    data = await state.get_data()
    data["interests"] = m.text.strip()[:100]
    save_user(m.from_user.id, m.from_user.username, data)
    await state.clear()
    await m.answer("✅ Anketa saqlandi!", reply_markup=main_menu())
    await show_profile(m)


# ---------------------- PROFILE ----------------------
async def show_profile(m: Message):
    u = get_user(m.from_user.id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Qayta to'ldirish", callback_data="redo")]
    ])
    await m.answer_photo(u["photo"], caption=caption(u), parse_mode="HTML", reply_markup=kb)


@router.message(F.text == "👤 Mening profilim")
async def my_profile(m: Message):
    if not get_user(m.from_user.id):
        return await m.answer("Avval /start bosing.")
    await show_profile(m)


@router.callback_query(F.data == "redo")
async def redo(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await begin_reg(c.message, state)


# ---------------------- SEARCH ----------------------
async def send_next(bot: Bot, chat_id: int, uid: int):
    me = get_user(uid)
    p = next_profile(uid, me["gender"])
    if not p:
        return await bot.send_message(
            chat_id, "😔 Hozircha yangi anketalar yo'q. Keyinroq urinib ko'ring.",
            reply_markup=main_menu(),
        )
    await bot.send_photo(
        chat_id, p["photo"], caption=caption(p), parse_mode="HTML",
        reply_markup=react_kb(p["id"], "s"),
    )


@router.message(F.text == "🔍 Qidirish")
async def search(m: Message, bot: Bot):
    if not get_user(m.from_user.id):
        return await m.answer("Avval /start bosib anketa to'ldiring.")
    await send_next(bot, m.chat.id, m.from_user.id)


@router.callback_query(F.data.startswith(("lk:", "ds:")))
async def react(c: CallbackQuery, bot: Bot):
    action, tid, src = c.data.split(":")
    tid = int(tid)
    uid = c.from_user.id
    me = get_user(uid)
    if not me:
        return await c.answer("Avval /start bosing", show_alert=True)

    try:
        await c.message.delete()
    except Exception:
        pass

    if action == "ds":
        set_reaction(uid, tid, 0)  # yoqmasa hech narsa yuborilmaydi
        await c.answer("O'tkazildi")
    else:
        set_reaction(uid, tid, 1)
        await c.answer("❤️")
        target = get_user(tid)
        if target:
            if liked_me(uid, tid):
                # o'zaro layk - MATCH
                await bot.send_message(
                    uid, f"🎉 <b>Match!</b> Siz va <a href='tg://user?id={tid}'>{target['name']}</a> "
                         f"bir-biringizni yoqtirdingiz.\nYozishingiz mumkin 💬"
                         + (f"\n@{target['username']}" if target["username"] else ""),
                    parse_mode="HTML",
                )
                try:
                    await bot.send_message(
                        tid, f"🎉 <b>Match!</b> Siz va <a href='tg://user?id={uid}'>{me['name']}</a> "
                             f"bir-biringizni yoqtirdingiz.\nYozishingiz mumkin 💬"
                             + (f"\n@{me['username']}" if me["username"] else ""),
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
            else:
                # faqat birinchi layk bosilganda xabar boradi
                try:
                    await bot.send_message(
                        tid, "❤️ Sizga bir kishi layk bosdi!\n"
                             "Ko'rish uchun <b>💌 Menga kim layk bosdi</b> tugmasini bosing.",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass

    if src == "s":
        await send_next(bot, c.message.chat.id, uid)
    elif src == "l":
        await show_liker(bot, c.message.chat.id, uid)


# ---------------------- WHO LIKED ME ----------------------
async def show_liker(bot: Bot, chat_id: int, uid: int):
    p = next_liker(uid)
    if not p:
        return await bot.send_message(chat_id, "Hozircha yangi layklar yo'q.", reply_markup=main_menu())
    await bot.send_photo(
        chat_id, p["photo"],
        caption="❤️ <b>Sizga shu odam layk bosdi:</b>\n\n" + caption(p),
        parse_mode="HTML", reply_markup=react_kb(p["id"], "l"),
    )


@router.message(F.text == "💌 Menga kim layk bosdi")
async def who_liked(m: Message, bot: Bot):
    if not get_user(m.from_user.id):
        return await m.answer("Avval /start bosing.")
    await show_liker(bot, m.chat.id, m.from_user.id)


@router.callback_query(F.data == "back")
async def back(c: CallbackQuery):
    await c.answer()
    try:
        await c.message.delete()
    except Exception:
        pass
    await c.message.answer("Menyudan tanlang 👇", reply_markup=main_menu())


# ---------------------- RUN ----------------------
async def main():
    logging.basicConfig(level=logging.INFO)
    bot = Bot(TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
