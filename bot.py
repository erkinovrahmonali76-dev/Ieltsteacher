import asyncio, base64, hashlib, hmac, json, os, urllib.parse
from datetime import date, timedelta

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import (BufferedInputFile, CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, MenuButtonWebApp, Message, WebAppInfo)
from aiohttp import web
from google import genai
from google.genai import types

BOT_TOKEN = os.environ["BOT_TOKEN"]
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
DATABASE_URL = os.environ["DATABASE_URL"]
WEBAPP_URL = os.environ.get("WEBAPP_URL", "").rstrip("/")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
PORT = int(os.environ.get("PORT", 10000))
FREE_LIMIT = 5
PRO_DAYS = 30
PRICE = "40 000 so'm"
CARD = "6262 5701 3419 2691"
HOLDER = "Boratova Zarnigor"

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
ai = genai.Client(api_key=GEMINI_API_KEY)
pool: asyncpg.Pool = None

PROMPTS = {
    "writing": (
        "Sen tajribali IELTS Writing examinersan. Foydalanuvchi vazifa (Task) va inglizcha matn yuboradi. "
        "Javobni O'ZBEK tilida, shu tartibda ber:\n"
        "**Taxminiy band:** X.X\n"
        "**Mezonlar:** Task Response, Coherence, Lexical Resource, Grammar (har biriga band va 1 gap)\n"
        "**Asosiy xatolar:** xato → to'g'risi (3-5 ta)\n"
        "**Yaxshilangan paragraf:** (inglizcha)\n"
        "**Maslahat:** 2-3 ta qisqa.\nQisqa va aniq yoz."
    ),
    "speaking": (
        "Sen tajribali IELTS Speaking examinersan. Foydalanuvchi savol va o'z javobini yuboradi. "
        "Javobni O'ZBEK tilida, shu tartibda ber:\n"
        "**Taxminiy band:** X.X\n"
        "**Mezonlar:** Fluency, Vocabulary, Grammar, Pronunciation(matn bo'yicha taxmin)\n"
        "**Xatolar:** xato → to'g'risi\n"
        "**Namunaviy javob:** (inglizcha, band 7 darajasida, qisqa)\n"
        "**Maslahat:** 2 ta.\nQisqa yoz."
    ),
}


async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, statement_cache_size=0)
    await pool.execute("CREATE TABLE IF NOT EXISTS usage (user_id BIGINT, day DATE, count INT, PRIMARY KEY (user_id, day))")
    await pool.execute("CREATE TABLE IF NOT EXISTS pro (user_id BIGINT PRIMARY KEY, until DATE)")


async def pro_until(uid):
    return await pool.fetchval("SELECT until FROM pro WHERE user_id=$1 AND until>=$2", uid, date.today())


async def grant(uid, days=PRO_DAYS):
    cur = await pro_until(uid)
    new = max(cur or date.today(), date.today()) + timedelta(days=days)
    await pool.execute("INSERT INTO pro (user_id, until) VALUES ($1,$2) ON CONFLICT (user_id) DO UPDATE SET until=$2", uid, new)
    return new


async def used_today(uid):
    return await pool.fetchval("SELECT count FROM usage WHERE user_id=$1 AND day=$2", uid, date.today()) or 0


async def add_usage(uid):
    await pool.execute("INSERT INTO usage (user_id, day, count) VALUES ($1,$2,1) ON CONFLICT (user_id, day) DO UPDATE SET count = usage.count + 1", uid, date.today())


async def ask_ai(system, text):
    r = await ai.aio.models.generate_content(model=GEMINI_MODEL, contents=text,
                                             config=types.GenerateContentConfig(system_instruction=system))
    return r.text[:4000]


def pay_text():
    return (f"💎 <b>Pro obuna: {PRICE} / {PRO_DAYS} kun</b>\n\n"
            "✅ Cheksiz AI tekshiruv\n✅ Barcha Reading va Listening testlar\n\n"
            f"💳 Karta: <code>{CARD}</code>\n👤 {HOLDER}\n\n"
            "To'lov qilgach, <b>chek rasmini shu botga yuboring</b>. Admin tasdiqlagach Pro yoqiladi.")


def app_kb(label="📚 Ilovani ochish", page="/app"):
    if not WEBAPP_URL:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=label, web_app=WebAppInfo(url=WEBAPP_URL + page))]])


async def send_receipt(uid, name, username, photo):
    if not ADMIN_ID:
        return False
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Tasdiqlash", callback_data=f"ok:{uid}"),
        InlineKeyboardButton(text="❌ Rad etish", callback_data=f"no:{uid}")]])
    cap = f"💳 To'lov cheki\n👤 {name} {('@' + username) if username else ''}\n🆔 {uid}\n💰 {PRICE}"
    await bot.send_photo(ADMIN_ID, photo, caption=cap, reply_markup=kb)
    return True


# ---------- bot ----------
@dp.message(CommandStart())
async def start(m: Message):
    await m.answer(
        "Salom! Men IELTS AI ustozman 🎓\n\nIlovada Writing, Speaking, Reading va Listening mashqlari bor. "
        f"Bepul: kuniga {FREE_LIMIT} ta AI tekshiruv. Cheksiz foydalanish uchun /pro", reply_markup=app_kb())


@dp.message(F.text == "/pro")
async def pro_cmd(m: Message):
    await m.answer(pay_text(), parse_mode="HTML", reply_markup=app_kb("💎 Ilovada to'lash", "/app#pro"))


@dp.message(F.photo)
async def photo(m: Message):
    ok = await send_receipt(m.from_user.id, m.from_user.full_name, m.from_user.username, m.photo[-1].file_id)
    await m.answer("✅ Chek adminga yuborildi. Tasdiqlangach xabar keladi." if ok else "Admin hali sozlanmagan, keyinroq urinib ko'ring.")


@dp.callback_query(F.data.regexp(r"^(ok|no):\d+$"))
async def decide(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID:
        return await c.answer("Ruxsat yo'q", show_alert=True)
    act, uid = c.data.split(":")
    uid = int(uid)
    if act == "ok":
        until = await grant(uid)
        msg, note = f"🎉 Pro faollashdi! {until:%d.%m.%Y} gacha cheksiz foydalaning.", "✅ Tasdiqlandi"
    else:
        msg, note = "❌ To'lov tasdiqlanmadi. Chekni tekshirib, qayta yuboring.", "❌ Rad etildi"
    try:
        await bot.send_message(uid, msg)
    except Exception:
        pass
    await c.message.edit_caption(caption=(c.message.caption or "") + f"\n\n{note}", reply_markup=None)
    await c.answer()


@dp.message(F.text)
async def check(m: Message):
    uid = m.from_user.id
    if not await pro_until(uid) and await used_today(uid) >= FREE_LIMIT:
        return await m.answer(pay_text(), parse_mode="HTML", reply_markup=app_kb("💎 Ilovada to'lash", "/app#pro"))
    if len(m.text) < 50:
        return await m.answer("Matn juda qisqa. Kamida 2-3 ta gap yuboring.")
    wait = await m.answer("Tekshiryapman... ⏳")
    try:
        await wait.edit_text((await ask_ai(PROMPTS["writing"], m.text)).replace("**", ""))
        await add_usage(uid)
    except Exception as e:
        print("Xato:", e)
        await wait.edit_text("Xatolik yuz berdi, keyinroq urinib ko'ring.")


# ---------- mini app api ----------
def verify(init_data):
    try:
        p = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True))
        got = p.pop("hash")
        s = "\n".join(f"{k}={v}" for k, v in sorted(p.items()))
        key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(hmac.new(key, s.encode(), hashlib.sha256).hexdigest(), got):
            return None
        return json.loads(p["user"])
    except Exception:
        return None


async def auth(request):
    try:
        d = await request.json()
    except Exception:
        return None, None
    return verify(d.get("initData", "")), d


def bad(err, status=400):
    return web.json_response({"ok": False, "error": err}, status=status)


async def api_me(request):
    u, _ = await auth(request)
    if not u:
        return bad("auth", 401)
    until = await pro_until(u["id"])
    used = await used_today(u["id"])
    return web.json_response({"ok": True, "name": u.get("first_name", ""), "pro": bool(until),
                              "until": until.strftime("%d.%m.%Y") if until else None,
                              "left": max(0, FREE_LIMIT - used), "limit": FREE_LIMIT,
                              "price": PRICE, "card": CARD, "holder": HOLDER})


async def api_ai(request):
    u, d = await auth(request)
    if not u:
        return bad("auth", 401)
    mode, text = d.get("mode"), (d.get("text") or "")[:3500]
    if mode not in PROMPTS or len(text) < 10:
        return bad("bad")
    uid = u["id"]
    pro = await pro_until(uid)
    if not pro and await used_today(uid) >= FREE_LIMIT:
        return bad("limit", 200)
    try:
        out = await ask_ai(PROMPTS[mode], text)
        await add_usage(uid)
        left = None if pro else max(0, FREE_LIMIT - await used_today(uid))
        return web.json_response({"ok": True, "text": out, "left": left})
    except Exception as e:
        print("Xato:", e)
        return bad("ai", 200)


async def api_pay(request):
    u, d = await auth(request)
    if not u:
        return bad("auth", 401)
    try:
        img = base64.b64decode((d.get("image") or "").split(",")[-1])
        ok = await send_receipt(u["id"], u.get("first_name", ""), u.get("username"), BufferedInputFile(img, "check.jpg"))
    except Exception as e:
        print("Chek xatosi:", e)
        return bad("pay", 200)
    return web.json_response({"ok": ok}) if ok else bad("noadmin", 200)


async def app_page(request):
    return web.FileResponse(os.path.join(os.path.dirname(__file__), "webapp", "index.html"))


async def health(request):
    return web.Response(text="OK")


async def start_web():
    app = web.Application(client_max_size=6 * 1024 * 1024)
    app.add_routes([web.get("/", health), web.get("/app", app_page), web.post("/api/me", api_me),
                    web.post("/api/ai", api_ai), web.post("/api/pay", api_pay)])
    r = web.AppRunner(app)
    await r.setup()
    await web.TCPSite(r, "0.0.0.0", PORT).start()


async def main():
    await init_db()
    await start_web()
    if WEBAPP_URL:
        try:
            await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text="Ilova", web_app=WebAppInfo(url=WEBAPP_URL + "/app")))
        except Exception as e:
            print("Menu tugmasi:", e)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
