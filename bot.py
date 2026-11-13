import asyncio
import hashlib
import hmac
import json
import os
import urllib.parse
from datetime import date

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    WebAppInfo,
)
from aiohttp import web
from google import genai
from google.genai import types

BOT_TOKEN = os.environ["BOT_TOKEN"]
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
DATABASE_URL = os.environ["DATABASE_URL"]
WEBAPP_URL = os.environ.get("WEBAPP_URL", "").rstrip("/")  # masalan https://ielts-bot.onrender.com
PORT = int(os.environ.get("PORT", 10000))
FREE_LIMIT = 3

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
ai = genai.Client(api_key=GEMINI_API_KEY)
pool: asyncpg.Pool = None

PROMPTS = {
    "writing": (
        "Sen IELTS Writing ustozisan. Foydalanuvchi inglizcha matn yuboradi. "
        "Javobni O'ZBEK tilida ber:\n1) Taxminiy band\n2) Asosiy xatolar va to'g'rilangani\n"
        "3) Yaxshilangan variant (inglizcha)\n4) 2-3 ta qisqa maslahat.\nQisqa yoz."
    ),
    "speaking": (
        "Sen IELTS Speaking ustozisan. Foydalanuvchi savol va o'z javobini yuboradi. "
        "Javobni O'ZBEK tilida ber:\n1) Taxminiy band (Fluency, Vocabulary, Grammar)\n"
        "2) Xatolar va to'g'rilangani\n3) Namunaviy yaxshi javob (inglizcha, qisqa)\nQisqa yoz."
    ),
}


async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, statement_cache_size=0)
    await pool.execute(
        "CREATE TABLE IF NOT EXISTS usage ("
        "user_id BIGINT, day DATE, count INT, PRIMARY KEY (user_id, day))"
    )
    await pool.execute("CREATE TABLE IF NOT EXISTS premium (user_id BIGINT PRIMARY KEY)")


async def is_premium(uid: int) -> bool:
    return await pool.fetchval("SELECT 1 FROM premium WHERE user_id=$1", uid) is not None


async def used_today(uid: int) -> int:
    n = await pool.fetchval(
        "SELECT count FROM usage WHERE user_id=$1 AND day=$2", uid, date.today()
    )
    return n or 0


async def add_usage(uid: int):
    await pool.execute(
        "INSERT INTO usage (user_id, day, count) VALUES ($1, $2, 1) "
        "ON CONFLICT (user_id, day) DO UPDATE SET count = usage.count + 1",
        uid,
        date.today(),
    )


async def ask_ai(system: str, text: str) -> str:
    resp = await ai.aio.models.generate_content(
        model=GEMINI_MODEL,
        contents=text,
        config=types.GenerateContentConfig(system_instruction=system),
    )
    return resp.text[:4000]


# ---------- Telegram bot ----------
@dp.message(CommandStart())
async def start(m: Message):
    kb = None
    if WEBAPP_URL:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📚 Ilovani ochish", web_app=WebAppInfo(url=f"{WEBAPP_URL}/app"))]
            ]
        )
    await m.answer(
        "Salom! Men IELTS AI ustozman 🎓\n\n"
        "Ilovada Writing, Speaking, Reading va Listening mashqlari bor.\n"
        "Yoki shu yerning o'ziga inglizcha matn yuboring, men tekshirib beraman.\n"
        f"Bepul: kuniga {FREE_LIMIT} ta AI tekshiruv.",
        reply_markup=kb,
    )


@dp.message(F.text)
async def check(m: Message):
    uid = m.from_user.id
    if not await is_premium(uid) and await used_today(uid) >= FREE_LIMIT:
        await m.answer("Bugungi bepul limit tugadi. Cheksiz foydalanish uchun obuna oling 💎")
        return
    if len(m.text) < 50:
        await m.answer("Matn juda qisqa. Kamida 2-3 ta gap yuboring.")
        return
    wait = await m.answer("Tekshiryapman... ⏳")
    try:
        await wait.edit_text(await ask_ai(PROMPTS["writing"], m.text))
        await add_usage(uid)
    except Exception as e:
        print("Xato:", e)
        await wait.edit_text("Xatolik yuz berdi, keyinroq urinib ko'ring.")


# ---------- Mini App ----------
def verify_init_data(init_data: str):
    try:
        pairs = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True))
        got = pairs.pop("hash")
        check_str = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calc = hmac.new(secret, check_str.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calc, got):
            return None
        return json.loads(pairs["user"])
    except Exception:
        return None


async def api_ai(request):
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "bad"}, status=400)
    user = verify_init_data(data.get("initData", ""))
    if not user:
        return web.json_response({"ok": False, "error": "auth"}, status=401)
    mode = data.get("mode")
    text = (data.get("text") or "")[:3000]
    if mode not in PROMPTS or len(text) < 10:
        return web.json_response({"ok": False, "error": "bad"}, status=400)
    uid = user["id"]
    if not await is_premium(uid) and await used_today(uid) >= FREE_LIMIT:
        return web.json_response({"ok": False, "error": "limit"})
    try:
        out = await ask_ai(PROMPTS[mode], text)
        await add_usage(uid)
        return web.json_response({"ok": True, "text": out})
    except Exception as e:
        print("Xato:", e)
        return web.json_response({"ok": False, "error": "ai"})


async def app_page(request):
    path = os.path.join(os.path.dirname(__file__), "webapp", "index.html")
    return web.FileResponse(path)


async def health(request):
    return web.Response(text="OK")


async def start_web():
    app = web.Application()
    app.add_routes(
        [web.get("/", health), web.get("/app", app_page), web.post("/api/ai", api_ai)]
    )
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()


async def main():
    await init_db()
    await start_web()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
