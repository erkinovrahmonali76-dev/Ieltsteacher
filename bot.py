import asyncio
import os
from datetime import date

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message
from google import genai
from google.genai import types
from aiohttp import web

BOT_TOKEN = os.environ["BOT_TOKEN"]
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
DATABASE_URL = os.environ["DATABASE_URL"]
PORT = int(os.environ.get("PORT", 10000))
FREE_LIMIT = 3

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
ai = genai.Client(api_key=GEMINI_API_KEY)
pool: asyncpg.Pool = None

SYSTEM = (
    "Sen IELTS Writing ustozisan. Foydalanuvchi inglizcha matn yuboradi. "
    "Javobni O'ZBEK tilida ber, quyidagi tartibda:\n"
    "1) Taxminiy band (masalan 6.0)\n"
    "2) Asosiy xatolar (grammatika, lug'at) va to'g'rilangani\n"
    "3) Matnning yaxshilangan varianti (inglizcha)\n"
    "4) 2-3 ta qisqa maslahat.\n"
    "Qisqa va aniq yoz."
)


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


@dp.message(CommandStart())
async def start(m: Message):
    await m.answer(
        "Salom! Men IELTS Writing AI ustozman 🎓\n\n"
        "Inglizcha matningizni yuboring, men band va xatolarni tekshirib beraman.\n"
        f"Bepul: kuniga {FREE_LIMIT} ta tekshiruv."
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
        resp = await ai.aio.models.generate_content(
            model=GEMINI_MODEL,
            contents=m.text,
            config=types.GenerateContentConfig(system_instruction=SYSTEM),
        )
        await wait.edit_text(resp.text[:4000])
        await add_usage(uid)
    except Exception as e:
        print("Xato:", e)
        await wait.edit_text("Xatolik yuz berdi, keyinroq urinib ko'ring.")


async def health(request):
    return web.Response(text="OK")


async def start_web():
    app = web.Application()
    app.add_routes([web.get("/", health)])
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()


async def main():
    await init_db()
    await start_web()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
