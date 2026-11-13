import asyncio, base64, hashlib, hmac, json, os, random, urllib.parse
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
DATABASE_URL = os.environ["DATABASE_URL"]
WEBAPP_URL = os.environ.get("WEBAPP_URL", "").rstrip("/")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
PORT = int(os.environ.get("PORT", 10000))
MODELS = [x for x in [os.environ.get("GEMINI_MODEL"), "gemini-2.5-flash", "gemini-flash-latest",
                      "gemini-2.0-flash", "gemini-2.5-flash-lite"] if x]
REF_DAYS = 3
PLANS = {"week": 7, "month": 30}
PLAN_NAME = {"week": "1 hafta", "month": "1 oy"}
SET = {"week_price": "40000", "month_price": "66000", "free_limit": "5",
       "card": "6262 5701 3419 2691", "holder": "Boratova Zarnigor"}

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
ai = genai.Client(api_key=GEMINI_API_KEY)
pool: asyncpg.Pool = None
BOT_USERNAME = ""
LAST_ERR = ""

PROMPTS = {
    "writing": (
        "Sen tajribali IELTS Writing examinersan. Foydalanuvchi vazifa (Task) va inglizcha matn yuboradi. "
        "Javobni O'ZBEK tilida, shu tartibda ber:\n**Taxminiy band:** X.X\n"
        "**Mezonlar:** Task Response, Coherence, Lexical Resource, Grammar (har biriga band va 1 gap)\n"
        "**Asosiy xatolar:** xato → to'g'risi (3-5 ta)\n**Yaxshilangan paragraf:** (inglizcha)\n"
        "**Maslahat:** 2-3 ta qisqa.\nQisqa va aniq yoz."),
    "speaking": (
        "Sen tajribali IELTS Speaking examinersan. Foydalanuvchi savol va o'z javobini yuboradi. "
        "Javobni O'ZBEK tilida, shu tartibda ber:\n**Taxminiy band:** X.X\n"
        "**Mezonlar:** Fluency, Vocabulary, Grammar, Pronunciation(matn bo'yicha taxmin)\n"
        "**Xatolar:** xato → to'g'risi\n**Namunaviy javob:** (inglizcha, band 7 darajasida, qisqa)\n"
        "**Maslahat:** 2 ta.\nQisqa yoz."),
}
TOPICS = ["climate and environment", "technology in education", "health and sleep", "city life", "ancient history",
          "space exploration", "language learning", "sports and fitness", "food and agriculture", "art and music",
          "transport", "tourism", "wildlife", "renewable energy", "psychology", "architecture"]


def price(plan): return int(SET[f"{plan}_price"])
def money(n): return f"{int(n):,}".replace(",", " ") + " so'm"
def free_limit(): return int(SET["free_limit"])


async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, statement_cache_size=0)
    for q in [
        "CREATE TABLE IF NOT EXISTS usage (user_id BIGINT, day DATE, count INT, PRIMARY KEY (user_id, day))",
        "CREATE TABLE IF NOT EXISTS pro (user_id BIGINT PRIMARY KEY, until DATE)",
        "ALTER TABLE pro ADD COLUMN IF NOT EXISTS reminded DATE",
        "CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY, joined DATE, ref BIGINT)",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS name TEXT",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT",
        "CREATE TABLE IF NOT EXISTS payments (id SERIAL PRIMARY KEY, user_id BIGINT, plan TEXT, amount INT DEFAULT 0, "
        "status TEXT DEFAULT 'pending', created TIMESTAMP DEFAULT now(), decided TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)"]:
        await pool.execute(q)
    for r in await pool.fetch("SELECT key, value FROM settings"):
        if r["key"] in SET:
            SET[r["key"]] = r["value"]


async def register(uid, name="", username=None, ref=None):
    return await pool.fetchval(
        "INSERT INTO users (user_id, joined, ref, name, username) VALUES ($1,$2,$3,$4,$5) "
        "ON CONFLICT (user_id) DO UPDATE SET name=EXCLUDED.name, username=EXCLUDED.username RETURNING (xmax = 0)",
        uid, date.today(), ref, name, username)


async def pro_until(uid):
    return await pool.fetchval("SELECT until FROM pro WHERE user_id=$1 AND until>=$2", uid, date.today())


async def is_pro(uid):
    return uid == ADMIN_ID or bool(await pro_until(uid))


async def grant(uid, days):
    cur = await pro_until(uid)
    new = max(cur or date.today(), date.today()) + timedelta(days=days)
    await pool.execute("INSERT INTO pro (user_id, until) VALUES ($1,$2) ON CONFLICT (user_id) DO UPDATE SET until=$2", uid, new)
    return new


async def used_today(uid):
    return await pool.fetchval("SELECT count FROM usage WHERE user_id=$1 AND day=$2", uid, date.today()) or 0


async def add_usage(uid):
    await pool.execute("INSERT INTO usage (user_id, day, count) VALUES ($1,$2,1) "
                       "ON CONFLICT (user_id, day) DO UPDATE SET count = usage.count + 1", uid, date.today())


async def ask_ai(system, text, json_mode=False):
    global LAST_ERR
    kw = {"response_mime_type": "application/json"} if json_mode else {}
    cfg = types.GenerateContentConfig(system_instruction=system, **kw)
    for mdl in MODELS:
        for _ in range(2):
            try:
                r = await asyncio.wait_for(
                    ai.aio.models.generate_content(model=mdl, contents=text, config=cfg), 45)
                t = (r.text or "").strip()
                if t:
                    LAST_ERR = ""
                    return t
                LAST_ERR = f"{mdl}: bo'sh javob"
            except Exception as e:
                LAST_ERR = f"{mdl}: {str(e)[:300]}"
                print("AI xato:", LAST_ERR)
                if "429" not in str(e) and "503" not in str(e):
                    break
                await asyncio.sleep(2)
    raise RuntimeError(LAST_ERR or "bo'sh javob")


async def reminder_loop():
    while True:
        try:
            rows = await pool.fetch("SELECT user_id FROM pro WHERE until=$1 AND (reminded IS NULL OR reminded<$2)",
                                    date.today() + timedelta(days=3), date.today())
            for r in rows:
                try:
                    await bot.send_message(r["user_id"], "⏰ Pro obunangiz 3 kundan keyin tugaydi. Davom ettirish uchun /pro")
                except Exception:
                    pass
                await pool.execute("UPDATE pro SET reminded=$2 WHERE user_id=$1", r["user_id"], date.today())
        except Exception as e:
            print("Eslatma xatosi:", e)
        await asyncio.sleep(6 * 3600)


def pay_text():
    return ("💎 <b>Pro obuna</b>\n\n"
            f"• 1 hafta — {money(price('week'))}\n• 1 oy — {money(price('month'))}\n\n"
            "✅ Cheksiz AI tekshiruv\n✅ Barcha testlar va AI yaratgan yangi testlar\n\n"
            f"💳 Karta: <code>{SET['card']}</code>\n👤 {SET['holder']}\n\n"
            "To'lovdan so'ng <b>chek rasmini shu botga yuboring</b> (yoki ilova ichidan). "
            "Admin tasdiqlasa Pro avtomatik yoqiladi.")


def app_kb(label="📚 Ilovani ochish", page="/app"):
    if not WEBAPP_URL:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=label, web_app=WebAppInfo(url=WEBAPP_URL + page))]])


async def send_receipt(uid, name, username, photo, plan=None):
    if not ADMIN_ID:
        return False
    plan = plan if plan in PLANS else "week"
    pid = await pool.fetchval("INSERT INTO payments (user_id, plan, amount) VALUES ($1,$2,$3) RETURNING id",
                              uid, plan, price(plan))
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ 1 hafta ({money(price('week'))})", callback_data=f"ap:{pid}:week")],
        [InlineKeyboardButton(text=f"✅ 1 oy ({money(price('month'))})", callback_data=f"ap:{pid}:month")],
        [InlineKeyboardButton(text="❌ Rad etish", callback_data=f"no:{pid}")]])
    sel = f"{PLAN_NAME[plan]} ({money(price(plan))})"
    cap = f"💳 To'lov cheki #{pid}\n👤 {name} {('@' + username) if username else ''}\n🆔 {uid}\n📦 Tanlangan: {sel}"
    await bot.send_photo(ADMIN_ID, photo, caption=cap, reply_markup=kb)
    return True


async def settle(pid, action, plan=None):
    row = await pool.fetchrow("SELECT * FROM payments WHERE id=$1", pid)
    if not row:
        return "Topilmadi"
    ok = action == "ok"
    plan = (plan or row["plan"] or "week") if ok else row["plan"]
    got = await pool.fetchval(
        "UPDATE payments SET status=$2, plan=$3, amount=$4, decided=now() WHERE id=$1 AND status='pending' RETURNING id",
        pid, "approved" if ok else "rejected", plan, price(plan) if ok else row["amount"])
    if not got:
        return "Allaqachon ko'rib chiqilgan"
    uid = row["user_id"]
    if ok:
        until = await grant(uid, PLANS[plan])
        msg, note = (f"🎉 Pro faollashdi ({PLAN_NAME[plan]})! {until:%d.%m.%Y} gacha cheksiz foydalaning.",
                     f"✅ Tasdiqlandi: {PLAN_NAME[plan]}")
    else:
        msg, note = "❌ To'lov tasdiqlanmadi. Chekni tekshirib, qayta yuboring.", "❌ Rad etildi"
    try:
        await bot.send_message(uid, msg)
    except Exception:
        pass
    return note


# ---------- bot ----------
@dp.message(CommandStart())
async def start(m: Message):
    uid = m.from_user.id
    parts = (m.text or "").split()
    ref = int(parts[1][4:]) if len(parts) > 1 and parts[1].startswith("ref_") and parts[1][4:].isdigit() else None
    if ref == uid:
        ref = None
    new = await register(uid, m.from_user.full_name, m.from_user.username, ref)
    if new and ref:
        await grant(uid, REF_DAYS)
        await grant(ref, REF_DAYS)
        for x, t in ((uid, "🎁 Do'st taklifi uchun sizga"), (ref, "🎁 Do'stingiz qo'shilgani uchun sizga")):
            try:
                await bot.send_message(x, f"{t} +{REF_DAYS} kun Pro berildi!")
            except Exception:
                pass
    await m.answer("Salom! Men IELTS AI ustozman 🎓\n\nIlovada Writing, Speaking, Reading, Listening va lug'at mashqlari bor. "
                   f"Bepul: kuniga {free_limit()} ta AI tekshiruv. Cheksiz foydalanish uchun /pro", reply_markup=app_kb())


@dp.message(F.text == "/pro")
async def pro_cmd(m: Message):
    await m.answer(pay_text(), parse_mode="HTML", reply_markup=app_kb("💎 Ilovada to'lash", "/app#pro"))


@dp.message(F.text == "/admin")
async def admin_cmd(m: Message):
    if m.from_user.id == ADMIN_ID:
        await m.answer("🛠 Admin panel", reply_markup=app_kb("🛠 Ochish", "/app#admin"))


@dp.message(F.text == "/test")
async def test_cmd(m: Message):
    if m.from_user.id != ADMIN_ID:
        return
    try:
        await ask_ai("Reply with OK", "Say OK")
        await m.answer("✅ AI ishlayapti")
    except Exception as e:
        await m.answer(f"❌ AI xatosi:\n{str(e)[:900]}")


@dp.message(F.photo)
async def photo(m: Message):
    await register(m.from_user.id, m.from_user.full_name, m.from_user.username)
    ok = await send_receipt(m.from_user.id, m.from_user.full_name, m.from_user.username, m.photo[-1].file_id)
    await m.answer("✅ Chek adminga yuborildi. Tasdiqlangach xabar keladi." if ok else "Admin hali sozlanmagan.")


@dp.callback_query(F.data.regexp(r"^(ap:\d+:(week|month)|no:\d+)$"))
async def decide(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID:
        return await c.answer("Ruxsat yo'q", show_alert=True)
    p = c.data.split(":")
    note = await settle(int(p[1]), "ok" if p[0] == "ap" else "no", p[2] if p[0] == "ap" else None)
    await c.message.edit_caption(caption=(c.message.caption or "") + f"\n\n{note}", reply_markup=None)
    await c.answer()


@dp.message(F.text)
async def check(m: Message):
    uid = m.from_user.id
    await register(uid, m.from_user.full_name, m.from_user.username)
    if not await is_pro(uid) and await used_today(uid) >= free_limit():
        return await m.answer(pay_text(), parse_mode="HTML", reply_markup=app_kb("💎 Ilovada to'lash", "/app#pro"))
    if len(m.text) < 50:
        return await m.answer("Matn juda qisqa. Kamida 2-3 ta gap yuboring.")
    wait = await m.answer("Tekshiryapman... ⏳")
    try:
        await wait.edit_text((await ask_ai(PROMPTS["writing"], m.text))[:4000].replace("**", ""))
        await add_usage(uid)
    except Exception:
        await wait.edit_text("Xatolik yuz berdi, keyinroq urinib ko'ring." + (f"\n\n{LAST_ERR}" if uid == ADMIN_ID else ""))


# ---------- api ----------
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


def bad(err, status=400, **kw):
    return web.json_response({"ok": False, "error": err, **kw}, status=status)


def dt(x):
    return x.strftime("%d.%m.%Y") if x else None


async def api_me(request):
    u, _ = await auth(request)
    if not u:
        return bad("auth", 401)
    uid = u["id"]
    name = (u.get("first_name", "") + " " + u.get("last_name", "")).strip()
    await register(uid, name, u.get("username"))
    until = await pro_until(uid)
    admin = uid == ADMIN_ID
    hist = await pool.fetch("SELECT plan, amount, status, created FROM payments WHERE user_id=$1 ORDER BY id DESC LIMIT 5", uid)
    return web.json_response({
        "ok": True, "id": uid, "name": u.get("first_name", ""), "admin": admin,
        "pro": admin or bool(until), "until": "cheksiz" if admin else dt(until),
        "left": max(0, free_limit() - await used_today(uid)), "limit": free_limit(),
        "plans": {p: {"days": PLANS[p], "price": price(p)} for p in PLANS},
        "card": SET["card"], "holder": SET["holder"],
        "ref": f"https://t.me/{BOT_USERNAME}?start=ref_{uid}",
        "joined": dt(await pool.fetchval("SELECT joined FROM users WHERE user_id=$1", uid)),
        "checks": int(await pool.fetchval("SELECT coalesce(sum(count),0) FROM usage WHERE user_id=$1", uid)),
        "invited": await pool.fetchval("SELECT count(*) FROM users WHERE ref=$1", uid),
        "hist": [{"plan": PLAN_NAME.get(h["plan"], "—"), "amount": h["amount"], "status": h["status"],
                  "date": dt(h["created"])} for h in hist]})


async def api_ai(request):
    u, d = await auth(request)
    if not u:
        return bad("auth", 401)
    mode, text = d.get("mode"), (d.get("text") or "")[:3500]
    if mode not in PROMPTS or len(text) < 10:
        return bad("bad")
    uid = u["id"]
    pro = await is_pro(uid)
    if not pro and await used_today(uid) >= free_limit():
        return bad("limit", 200)
    try:
        out = await ask_ai(PROMPTS[mode], text)
        await add_usage(uid)
        left = None if pro else max(0, free_limit() - await used_today(uid))
        return web.json_response({"ok": True, "text": out[:4000], "left": left})
    except Exception:
        return bad("ai", 200, detail=LAST_ERR if uid == ADMIN_ID else "")


async def api_gen(request):
    u, d = await auth(request)
    if not u:
        return bad("auth", 401)
    if not await is_pro(u["id"]):
        return bad("pro", 200)
    kind = d.get("kind")
    if kind == "reading":
        sysm = ("You are an IELTS exam writer. Return ONLY JSON: {\"t\": \"an academic passage of 180-220 words\", "
                "\"q\": [5 items like {\"q\": \"question\", \"o\": [\"opt1\",\"opt2\",\"opt3\"], \"a\": 0}]}. "
                "a is the index of the correct option. Questions must test comprehension of the passage.")
    elif kind == "listening":
        sysm = ("You are an IELTS exam writer. Return ONLY JSON: {\"t\": \"a spoken monologue of 100-130 words with concrete details "
                "(numbers, times, names)\", \"q\": [4 items like {\"q\": \"question\", \"o\": [\"opt1\",\"opt2\",\"opt3\"], \"a\": 0}]}. "
                "a is the index of the correct option.")
    else:
        return bad("bad")
    try:
        j = json.loads(await ask_ai(sysm, f"Topic: {random.choice(TOPICS)}. Difficulty: band {random.choice(['5.5', '6.5', '7.5'])}.", True))
        q = [[x["q"], [str(o) for o in x["o"]], int(x["a"])] for x in j["q"]]
        assert q and all(len(x[1]) >= 2 and 0 <= x[2] < len(x[1]) for x in q)
        return web.json_response({"ok": True, "data": {"t": str(j["t"]), "q": q}})
    except Exception as e:
        print("Gen xato:", e)
        return bad("ai", 200, detail=LAST_ERR if u["id"] == ADMIN_ID else "")


async def api_pay(request):
    u, d = await auth(request)
    if not u:
        return bad("auth", 401)
    try:
        img = base64.b64decode((d.get("image") or "").split(",")[-1])
        name = (u.get("first_name", "") + " " + u.get("last_name", "")).strip()
        ok = await send_receipt(u["id"], name, u.get("username"), BufferedInputFile(img, "check.jpg"), d.get("plan"))
    except Exception as e:
        print("Chek xatosi:", e)
        return bad("pay", 200)
    return web.json_response({"ok": ok}) if ok else bad("noadmin", 200)


async def broadcast(text):
    ids = [r[0] for r in await pool.fetch("SELECT user_id FROM users")]
    n = 0
    for i in ids:
        try:
            await bot.send_message(i, text)
            n += 1
        except Exception:
            pass
        await asyncio.sleep(0.05)
    try:
        await bot.send_message(ADMIN_ID, f"📣 Xabar yuborildi: {n}/{len(ids)}")
    except Exception:
        pass


async def api_admin(request):
    u, d = await auth(request)
    if not u or u["id"] != ADMIN_ID:
        return bad("auth", 401)
    a = d.get("a")
    td = date.today()
    if a == "stats":
        f = pool.fetchval
        return web.json_response({
            "ok": True,
            "users": await f("SELECT count(*) FROM users"),
            "new_today": await f("SELECT count(*) FROM users WHERE joined=$1", td),
            "pro": await f("SELECT count(*) FROM pro WHERE until>=$1", td),
            "active": await f("SELECT count(*) FROM usage WHERE day=$1", td),
            "checks": int(await f("SELECT coalesce(sum(count),0) FROM usage WHERE day=$1", td)),
            "pending": await f("SELECT count(*) FROM payments WHERE status='pending'"),
            "rev30": int(await f("SELECT coalesce(sum(amount),0) FROM payments WHERE status='approved' AND decided>now()-interval '30 days'")),
            "rev": int(await f("SELECT coalesce(sum(amount),0) FROM payments WHERE status='approved'"))})
    if a == "users":
        q = (d.get("q") or "").strip()
        base = ("SELECT u.user_id, u.name, u.username, u.joined, p.until, "
                "coalesce((SELECT sum(count) FROM usage x WHERE x.user_id=u.user_id),0) AS checks "
                "FROM users u LEFT JOIN pro p ON p.user_id=u.user_id ")
        if q.isdigit():
            rows = await pool.fetch(base + "WHERE u.user_id=$1", int(q))
        elif q:
            rows = await pool.fetch(base + "WHERE u.name ILIKE $1 OR u.username ILIKE $1 ORDER BY u.joined DESC LIMIT 30",
                                    f"%{q.lstrip('@')}%")
        else:
            rows = await pool.fetch(base + "ORDER BY u.joined DESC, u.user_id DESC LIMIT 30")
        return web.json_response({"ok": True, "users": [{
            "id": r["user_id"], "name": r["name"], "username": r["username"], "joined": dt(r["joined"]),
            "until": dt(r["until"]) if r["until"] and r["until"] >= td else None,
            "checks": int(r["checks"])} for r in rows]})
    if a == "pays":
        rows = await pool.fetch("SELECT p.id, p.user_id, p.plan, p.amount, p.status, p.created, u.name FROM payments p "
                                "LEFT JOIN users u ON u.user_id=p.user_id ORDER BY (p.status='pending') DESC, p.id DESC LIMIT 25")
        return web.json_response({"ok": True, "pays": [{
            "id": r["id"], "uid": r["user_id"], "name": r["name"], "plan": r["plan"], "amount": r["amount"],
            "status": r["status"], "date": dt(r["created"])} for r in rows]})
    if a == "decide":
        note = await settle(int(d["pid"]), d.get("act"), d.get("plan"))
        return web.json_response({"ok": True, "note": note})
    if a == "grant":
        until = await grant(int(d["uid"]), int(d["days"]))
        try:
            await bot.send_message(int(d["uid"]), f"🎁 Sizga Pro berildi: {until:%d.%m.%Y} gacha.")
        except Exception:
            pass
        return web.json_response({"ok": True})
    if a == "revoke":
        await pool.execute("DELETE FROM pro WHERE user_id=$1", int(d["uid"]))
        return web.json_response({"ok": True})
    if a == "bcast":
        text = (d.get("text") or "").strip()[:3000]
        if len(text) < 3:
            return bad("bad")
        asyncio.create_task(broadcast(text))
        return web.json_response({"ok": True})
    if a == "settings":
        return web.json_response({"ok": True, "set": SET})
    if a == "set":
        k, v = d.get("key"), str(d.get("value", "")).strip()
        if k not in SET or not v or ((k.endswith("price") or k == "free_limit") and not v.isdigit()):
            return bad("bad")
        SET[k] = v
        await pool.execute("INSERT INTO settings (key, value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2", k, v)
        return web.json_response({"ok": True})
    return bad("bad")


async def app_page(request):
    return web.FileResponse(os.path.join(os.path.dirname(__file__), "webapp", "index.html"))


async def health(request): return web.Response(text="OK")


async def start_web():
    app = web.Application(client_max_size=6 * 1024 * 1024)
    app.add_routes([web.get("/", health), web.get("/app", app_page), web.post("/api/me", api_me),
                    web.post("/api/ai", api_ai), web.post("/api/gen", api_gen), web.post("/api/pay", api_pay),
                    web.post("/api/admin", api_admin)])
    r = web.AppRunner(app)
    await r.setup()
    await web.TCPSite(r, "0.0.0.0", PORT).start()


async def main():
    global BOT_USERNAME
    await init_db()
    BOT_USERNAME = (await bot.get_me()).username
    asyncio.create_task(reminder_loop())
    await start_web()
    if WEBAPP_URL:
        try:
            await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text="Ilova", web_app=WebAppInfo(url=WEBAPP_URL + "/app")))
        except Exception as e:
            print("Menu tugmasi:", e)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
