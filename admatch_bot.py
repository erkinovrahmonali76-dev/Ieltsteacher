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
