[Uploading README (1).md…]()
# AKOSH COMMUNITY BOT

Bu bot ta’lim, texnologiya, o‘yinlar, ijod va sport mavzularidagi matnli postlarni qabul qiladi. Har bir post admin tekshiruvidan keyin Telegram kanaliga chiqariladi.

## Muhim
- Bu tanishuv yoki romantik match boti emas.
- Foydalanuvchi profillari, layklar, shaxsiy kontakt almashish va VIP to‘lovlar olib tashlangan.
- `bot.py` ichida botning barcha asosiy logikasi bitta faylda.
- Render sozlamalari va kutubxonalar uchun alohida kichik fayllar kerak.

## GitHub fayllari
Repo ichiga `bot.py`, `requirements.txt`, `runtime.txt` fayllarini joylang.

## Render
1. Render’da PostgreSQL database yarating.
2. Telegram bot tokenini BotFather’dan oling.
3. Botni post chiqariladigan kanalga admin qiling.
4. Render’da **Web Service** yarating va GitHub repo’ni ulang.
5. Build Command: `pip install -r requirements.txt`
6. Start Command: `python bot.py`
7. Environment Variables:
   - `BOT_TOKEN` = Telegram bot tokeni
   - `DATABASE_URL` = Render PostgreSQL Internal Database URL
   - `ADMIN_IDS` = admin Telegram ID raqamlari, bir nechta bo‘lsa vergul bilan
   - `CHANNEL_ID` = kanal ID yoki `@kanal_username`
   - `DB_SSL` = `require`

`runtime.txt` Python 3.12.10 ni belgilaydi.

## Telegram sozlamasi
- Bot kanalga admin bo‘lishi va post yuborish huquqiga ega bo‘lishi kerak.
- Admin botga `/start` yuborsin.
- Admin buyruqlari: `/admin`, `/pending`, `/stats`.

## Eslatma
Kod sintaksis jihatdan tekshiriladi, lekin sizning Telegram tokeningiz, Render bazangiz va kanal ruxsatlaringiz bilan jonli sinovdan o‘tkazilmagan. Token yoki database parolini hech kimga yubormang.
