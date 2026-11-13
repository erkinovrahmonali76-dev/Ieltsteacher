[README.md](https://github.com/user-attachments/files/34813800/README.md)
# AKOSH COMMUNITY BOT

Telegram hamjamiyat boti: foydalanuvchi matnli post yuboradi, admin tekshiradi, tasdiqlangan post kanalga chiqadi.

## Render sozlamalari

1. GitHub repo ichiga `bot.py`, `requirements.txt`, `render.yaml` fayllarini joylang.
2. Render'da PostgreSQL database yarating.
3. Web Service yarating va repo bilan ulang.
4. Environment qiymatlarini kiriting:
   - `BOT_TOKEN`: BotFather tokeni
   - `DATABASE_URL`: Render PostgreSQL connection string
   - `ADMIN_IDS`: admin Telegram ID raqami(lar)i, masalan `123456789,987654321`
   - `CHANNEL_ID`: kanal username'i (`@kanal`) yoki numeric chat ID
   - `DB_SSL`: odatda `require`
5. Botni kanalga administrator qiling va unga post yuborish huquqini bering.
6. Admin Telegram akkauntidan botga `/start` yuboring.
7. Render Start Command: `python bot.py`.

## Admin buyruqlari

- `/admin` — admin paneli
- `/pending` — kutilayotgan postlar
- `/stats` — statistika

## Muhim

- `BOT_TOKEN` va `DATABASE_URL` maxfiy. Ularni GitHub'ga joylamang va chatga yubormang.
- Bitta Render instance ishlating: polling bir nechta instance'da parallel ishlamasin.
- Kanalga post chiqarish uchun bot kanal administratori bo'lishi kerak.
- `DB_SSL=disable` faqat SSL talab qilmaydigan lokal/test PostgreSQL uchun.
