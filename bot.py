# bot.py ichida 3 ta joyni almashtiring.

# 1) MODELS qatorini shunday qiling (ko'proq zaxira model):
MODELS = [x for x in [os.environ.get("GEMINI_MODEL"), "gemini-2.5-flash", "gemini-flash-latest",
                      "gemini-2.0-flash", "gemini-2.5-flash-lite"] if x]

# 2) ask_ai funksiyasini butunlay shu bilan almashtiring
#    (har modelga 2 urinish, 45 soniya limit, 429/503 da kutib qayta urinadi):
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

# 3) send_receipt ichida, "plan = plan if plan in PLANS else None" qatorini shunga almashtiring
#    (chek kelganda reja ko'rsatilmasa, avtomatik 1 hafta bo'ladi):
    plan = plan if plan in PLANS else "week"
