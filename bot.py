import os
import re
import asyncio
import tempfile
from openpyxl import load_workbook, Workbook
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

BOT_TOKEN = os.getenv("BOT_TOKEN")

user_queues = {}
user_tasks = {}

def is_person(fullname):
    if not fullname:
        return False

    text = str(fullname).strip()
    parts = text.split()

    if len(parts) != 3:
        return False

    bad_words = [
        "ооо", "оао", "ао", "зао", "пао", "ип",
        "компания", "управляющая", "организация",
        "администрация", "отдел", "отделение",
        "управление", "участок", "полиция",
        "суд", "банк", "школа", "лицей", "гимназия",
        "университет", "институт", "колледж",
        "центр", "служба", "фонд", "завод", "фабрика",
        "предприятие", "общество", "товарищество",
        "кооператив", "агентство", "министерство",
        "департамент", "комитет", "больница",
        "поликлиника", "клиника", "аптека",
        "магазин", "салон", "студия", "кафе",
        "ресторан", "группа", "холдинг",
        "жкх", "тсж", "жск", "снт"
    ]

    lower = text.lower()
    for word in bad_words:
        if word in lower:
            return False

    for part in parts:
        if not re.match(r"^[А-ЯЁ][а-яё-]+$", part):
            return False

    return True

def process_excel(input_path, output_path):
    wb = load_workbook(input_path, read_only=True, data_only=True)
    ws = wb.active

    rows = ws.iter_rows(values_only=True)
    headers = next(rows)

    headers_lower = [str(h).strip().lower() if h else "" for h in headers]

    try:
        request_col = headers_lower.index("request")
        fullname_col = headers_lower.index("fullname")
        address_col = headers_lower.index("address")
    except ValueError:
        raise Exception("Не нашёл столбцы request, FullName, Address")

    new_wb = Workbook()
    new_ws = new_wb.active
    new_ws.title = "result"
    new_ws.append(["request", "FullName", "Address"])

    count = 0

    for row in rows:
        fullname = row[fullname_col]

        if is_person(fullname):
            new_ws.append([
                row[request_col],
                row[fullname_col],
                row[address_col]
            ])
            count += 1

    new_wb.save(output_path)
    return count

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Я готов ✅\n"
        "Закидывай Excel-файлы .xlsx\n\n"
        "Можно сразу 4–6 файлов.\n"
        "Я оставлю только столбцы:\n"
        "request | FullName | Address\n\n"
        "Оставляю только ФИО: строго 3 слова кириллицей."
    )

async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document = update.message.document

    if not document.file_name.lower().endswith(".xlsx"):
        await update.message.reply_text("Отправь Excel-файл именно в формате .xlsx")
        return

    user_id = update.effective_user.id

    if user_id not in user_queues:
        user_queues[user_id] = []

    user_queues[user_id].append({
        "document": document,
        "chat_id": update.effective_chat.id
    })

    await update.message.reply_text(f"Файл принят ✅ {document.file_name}")

    if user_id not in user_tasks or user_tasks[user_id].done():
        user_tasks[user_id] = asyncio.create_task(process_queue(context, user_id))

async def process_queue(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    await asyncio.sleep(3)

    queue = user_queues.get(user_id, [])
    total = len(queue)

    if total == 0:
        return

    chat_id = queue[0]["chat_id"]
    file_index = 0

    while user_queues.get(user_id):
        item = user_queues[user_id].pop(0)
        document = item["document"]
        file_index += 1

        progress = await context.bot.send_message(
            chat_id=chat_id,
            text=f"🔴 0% | Файл {file_index} из {total}\n{document.file_name}"
        )

        input_path = None
        output_path = None

        try:
            await progress.edit_text(
                f"🟠 25% | Скачиваю файл {file_index} из {total}\n{document.file_name}"
            )

            tg_file = await document.get_file()

            safe_name = document.file_name.replace("/", "_").replace("\\", "_")
            input_path = os.path.join(tempfile.gettempdir(), safe_name)
            output_name = safe_name.replace(".xlsx", "_filtered.xlsx")
            output_path = os.path.join(tempfile.gettempdir(), output_name)

            await tg_file.download_to_drive(input_path)

            await progress.edit_text(
                f"🟡 50% | Обрабатываю файл {file_index} из {total}\n{document.file_name}"
            )

            count = process_excel(input_path, output_path)

            await progress.edit_text(
                f"🟢 75% | Отправляю результат {file_index} из {total}\n{document.file_name}"
            )

            with open(output_path, "rb") as result_file:
                await context.bot.send_document(
                    chat_id=chat_id,
                    document=result_file,
                    filename=output_name,
                    caption=f"Готово ✅\nФайл: {document.file_name}\nОставлено строк: {count}"
                )

            await progress.edit_text(
                f"✅ 100% | Файл {file_index} из {total} обработан\n{document.file_name}"
            )

        except Exception as e:
            await progress.edit_text(
                f"❌ Ошибка | Файл {file_index} из {total}\n{document.file_name}"
            )
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"Ошибка при обработке файла {document.file_name}:\n{e}"
            )

        finally:
            if input_path and os.path.exists(input_path):
                os.remove(input_path)
            if output_path and os.path.exists(output_path):
                os.remove(output_path)

    await context.bot.send_message(
        chat_id=chat_id,
        text="✅ Готово, все файлы обработаны"
    )

def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN не найден в Railway Variables")

    print("Бот запускается...")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_file))

    print("Бот запущен!")

    app.run_polling()

if __name__ == "__main__":
    main()
