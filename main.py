import asyncio
import logging
import re
from datetime import datetime
from typing import Any

import dateparser
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore

# ---------- НАСТРОЙКИ ----------
API_TOKEN = "8604277117:AAGeHqTCidTZ0WLFCSssw07Txqrd83D-F8Y"           # Токен бота
GROUP_CHAT_ID = -5287608002         # ID группы (начинается с -100 для супергрупп)
ADMIN_IDS = [5726645385]       # Список Telegram ID администраторов
TIMEZONE = "Europe/Moscow"              # Ваш часовой пояс
# --------------------------------

jobstores = {'default': MemoryJobStore()}
scheduler = AsyncIOScheduler(jobstores=jobstores)

bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
logging.basicConfig(level=logging.INFO)

# Состояния FSM
class PostForm(StatesGroup):
    waiting_for_content = State()   # Ожидание контента (сообщения/медиа)
    waiting_for_time = State()      # Ожидание времени

# Проверка прав
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# Парсинг времени (поддержка "каждый день")
def parse_time_input(text: str) -> tuple[str, Any] | None:
    """
    Возвращает кортеж (type, value), где type:
      - 'once': datetime объект
      - 'daily': (hour, minute)
    Если не удалось распознать, возвращает None.
    """
    text = text.lower().strip()

    # Проверка на ежедневное повторение
    daily_patterns = [
        r'(каждый день|ежедневно)\s+в\s+(.+)',
        r'(каждый день|ежедневно)\s+(.+)',
        r'в\s+(.+)\s+(каждый день|ежедневно)',
        r'(.+)\s+(каждый день|ежедневно)',
    ]
    for pattern in daily_patterns:
        match = re.match(pattern, text)
        if match:
            # Извлекаем часть с временем
            groups = match.groups()
            time_part = groups[0] if 'каждый день' in groups[1] else groups[1]
            # Парсим время
            dt = dateparser.parse(time_part, languages=['ru'], settings={'TIMEZONE': TIMEZONE})
            if dt:
                return ('daily', (dt.hour, dt.minute))
            # Попробуем просто ЧЧ:ММ
            time_match = re.match(r'(\d{1,2})[:.](\d{2})', time_part)
            if time_match:
                hour, minute = map(int, time_match.groups())
                if 0 <= hour <= 23 and 0 <= minute <= 59:
                    return ('daily', (hour, minute))
            return None

    # Если нет ключевых слов "каждый день" — парсим как обычную дату/время
    dt = dateparser.parse(text, languages=['ru'], settings={
        'PREFER_DATES_FROM': 'future',
        'TIMEZONE': TIMEZONE,
        'RETURN_AS_TIMEZONE_AWARE': False
    })
    if dt:
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return ('once', dt)
    return None

# Сохранение метаданных задания
def save_job_metadata(job_id: str, post_info: dict):
    if not hasattr(dp, 'scheduled_posts'):
        dp.scheduled_posts = {}
    dp.scheduled_posts[job_id] = post_info

def remove_job_metadata(job_id: str):
    if hasattr(dp, 'scheduled_posts') and job_id in dp.scheduled_posts:
        del dp.scheduled_posts[job_id]

# Функция отправки поста по job_id
async def send_post_by_id(job_id: str):
    post_info = getattr(dp, 'scheduled_posts', {}).get(job_id)
    if not post_info:
        logging.error(f"Нет метаданных для задания {job_id}")
        return

    try:
        content_type = post_info['type']
        if content_type == 'text':
            await bot.send_message(chat_id=GROUP_CHAT_ID, text=post_info['text'])

        elif content_type == 'photo':
            await bot.send_photo(
                chat_id=GROUP_CHAT_ID,
                photo=post_info['file_id'],
                caption=post_info.get('caption', '')
            )

        elif content_type == 'video':
            await bot.send_video(
                chat_id=GROUP_CHAT_ID,
                video=post_info['file_id'],
                caption=post_info.get('caption', '')
            )

        elif content_type == 'document':
            await bot.send_document(
                chat_id=GROUP_CHAT_ID,
                document=post_info['file_id'],
                caption=post_info.get('caption', '')
            )

        elif content_type == 'poll':
            await bot.send_poll(
                chat_id=GROUP_CHAT_ID,
                question=post_info['question'],
                options=post_info['options'],
                is_anonymous=post_info.get('is_anonymous', False),
                allows_multiple_answers=post_info.get('allows_multiple_answers', False)
            )

        logging.info(f"Пост {job_id} отправлен в группу")
    except Exception as e:
        logging.error(f"Ошибка отправки поста {job_id}: {e}")

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Доступ запрещён.")
        return
    await message.answer(
        "👋 Привет, администратор!\n"
        "Просто отправь мне пост (текст, фото, видео, документ, опрос), а затем напиши время.\n\n"
        "Примеры времени:\n"
        "• через 10 минут\n"
        "• завтра в 10:00\n"
        "• в понедельник 14:30\n"
        "• каждый день в 9 утра\n\n"
        "Команды:\n"
        "/newpost – начать создание поста\n"
        "/list – показать запланированные посты\n"
        "/cancel – отменить текущее действие"
    )

# Команда /newpost – начало создания
@dp.message(Command("newpost"))
async def cmd_newpost(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(PostForm.waiting_for_content)
    await message.answer("📤 Отправь мне контент, который нужно постить в группу (текст, фото, видео, документ, опрос).")

# Обработка контента (текст, медиа, опрос)
@dp.message(PostForm.waiting_for_content, F.text)
async def process_text_content(message: Message, state: FSMContext):
    await state.update_data(
        type='text',
        text=message.text,
        entities=message.entities  # можно сохранить для форматирования, но пока не используем
    )
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Теперь укажи время отправки (например, 'завтра в 10:00' или 'каждый день в 8 утра').")

@dp.message(PostForm.waiting_for_content, F.photo)
async def process_photo_content(message: Message, state: FSMContext):
    # Берём самое большое фото
    file_id = message.photo[-1].file_id
    caption = message.caption or ""
    await state.update_data(type='photo', file_id=file_id, caption=caption)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Теперь укажи время отправки.")

@dp.message(PostForm.waiting_for_content, F.video)
async def process_video_content(message: Message, state: FSMContext):
    await state.update_data(type='video', file_id=message.video.file_id, caption=message.caption or "")
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Теперь укажи время отправки.")

@dp.message(PostForm.waiting_for_content, F.document)
async def process_document_content(message: Message, state: FSMContext):
    await state.update_data(type='document', file_id=message.document.file_id, caption=message.caption or "")
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Теперь укажи время отправки.")

@dp.message(PostForm.waiting_for_content, F.poll)
async def process_poll_content(message: Message, state: FSMContext):
    poll = message.poll
    await state.update_data(
        type='poll',
        question=poll.question,
        options=[opt.text for opt in poll.options],
        is_anonymous=poll.is_anonymous,
        allows_multiple_answers=poll.allows_multiple_answers
    )
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Теперь укажи время отправки.")

# Если прислали что-то другое (стикер, голос и т.п.)
@dp.message(PostForm.waiting_for_content)
async def process_unsupported_content(message: Message, state: FSMContext):
    await message.answer("❌ Этот тип контента не поддерживается. Отправь текст, фото, видео, документ или опрос.")

# Обработка времени
@dp.message(PostForm.waiting_for_time)
async def process_time(message: Message, state: FSMContext):
    result = parse_time_input(message.text)
    if result is None:
        await message.answer("❌ Не удалось распознать время. Попробуй ещё раз или /cancel")
        return

    schedule_type, value = result
    data = await state.get_data()
    content_type = data['type']

    # Формируем метаданные поста
    post_info = {
        'type': content_type,
        'author_id': message.from_user.id,
        'preview': ''
    }

    if content_type == 'text':
        post_info['text'] = data['text']
        post_info['preview'] = data['text'][:50] + '...' if len(data['text']) > 50 else data['text']
    elif content_type in ('photo', 'video', 'document'):
        post_info['file_id'] = data['file_id']
        post_info['caption'] = data.get('caption', '')
        post_info['preview'] = f"{'🖼' if content_type=='photo' else '🎬' if content_type=='video' else '📎'} {post_info['caption'][:50]}" if post_info['caption'] else (content_type.capitalize())
    elif content_type == 'poll':
        post_info['question'] = data['question']
        post_info['options'] = data['options']
        post_info['is_anonymous'] = data.get('is_anonymous', False)
        post_info['allows_multiple_answers'] = data.get('allows_multiple_answers', False)
        post_info['preview'] = f"📊 {data['question'][:50]}"

    # Создаём задание в планировщике
    if schedule_type == 'once':
        dt = value
        if dt <= datetime.now():
            await message.answer("⚠️ Указанное время уже прошло. Укажи будущее время.")
            return
        job_id = f"once_{dt.timestamp()}_{message.from_user.id}"
        trigger = DateTrigger(run_date=dt)
        run_description = dt.strftime('%d.%m.%Y %H:%M')
    else:  # daily
        hour, minute = value
        job_id = f"daily_{hour:02d}{minute:02d}_{message.from_user.id}"
        trigger = CronTrigger(hour=hour, minute=minute)
        run_description = f"ежедневно в {hour:02d}:{minute:02d}"

    # Сохраняем метаданные
    save_job_metadata(job_id, post_info)

    # Добавляем задание
    scheduler.add_job(
        send_post_by_id,
        trigger=trigger,
        args=[job_id],
        id=job_id,
        replace_existing=True
    )

    await state.clear()
    await message.answer(
        f"✅ Пост запланирован {run_description}.\n"
        f"ID: {job_id}"
    )

# Команда /list – показать запланированные посты
@dp.message(Command("list"))
async def cmd_list(message: Message):
    if not is_admin(message.from_user.id):
        return

    jobs = scheduler.get_jobs()
    if not jobs:
        await message.answer("📭 Нет запланированных постов.")
        return

    scheduled = getattr(dp, 'scheduled_posts', {})
    builder = InlineKeyboardBuilder()

    for job in jobs:
        job_id = job.id
        if job_id in scheduled:
            info = scheduled[job_id]
            # Определяем время следующего запуска
            if isinstance(job.trigger, DateTrigger):
                run_time = job.next_run_time.strftime('%d.%m %H:%M') if job.next_run_time else '?'
            else:  # CronTrigger
                run_time = f"ежедн {job.trigger.hour:02d}:{job.trigger.minute:02d}"
            preview = info.get('preview', '?')
            button_text = f"{run_time} – {preview}"
        else:
            run_time = job.next_run_time.strftime('%d.%m %H:%M') if job.next_run_time else '?'
            button_text = f"{run_time} (ID:{job_id[:8]})"

        builder.button(text=button_text, callback_data=f"cancel_{job_id}")

    builder.adjust(1)
    await message.answer(
        "📋 Запланированные посты (нажми для отмены):",
        reply_markup=builder.as_markup()
    )

# Отмена поста
@dp.callback_query(F.data.startswith("cancel_"))
async def cancel_post(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return

    job_id = callback.data[7:]
    job = scheduler.get_job(job_id)
    if job:
        scheduler.remove_job(job_id)
        remove_job_metadata(job_id)
        await callback.message.edit_text(
            f"🗑 Пост с ID {job_id} отменён.",
            reply_markup=None
        )
        await callback.answer("Пост удалён")
    else:
        await callback.answer("Задание не найдено", show_alert=True)

# Команда /cancel – выход из состояния
@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("Нет активного действия.")
        return
    await state.clear()
    await message.answer("Действие отменено.")

# Запуск
async def on_startup():
    scheduler.start()
    logging.info("Планировщик запущен")

async def on_shutdown():
    scheduler.shutdown()
    await bot.session.close()

async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())