import asyncio
import logging
import re
from datetime import datetime
from typing import Any

import dateparser
import pytz
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
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from sqlalchemy import Column, String, Integer, PickleType
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.future import select
import os
from dotenv import load_dotenv

load_dotenv()
API_TOKEN = os.environ.get("BOT_TOKEN")
GROUP_CHAT_ID = int(os.environ.get("GROUP_CHAT_ID", "0"))
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x]
TIMEZONE = os.environ.get("TIMEZONE", "Europe/Moscow")

Base = declarative_base()
class ScheduledPost(Base):
    __tablename__ = 'scheduled_posts'
    job_id = Column(String, primary_key=True)
    type = Column(String)
    data = Column(PickleType)
    preview = Column(String)
    author_id = Column(Integer)
    schedule_type = Column(String)
    cron_expr = Column(String, nullable=True)
    run_time = Column(String, nullable=True)

engine = create_async_engine('sqlite+aiosqlite:///bot_data.db')
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

jobstores = {'default': SQLAlchemyJobStore(url='sqlite:///apscheduler_jobs.db')}
scheduler = AsyncIOScheduler(jobstores=jobstores)
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
logging.basicConfig(level=logging.INFO)

class PostForm(StatesGroup):
    waiting_for_content = State()
    waiting_for_time = State()

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def parse_natural_cron(text: str) -> tuple[str, str] | None:
    text = text.lower().strip()
    days_map = {'понедельник': 'mon', 'вторник': 'tue', 'среда': 'wed', 'четверг': 'thu',
                'пятница': 'fri', 'суббота': 'sat', 'воскресенье': 'sun'}
    month_pattern = r'каждое\s+(\d{1,2})\s+число\s+в\s+(\d{1,2}):(\d{2})'
    match = re.search(month_pattern, text)
    if match:
        day, hour, minute = map(int, match.groups())
        return ('cron', f'{minute} {hour} {day} * *')
    for day_name, day_cron in days_map.items():
        pattern = rf'каждый\s+{day_name}\s+в\s+(\d{{1,2}}):(\d{{2}})'
        match = re.search(pattern, text)
        if match:
            hour, minute = map(int, match.groups())
            return ('cron', f'{minute} {hour} * * {day_cron}')
    daily_pattern = r'каждый\s+день\s+в\s+(\d{1,2}):(\d{2})'
    match = re.search(daily_pattern, text)
    if match:
        hour, minute = map(int, match.groups())
        return ('cron', f'{minute} {hour} * * *')
    return None

def parse_time_input(text: str) -> tuple[str, Any] | None:
    cron_result = parse_natural_cron(text)
    if cron_result:
        return cron_result
    dt = dateparser.parse(text, languages=['ru'], settings={
        'PREFER_DATES_FROM': 'future', 'TIMEZONE': TIMEZONE, 'RETURN_AS_TIMEZONE_AWARE': False})
    if dt:
        if dt.tzinfo is None:
            dt = pytz.timezone(TIMEZONE).localize(dt)
        return ('once', dt)
    return None

async def save_post_metadata(job_id: str, post_info: dict):
    async with async_session() as session:
        session.add(ScheduledPost(
            job_id=job_id, type=post_info['type'], data=post_info['data'],
            preview=post_info.get('preview', ''), author_id=post_info['author_id'],
            schedule_type=post_info['schedule_type'], cron_expr=post_info.get('cron_expr'),
            run_time=post_info.get('run_time')))
        await session.commit()

async def delete_post_metadata(job_id: str):
    async with async_session() as session:
        result = await session.execute(select(ScheduledPost).where(ScheduledPost.job_id == job_id))
        sp = result.scalar_one_or_none()
        if sp:
            await session.delete(sp)
            await session.commit()

async def get_all_posts_metadata():
    async with async_session() as session:
        result = await session.execute(select(ScheduledPost))
        return {sp.job_id: sp for sp in result.scalars().all()}

async def send_post_by_id(job_id: str):
    async with async_session() as session:
        result = await session.execute(select(ScheduledPost).where(ScheduledPost.job_id == job_id))
        sp = result.scalar_one_or_none()
    if not sp:
        logging.error(f"Нет метаданных для задания {job_id}")
        return
    post_info = sp.data
    ct = sp.type
    try:
        if ct == 'text':
            entities = None
            if post_info.get('entities'):
                entities = [types.MessageEntity(**e) for e in post_info['entities']]
            await bot.send_message(GROUP_CHAT_ID, post_info['text'], entities=entities)
        elif ct == 'photo':
            await bot.send_photo(GROUP_CHAT_ID, post_info['file_id'], caption=post_info.get('caption'))
        elif ct == 'video':
            await bot.send_video(GROUP_CHAT_ID, post_info['file_id'], caption=post_info.get('caption'))
        elif ct == 'document':
            await bot.send_document(GROUP_CHAT_ID, post_info['file_id'], caption=post_info.get('caption'))
        elif ct == 'audio':
            await bot.send_audio(GROUP_CHAT_ID, post_info['file_id'], caption=post_info.get('caption'))
        elif ct == 'animation':
            await bot.send_animation(GROUP_CHAT_ID, post_info['file_id'], caption=post_info.get('caption'))
        elif ct == 'sticker':
            await bot.send_sticker(GROUP_CHAT_ID, post_info['file_id'])
        elif ct == 'voice':
            await bot.send_voice(GROUP_CHAT_ID, post_info['file_id'])
        elif ct == 'video_note':
            await bot.send_video_note(GROUP_CHAT_ID, post_info['file_id'])
        elif ct == 'poll':
            await bot.send_poll(GROUP_CHAT_ID, post_info['question'], post_info['options'],
                                is_anonymous=post_info.get('is_anonymous', False),
                                allows_multiple_answers=post_info.get('allows_multiple_answers', False))
        elif ct == 'location':
            await bot.send_location(GROUP_CHAT_ID, post_info['latitude'], post_info['longitude'])
        elif ct == 'contact':
            await bot.send_contact(GROUP_CHAT_ID, post_info['phone_number'], post_info['first_name'],
                                   last_name=post_info.get('last_name'), vcard=post_info.get('vcard'))
        elif ct == 'venue':
            await bot.send_venue(GROUP_CHAT_ID, post_info['latitude'], post_info['longitude'],
                                 post_info['title'], post_info['address'])
        elif ct == 'dice':
            await bot.send_dice(GROUP_CHAT_ID, emoji=post_info.get('emoji'))
        elif ct == 'album':
            media = []
            for item in post_info['media']:
                if item['type'] == 'photo':
                    media.append(types.InputMediaPhoto(media=item['file_id'], caption=item.get('caption')))
                elif item['type'] == 'video':
                    media.append(types.InputMediaVideo(media=item['file_id'], caption=item.get('caption')))
            await bot.send_media_group(GROUP_CHAT_ID, media)
        logging.info(f"Пост {job_id} отправлен")
    except Exception as e:
        logging.error(f"Ошибка отправки поста {job_id}: {e}")

# Команды (размещены до обработчиков состояний для корректного перехвата)
@dp.message(Command("start"))
async def cmd_start(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Доступ запрещён.")
        return
    await message.answer("👋 Отправь мне команду /newpost")

@dp.message(Command("newpost"))
async def cmd_newpost(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(PostForm.waiting_for_content)
    await message.answer("📤 Отправь что нужно постить в группу.")

@dp.message(Command("list"))
async def cmd_list(message: Message):
    if not is_admin(message.from_user.id):
        return
    jobs = scheduler.get_jobs()
    if not jobs:
        await message.answer("📭 Нет запланированных постов.")
        return
    scheduled = await get_all_posts_metadata()
    builder = InlineKeyboardBuilder()
    for job in jobs:
        job_id = job.id
        if job_id in scheduled:
            sp = scheduled[job_id]
            if isinstance(job.trigger, DateTrigger):
                run_time = job.next_run_time.strftime('%d.%m %H:%M') if job.next_run_time else '?'
            else:
                run_time = f"ежедн {job.trigger.hour:02d}:{job.trigger.minute:02d}" if hasattr(job.trigger, 'hour') else 'cron'
            preview = sp.preview if sp.preview else '?'
            button_text = f"{run_time} – {preview}"
        else:
            run_time = job.next_run_time.strftime('%d.%m %H:%M') if job.next_run_time else '?'
            button_text = f"{run_time} (ID:{job_id[:8]})"
        builder.button(text=button_text, callback_data=f"cancel_{job_id}")
    builder.adjust(1)
    await message.answer("📋 Запланированные посты (нажми для удаления):", reply_markup=builder.as_markup())

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

# Обработчики состояний
@dp.message(PostForm.waiting_for_content, F.media_group_id)
async def process_album(message: Message, state: FSMContext, album: list[Message]):
    media_list = []
    for msg in album:
        if msg.photo:
            media_list.append({'type': 'photo', 'file_id': msg.photo[-1].file_id, 'caption': msg.caption})
        elif msg.video:
            media_list.append({'type': 'video', 'file_id': msg.video.file_id, 'caption': msg.caption})
    await state.update_data(type='album', media=media_list)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Время отправки?")

@dp.message(PostForm.waiting_for_content, F.text)
async def process_text(message: Message, state: FSMContext):
    entities = [e.model_dump() for e in message.entities] if message.entities else None
    await state.update_data(type='text', text=message.text, entities=entities)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Время отправки?")

@dp.message(PostForm.waiting_for_content, F.photo)
async def process_photo(message: Message, state: FSMContext):
    await state.update_data(type='photo', file_id=message.photo[-1].file_id, caption=message.caption)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Время отправки?")

@dp.message(PostForm.waiting_for_content, F.video)
async def process_video(message: Message, state: FSMContext):
    await state.update_data(type='video', file_id=message.video.file_id, caption=message.caption)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Время отправки?")

@dp.message(PostForm.waiting_for_content, F.document)
async def process_document(message: Message, state: FSMContext):
    await state.update_data(type='document', file_id=message.document.file_id, caption=message.caption)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Время отправки?")

@dp.message(PostForm.waiting_for_content, F.audio)
async def process_audio(message: Message, state: FSMContext):
    await state.update_data(type='audio', file_id=message.audio.file_id, caption=message.caption)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Время отправки?")

@dp.message(PostForm.waiting_for_content, F.animation)
async def process_animation(message: Message, state: FSMContext):
    await state.update_data(type='animation', file_id=message.animation.file_id, caption=message.caption)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Время отправки?")

@dp.message(PostForm.waiting_for_content, F.sticker)
async def process_sticker(message: Message, state: FSMContext):
    await state.update_data(type='sticker', file_id=message.sticker.file_id)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Время отправки?")

@dp.message(PostForm.waiting_for_content, F.voice)
async def process_voice(message: Message, state: FSMContext):
    await state.update_data(type='voice', file_id=message.voice.file_id)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Время отправки?")

@dp.message(PostForm.waiting_for_content, F.video_note)
async def process_video_note(message: Message, state: FSMContext):
    await state.update_data(type='video_note', file_id=message.video_note.file_id)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Время отправки?")

@dp.message(PostForm.waiting_for_content, F.poll)
async def process_poll(message: Message, state: FSMContext):
    p = message.poll
    await state.update_data(type='poll', question=p.question,
                            options=[opt.text for opt in p.options],
                            is_anonymous=p.is_anonymous, allows_multiple_answers=p.allows_multiple_answers)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Время отправки?")

@dp.message(PostForm.waiting_for_content, F.location)
async def process_location(message: Message, state: FSMContext):
    loc = message.location
    await state.update_data(type='location', latitude=loc.latitude, longitude=loc.longitude)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Время отправки?")

@dp.message(PostForm.waiting_for_content, F.contact)
async def process_contact(message: Message, state: FSMContext):
    c = message.contact
    await state.update_data(type='contact', phone_number=c.phone_number, first_name=c.first_name,
                            last_name=c.last_name, vcard=c.vcard)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Время отправки?")

@dp.message(PostForm.waiting_for_content, F.venue)
async def process_venue(message: Message, state: FSMContext):
    v = message.venue
    await state.update_data(type='venue', latitude=v.location.latitude, longitude=v.location.longitude,
                            title=v.title, address=v.address)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Время отправки?")

@dp.message(PostForm.waiting_for_content, F.dice)
async def process_dice(message: Message, state: FSMContext):
    await state.update_data(type='dice', emoji=message.dice.emoji)
    await state.set_state(PostForm.waiting_for_time)
    await message.answer("🕐 Время отправки?")

@dp.message(PostForm.waiting_for_content)
async def process_unsupported(message: Message, state: FSMContext):
    await message.answer("❌ Неподдерживаемый тип.")

@dp.message(PostForm.waiting_for_time)
async def process_time(message: Message, state: FSMContext):
    # Проверка на команду отмены прямо в состоянии
    if message.text and message.text.startswith('/cancel'):
        await state.clear()
        await message.answer("Действие отменено.")
        return

    result = parse_time_input(message.text)
    if result is None:
        await message.answer("❌ Не могу распознать время. Попробуй ещё раз или /cancel")
        return

    schedule_type, value = result
    data = await state.get_data()
    ct = data['type']
    post_data, preview = {}, ''
    if ct == 'text':
        post_data.update(text=data['text'], entities=data.get('entities'))
        preview = data['text'][:50] + ('...' if len(data['text']) > 50 else '')
    elif ct in ('photo', 'video', 'document', 'audio', 'animation'):
        post_data.update(file_id=data['file_id'], caption=data.get('caption'))
        preview = f"{ct} {post_data.get('caption', '')[:50]}"
    elif ct == 'sticker':
        post_data['file_id'] = data['file_id']; preview = '🎭 Стикер'
    elif ct == 'voice':
        post_data['file_id'] = data['file_id']; preview = '🎤 Голосовое'
    elif ct == 'video_note':
        post_data['file_id'] = data['file_id']; preview = '📹 Кружок'
    elif ct == 'poll':
        for k in ('question','options','is_anonymous','allows_multiple_answers'):
            post_data[k] = data[k]
        preview = f"📊 {data['question'][:50]}"
    elif ct == 'location':
        post_data.update(latitude=data['latitude'], longitude=data['longitude'])
        preview = '📍 Местоположение'
    elif ct == 'contact':
        post_data.update(phone_number=data['phone_number'], first_name=data['first_name'],
                         last_name=data.get('last_name'), vcard=data.get('vcard'))
        preview = f"👤 {data['first_name']}"
    elif ct == 'venue':
        post_data.update(latitude=data['latitude'], longitude=data['longitude'],
                         title=data['title'], address=data['address'])
        preview = f"🏛 {data['title']}"
    elif ct == 'dice':
        post_data['emoji'] = data['emoji']; preview = f"🎲 {data['emoji']}"
    elif ct == 'album':
        post_data['media'] = data['media']; preview = f"📷 Альбом ({len(data['media'])} шт.)"

    if schedule_type == 'once':
        dt = value
        if dt <= datetime.now(pytz.timezone(TIMEZONE)):
            await message.answer("⚠️ Время уже прошло. Укажи будущее.")
            return
        job_id = f"once_{dt.timestamp()}_{message.from_user.id}"
        trigger = DateTrigger(run_date=dt)
        run_description = dt.strftime('%d.%m.%Y %H:%M')
        cron_expr, run_time_str = None, dt.isoformat()
    else:
        cron_expr = value
        job_id = f"cron_{cron_expr}_{message.from_user.id}"
        tz = pytz.timezone(TIMEZONE)
        trigger = CronTrigger.from_crontab(cron_expr, timezone=tz)
        run_description = f"по расписанию: {cron_expr}"
        run_time_str = None

    post_info = {'type': ct, 'data': post_data, 'preview': preview,
                 'author_id': message.from_user.id, 'schedule_type': schedule_type,
                 'cron_expr': cron_expr, 'run_time': run_time_str}
    await save_post_metadata(job_id, post_info)
    scheduler.add_job(send_post_by_id, trigger=trigger, args=[job_id], id=job_id, replace_existing=True)
    await state.clear()
    await message.answer(f"✅ Пост запланирован {run_description}.\nID: {job_id}")

@dp.callback_query(F.data.startswith("cancel_"))
async def cancel_post(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Недостаточно прав", show_alert=True)
        return
    job_id = callback.data[7:]
    job = scheduler.get_job(job_id)
    if job:
        scheduler.remove_job(job_id)
        await delete_post_metadata(job_id)
        await callback.message.edit_text(f"🗑 Пост {job_id} отменён.", reply_markup=None)
        await callback.answer("Пост удалён")
    else:
        await callback.answer("Задание не найдено", show_alert=True)

async def on_startup():
    await init_db()
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