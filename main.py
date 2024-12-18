import asyncio
import logging
import sys
import re  # Добавляем импорт re для регулярных выражений
from os import getenv
from telethon import TelegramClient
import calendar
from datetime import datetime, timedelta
from aiogram import F, Bot, Dispatcher, html, types, filters
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import Message
import time
from contextlib import asynccontextmanager
import aiosqlite
from urllib.parse import urlparse

from aiogram.filters.callback_data import CallbackData
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
import sqlite3
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from telethon.tl.functions.channels import GetFullChannelRequest, JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.errors import InviteHashExpiredError, InviteHashInvalidError
import os
from aiogram.types import FSInputFile
from typing import Optional
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel
import tempfile
from telethon.sessions import StringSession  # Добавить в начало с другими импортами

# В начале файла добавляем константы
api_id = ""
api_hash = ""
TOKEN = "7859103520:AAGAXZdFen9ffp7XOlBUy4x4cFphsaDBTw4"
USER_PHONE = "+7"  # Замените на реальный номер телефона
ADMIN_CHAT_ID = 1234567  # Замените на ваш ID чата
db_name = "PR_Data_.db"  

# Глобальные переменные
client = None
db_lock = asyncio.Lock()
flood_wait_until = None

# Остальные глобальные переменные
dates = ''                                              
ID_Chan = ''
description = ''

# Создание таблицы при запуске
def init_db():
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    # Проверяем существующие колонки
    c.execute("PRAGMA table_info(data)")
    columns = [column[1] for column in c.fetchall()]
    
    # Создаем таблицу, если её нет
    c.execute("""
        CREATE TABLE IF NOT EXISTS data (
            ID_Chan TEXT PRIMARY KEY,
            description TEXT,
            dates TEXT DEFAULT '',
            message_id INTEGER,
            chat_id INTEGER,
            is_scheduled INTEGER DEFAULT 0
        )
    """)
    
    # Добавляем недостающие колонки
    if 'message_id' not in columns:
        c.execute("ALTER TABLE data ADD COLUMN message_id INTEGER")
    if 'chat_id' not in columns:
        c.execute("ALTER TABLE data ADD COLUMN chat_id INTEGER")
    if 'is_scheduled' not in columns:
        c.execute("ALTER TABLE data ADD COLUMN is_scheduled INTEGER DEFAULT 0")
    
    conn.commit()
    conn.close()

# Вызываем функцию для создания новой структуры БД
init_db()

# Контекстный менеджер для работы с БД
@asynccontextmanager
async def get_db():
    conn = sqlite3.connect(db_name)
    try:
        yield conn
    finally:
        conn.close()

# Добавьте в начало файла после импортов
class TelegramClientSingleton:
    _instance = None
    _client = None
    
    @classmethod
    async def get_client(cls):
        if cls._client is None or not cls._client.is_connected():
            cls._client = TelegramClient('user_session', api_id, api_hash)  # Используем строку вместо StringSession()
            if not cls._client.is_connected():
                await cls._client.start(phone=USER_PHONE)
        return cls._client
    
    @classmethod
    async def disconnect(cls):
        if cls._client and cls._client.is_connected():
            await cls._client.disconnect()
            cls._client = None

async def get_telethon_client():
    return await TelegramClientSingleton.get_client()

async def cleanup_old_sessions():
    """Очищает старые файлы сессий"""
    try:
        current_time = int(time.time())
        for file in os.listdir():
            if file.startswith("bot_session_") and file.endswith(".session"):
                try:
                    # Получаем timestamp из имени файла
                    timestamp = int(file.replace("bot_session_", "").replace(".session", ""))
                    # Удаляем файлы старше 1 часа
                    if current_time - timestamp > 3600:
                        os.remove(file)
                except (ValueError, OSError) as e:
                    logging.error(f"Ошибка при очистке старой сессии {file}: {e}")
    except Exception as e:
        logging.error(f"Ошибка при очистке старых сессий: {e}")

# Функция для очистки URL канала
def clean_channel_url(channel_url: str) -> str:
    """Очищает URL канала от лишних элементов"""
    if not channel_url:
        return ""
    
    # Для приватных ссылок возвращаем как есть
    if 'joinchat' in channel_url or '+' in channel_url:
        return channel_url
    
    # Убираем t.me/ и https://t.me/ если есть
    channel = channel_url.replace('https://t.me/', '').replace('t.me/', '')
    
    # Убираем @ если есть
    if channel.startswith('@'):
        channel = channel[1:]
    
    return channel

async def get_channel_subscribers(channel_id: str) -> int:
    try:
        original_url = channel_id  # Сохраняем оригинальную ссылку
        channel_id = clean_channel_url(channel_id)
        if not channel_id:
            return 0
            
        client = await get_telethon_client()
        try:
            try:
                # Сначала пробуем как публичный канал
                channel = await client.get_entity(f"@{channel_id}")
            except Exception:
                try:
                    if 'joinchat' in original_url or '+' in original_url:
                        # Извлекаем хеш из ссылки
                        invite_hash = original_url.split('/')[-1].replace('+', '')
                        
                        try:
                            # Проверяем инвайт перед присоединением
                            invite_info = await client(CheckChatInviteRequest(invite_hash))
                            
                            try:
                                # Пытаемся присоединиться к каналу
                                result = await client(ImportChatInviteRequest(invite_hash))
                                channel = result.chats[0]
                            except Exception as e:
                                if hasattr(invite_info, 'chat'):
                                    channel = invite_info.chat
                                else:
                                    raise e
                        except (InviteHashExpiredError, InviteHashInvalidError):
                            logging.error("Ссылка-приглашение недействительна или истекла")
                            return 0
                    else:
                        # Пробуем присоединиться напрямую к каналу
                        try:
                            channel = await client.get_entity(channel_id)
                            await client(JoinChannelRequest(channel))
                        except Exception as e:
                            logging.error(f"Не удалось присоединиться к каналу: {e}")
                            return 0

                except Exception as e:
                    logging.error(f"Не удалось получить доступ к каналу: {e}")
                    return 0

            # Получаем полную информацию о канале
            full_channel = await client(GetFullChannelRequest(channel))
            return full_channel.full_chat.participants_count

        except Exception as e:
            logging.error(f"Ошибка при получении информации о канале: {e}")
            return 0
            
    except Exception as e:
        logging.error(f"Ошибка при получении количества подписчиков: {e}")
        return 0

async def safe_db_operation(operation):
    async with db_lock:
        conn = None
        try:
            conn = sqlite3.connect(db_name, timeout=60.0)
            conn.execute('PRAGMA busy_timeout = 60000')
            result = operation(conn)
            return result
        finally:
            if conn:
                conn.close()

def get_db_connection():
    return sqlite3.connect(db_name, timeout=20, check_same_thread=False)

async def get_channels_from_db():
    def operation(conn):
        c = conn.cursor()
        c.execute("SELECT ID_Chan, description FROM data WHERE ID_Chan IS NOT NULL AND ID_Chan != ''")
        return c.fetchall()
    
    return await safe_db_operation(operation)

dp = Dispatcher()
class DeleteChannelStates(StatesGroup):
    waiting_for_channel = State()
class UserChannelStates(StatesGroup):
    waiting_for_id = State()
    waiting_for_url = State()  # Новое состояние для URL канала
    waiting_for_screenshot = State()
    waiting_for_message_text = State()
class ChannelCallback(CallbackData, prefix="channel"):
    action: str
    id: str
class MyCallback(CallbackData, prefix='my'):
    delete: str
    add:str 
class CalendarCallback:
    def __init__(self, action: str, year: int, month: int, day: int):
        self.action = action
        self.year = year
        self.month = month
        self.day = day

    @staticmethod
    def pack(action: str, year: int, month: int, day: int) -> str:
        return f"{action}:{year}:{month}:{day}"

    @staticmethod
    def unpack(cb_data: str) -> 'CalendarCallback':
        action, year, month, day = cb_data.split(':')
        return CalendarCallback(action, int(year), int(month), int(day))
class AddChannelStates(StatesGroup):
    waiting_for_id = State()
    waiting_for_description = State()

async def create_calendar(year: int = None, month: int = None):
    now = datetime.now()
    if year is None:
        year = now.year
    if month is None:
        month = now.month

    keyboard = InlineKeyboardBuilder()
    
    # Первая строка - месяц и год
    keyboard.row(
        InlineKeyboardButton(text=f'{datetime(year, month, 1).strftime("%B %Y")}', callback_data='ignore')
    )
    
    # Вторая строка - дни недели
    week_days = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
    for day in week_days:
        keyboard.add(InlineKeyboardButton(text=day, callback_data='ignore'))
    keyboard.adjust(7)

    month_calendar = calendar.monthcalendar(year, month)
    for week in month_calendar:
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(text=" ", callback_data='ignore'))
            else:
                # Проверяем, не прошла ли дата
                current_date = datetime(year, month, day)
                if current_date.date() < now.date():
                    # Для рошедших дат покаыаем символ ❌
                    row.append(InlineKeyboardButton(text="❌", callback_data='ignore'))
                else:
                    row.append(InlineKeyboardButton(
                        text=str(day),
                        callback_data=CalendarCallback.pack('day', year, month, day)
                    ))
        keyboard.row(*row)

    # Кнопки навигации
    keyboard.row(
        InlineKeyboardButton(text="<<", callback_data=CalendarCallback.pack('prev-month', year, month, 1)),
        InlineKeyboardButton(text=">>", callback_data=CalendarCallback.pack('next-month', year, month, 1))
    )

    return keyboard.as_markup()

@dp.message(Command("channels"))
async def show_channels(message: Message):
    channels = await get_channels_from_db()
    if not channels:
        await message.answer("Нет добавленных каналов.")
        return

    keyboard_of_chanells = await create_channels_keyboard()
    
    # Формируем описание каналов с количеством подписчиков
    descriptions = []
    for channel in channels:
        try:
            subscribers = await get_channel_subscribers(channel[0])
            descriptions.append(
                f"ID: {channel[0]}\n"
                f"Описание: {channel[1]}\n"
                f"Подписчиков: {subscribers}"
            )
        except Exception as e:
            logging.error(f"Ошибка при получении информации о канале {channel[0]}: {e}")
            descriptions.append(
                f"ID: {channel[0]}\n"
                f"Описание: {channel[1]}\n"
                f"Подписчиков: Ошибка подсчета"
            )
    
    description_text = "\n\n".join(descriptions)
    await message.answer(
        "Выберите канал для панрования:\n\n" + description_text, 
        reply_markup=keyboard_of_chanells
    )

async def create_channels_keyboard():
    channels = await get_channels_from_db()
    builder = InlineKeyboardBuilder()
    
    for channel in channels:
        try:
            subscribers = await get_channel_subscribers(channel[0])
            builder.button(
                text=f"{channel[0]} | {subscribers} подписчиков", 
                callback_data=ChannelCallback(action="select", id=channel[0]).pack()
            )
        except Exception as e:
            logging.error(f"Ошибка при создании кнопки для канала {channel[0]}: {e}")
            builder.button(
                text=f"{channel[0]} | Ошибка подсчета", 
                callback_data=ChannelCallback(action="select", id=channel[0]).pack()
            )
    
    builder.adjust(1)  # Размещаем кнопки в один столбец
    return builder.as_markup()

@dp.callback_query(ChannelCallback.filter(F.action == "select"))
async def channel_selected(callback_query: CallbackQuery, callback_data: ChannelCallback, state: FSMContext):
    channel_id = callback_data.id
    if not channel_id:
        await callback_query.message.answer("Ошибка: не выбран канал")
        return
        
    logging.info(f"Выбран канал: {channel_id}")
    
    try:
        # Сохраняем выбранный канал в состояние
        await state.update_data(target_channel=channel_id, selected_channel=channel_id)  # Сохраняем в обоих ключах
        
        # Получаем последний п��ст
        last_post = await get_last_post(channel_id)
        
        if last_post:
            if last_post['text']:
                await callback_query.message.answer(f"Последний пост с канала:\n\n{last_post['text']}")
            
            if last_post['file_path']:
                try:
                    if last_post['media_type'] == 'photo':
                        await callback_query.message.answer_photo(FSInputFile(last_post['file_path']))
                    elif last_post['media_type'] == 'video':
                        await callback_query.message.answer_video(FSInputFile(last_post['file_path']))
                    elif last_post['media_type'] == 'animation':
                        await callback_query.message.answer_animation(FSInputFile(last_post['file_path']))
                finally:
                    if os.path.exists(last_post['file_path']):
                        os.remove(last_post['file_path'])
        
        # Показываем календарь
        await callback_query.message.answer(
            f"Выберите дату для канала {channel_id}:",
            reply_markup=await create_calendar()
        )
        
    except Exception as e:
        logging.error(f"Ошибка при обработке выбора канала: {e}")
        await callback_query.message.answer(
            "Произошла ошибка при получении информации о канале. "
            "Пожалуйста, попробуйте еще раз."
        )

def create_hours_keyboard(year: int, month: int, day: int, channel_id: str):
    keyboard = InlineKeyboardBuilder()
    
    # Получаем текущее время
    now = datetime.now()
    selected_date = datetime(year, month, day)
    
    # Получаем занятые часы для выбранной даты и канала
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    date_prefix = f"{day}.{month}.{year}"
    c.execute("SELECT dates FROM data WHERE ID_Chan = ? AND dates LIKE ?", 
             (channel_id, f"{date_prefix}%"))
    busy_times = [int(row[0].split()[-1].split(':')[0]) for row in c.fetchall()]
    conn.close()
    
    # Список разрешенных часов
    allowed_hours = [1, 2, 3, 4, 5, 6, 8, 9, 11, 12, 14, 16, 18]
    
    # Фильтруем часы
    available_hours = []
    for hour in allowed_hours:
        # Проверяем, не прошло ли время
        current_time = datetime(year, month, day, hour)
        if current_time > now and hour not in busy_times:
            available_hours.append(hour)
    
    # Если нет доступных часов
    if not available_hours:
        keyboard.add(InlineKeyboardButton(
            text="Нет доступного времени",
            callback_data="no_time"
        ))
    else:
        for hour in available_hours:
            keyboard.add(InlineKeyboardButton(
                text=f"{hour:02d}:00",
                callback_data=f"time:{year}:{month}:{day}:{hour}"
            ))
    
    keyboard.add(InlineKeyboardButton(
        text="Назад к календарю",
        callback_data=f"back_to_calendar"
    ))
    
    keyboard.adjust(4)
    return keyboard.as_markup()

@dp.callback_query(lambda c: c.data.startswith(('day', 'prev-month', 'next-month')))
async def process_calendar_selection(callback_query: CallbackQuery, state: FSMContext):
    callback_data = CalendarCallback.unpack(callback_query.data)
    
    if callback_data.action == "day":
        # Проверяем, не прошла ли выбранная дата
        selected_date = datetime(callback_data.year, callback_data.month, callback_data.day)
        if selected_date.date() < datetime.now().date():
            await callback_query.answer("Нельзя выбрать пршедшую дату!", show_alert=True)
            return
            
        # Получаем выбранный канал из состояния
        state_data = await state.get_data()
        channel_id = state_data.get("selected_channel")
        
        await callback_query.message.edit_text(
            f"Выберите время на {callback_data.day}.{callback_data.month}.{callback_data.year}:",
            reply_markup=create_hours_keyboard(
                callback_data.year, 
                callback_data.month, 
                callback_data.day,
                channel_id
            )
        )
    
    elif callback_data.action == "prev-month":
        # Переход к предыдущему месяцу
        if callback_data.month == 1:
            prev_year = callback_data.year - 1
            prev_month = 12
        else:
            prev_year = callback_data.year
            prev_month = callback_data.month - 1
        await callback_query.message.edit_reply_markup(
            reply_markup=await create_calendar(prev_year, prev_month)
        )
    
    elif callback_data.action == "next-month":
        # Переход к следующему месяцу
        if callback_data.month == 12:
            next_year = callback_data.year + 1
            next_month = 1
        else:
            next_year = callback_data.year
            next_month = callback_data.month + 1
            
        await callback_query.message.edit_reply_markup(
            reply_markup=await create_calendar(next_year, next_month)
        )

@dp.callback_query(lambda c: c.data.startswith('time:'))
async def process_time_selection(callback_query: CallbackQuery, state: FSMContext):
    _, year, month, day, hour = callback_query.data.split(':')
    dates = f"{day}.{month}.{year} {hour}:00"
    
    # Получаем выбранный канал из состояния
    state_data = await state.get_data()
    channel_id = state_data.get("target_channel")
    
    if not channel_id:
        await callback_query.message.edit_text("Ошибка: канал не выбран. Начните сначала с /channels")
        return
    
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    try:
        # Проверяем, не занято ли время
        c.execute("SELECT dates FROM data WHERE ID_Chan = ? AND dates = ?", 
                  (channel_id, dates))
        if c.fetchone():
            await callback_query.message.edit_text("Это время уже занято. Выберите другое время.")
            return
        
        # Обновляем запись в базе данных
        c.execute("UPDATE data SET dates = ? WHERE ID_Chan = ?", 
                  (dates, channel_id))
        conn.commit()

        # Сохраняем дату и канал в состояние
        await state.update_data(scheduled_date=dates, target_channel=channel_id)
        
        # Запрашиваем URL канала пользователя
        await callback_query.message.edit_text(
            "Пожалуйста, отправьте URL вашего канала для проверки подписчиков\n"
            "Формат: @channel или https://t.me/channel"
        )
        await state.set_state(UserChannelStates.waiting_for_url)
        
    finally:
        conn.close()

# Добавляем обработчик URL канала
@dp.message(UserChannelStates.waiting_for_url)
async def process_user_channel(message: Message, state: FSMContext):
    user_channel = message.text
    state_data = await state.get_data()
    target_channel = state_data.get("target_channel")
    scheduled_date = state_data.get("scheduled_date")
    
    try:
        # Очищаем ID каналов
        user_channel = clean_channel_url(user_channel)
        target_channel = clean_channel_url(target_channel)
        
        # Получаем информацию о каналах
        target_subscribers = await get_channel_subscribers(target_channel)
        user_subscribers = await get_channel_subscribers(user_channel)

        if target_subscribers == 0 or user_subscribers == 0:
            await message.answer("Не удалось получить информацию о подписчиках одного из каналов")
            return

        # Проверяем условия
        if target_subscribers < 100:
            is_suitable = True
        else:
            required_subscribers = target_subscribers * 1.1
            is_suitable = (user_subscribers >= target_subscribers) or (user_subscribers >= required_subscribers)

        if not is_suitable:
            await message.answer(
                f"Ваш канал не соответствует требованиям по количеству подписчиков\n"
                f"Подписчики выбранного канала: {target_subscribers}\n"
                f"Подписчики вашего канала: {user_subscribers}"
            )
            return

        # Получаем последний пост
        last_post = await get_last_post(target_channel)
        if last_post:
            await message.answer("Последний пост с канала:")
            
            if last_post['text']:
                await message.answer(last_post['text'])
            
            if last_post['media']:
                if last_post['media_type'] == 'photo':
                    await message.answer_photo(last_post['media'])
                elif last_post['media_type'] == 'video':
                    await message.answer_video(last_post['media'])
                elif last_post['media_type'] == 'document':
                    await message.answer_document(last_post['media'])

        await message.answer(
            f"Проверка пройдена успешно!\n"
            f"Подписчики выбранного канала: {target_subscribers}\n"
            f"Подписчики вашего канала: {user_subscribers}\n"
            f"Выбранное время: {scheduled_date}\n"
            f"Пожалуйста, отправьте скриншот отложенного поста"
        )
        await state.set_state(UserChannelStates.waiting_for_screenshot)

    except Exception as e:
        logging.error(f"Ошибка при проверке каналов: {str(e)}")
        await message.answer(f"Произошла ошибка при проверке каналов: {str(e)}")
        await state.clear()

@dp.message(UserChannelStates.waiting_for_screenshot)
async def process_screenshot(message: Message, state: FSMContext):
    if not message.photo:
        await message.answer("Пожалуйста, отправьте скриншот")
        return
        
    state_data = await state.get_data()
    channel_id = state_data.get("target_channel")
    scheduled_date = state_data.get("scheduled_date")
    
    try:
        # Получаем файл фото
        file = await message.bot.get_file(message.photo[-1].file_id)
        file_path = file.file_id
        
        # Создаем клавиатуру
        keyboard = InlineKeyboardBuilder()
        keyboard.button(
            text="❌ Удалить бронь",
            callback_data=f"delete_booking:{channel_id}:{scheduled_date}"
        )
        
        # Отправляем фото и информацию админу
        await message.bot.send_photo(
            chat_id=ADMIN_CHAT_ID,
            photo=file_path,
            caption=(
                f"📅 Новая бронь:\n"
                f"Канал: {channel_id}\n"
                f"Время: {scheduled_date}"
            ),
            reply_markup=keyboard.as_markup()
        )
        
        await message.answer("✅ Скриншот успешно отправлен администратору")
        await message.answer("Пожалуйста, отправьте текст сообщения для публикации")
        await state.set_state(UserChannelStates.waiting_for_message_text)
        
    except Exception as e:
        logging.error(f"Ошибка при обработке скриншота: {e}")
        await message.answer(
            "Произошла ошибка при обработке скриншота. "
            "Пожалуйста, попробуйте отправить его еще раз."
        )

@dp.message(UserChannelStates.waiting_for_id)
async def process_user_channel(message: Message, state: FSMContext):
    user_channel = message.text
    state_data = await state.get_data()
    target_channel = state_data.get("target_channel")
    scheduled_date = state_data.get("scheduled_date")  # Получаем сохраненную дату
    
    try:
        # Очищаем ID каналов
        user_channel = clean_channel_url(user_channel)
        target_channel = clean_channel_url(target_channel)
        
        # Получаем информацию о каналах
        target_subscribers = await get_channel_subscribers(target_channel)
        user_subscribers = await get_channel_subscribers(user_channel)

        if target_subscribers == 0 or user_subscribers == 0:
            await message.answer("Не удалось получить информацию о подписчиках одного из каналов")
            return

        # Проверяем условия
        if target_subscribers < 100:
            is_suitable = True
        else:
            required_subscribers = target_subscribers * 1.1
            is_suitable = (user_subscribers >= target_subscribers) or (user_subscribers >= required_subscribers)

        if not is_suitable:
            await message.answer(
                f"Ваш канал не соответствует требованиям по количеству подписчиков\n"
                f"Подписчики выбранного канала: {target_subscribers}\n"
                f"Подписчики вашего канала: {user_subscribers}"
            )
            return

        # Получаем последний пост
        last_post = await get_last_post(target_channel)
        if last_post:
            await message.answer("Последний пост с канала:")
            
            if last_post['text']:
                await message.answer(last_post['text'])
            
            if last_post['media']:
                if last_post['media_type'] == 'photo':
                    await message.answer_photo(last_post['media'])
                elif last_post['media_type'] == 'video':
                    await message.answer_video(last_post['media'])
                elif last_post['media_type'] == 'document':
                    await message.answer_document(last_post['media'])
        else:
            await message.answer("Не удалось получить последний пост из канала")

        await message.answer(
            f"Проверка пройдена успешно!, , на выбранное время и пришлите в этот чат скриншот \n"
            f"Подписчики выбранного канала: {target_subscribers}\n"
            f"Подписчики вашего канала: {user_subscribers}\n"
            f"Выбранное время: {scheduled_date}\n"  # Используем сохраненную дату
            f"Пожалуйста, отправьте скриншот отложенного поста"
        )

        # Переходим к ожиданию скриншота вместо очистки состояния
        await state.set_state(UserChannelStates.waiting_for_screenshot)

    except Exception as e:
        logging.error(f"Ошибка при проверке каналов: {str(e)}")
        await message.answer(f"Произошла ошибка при проверке каналов: {str(e)}")
        await state.clear()

async def find_last_message_id(bot: Bot, chat_id: int) -> Optional[int]:
    """Находит ID последнего сообщения в канале методом бинарного поиска"""
    try:
        left = 1
        right = 1000000  # Предполагаемый максимальный ID
        last_valid_id = None

        while left <= right:
            mid = (left + right) // 2
            try:
                message = await bot.get_message(chat_id, mid)
                last_valid_id = mid
                left = mid + 1
            except Exception:
                right = mid - 1

        return last_valid_id

    except Exception as e:
        logging.error(f"Ошибка при поиске последнего сообщения: {e}")
        return None

async def get_last_post(channel_id: str):
    try:
        # Очищаем ID канала
        channel_id = clean_channel_url(channel_id)
        
        if not channel_id:
            return None
            
        client = await get_telethon_client()
        
        try:
            # Получаем сущность канала
            entity = await client.get_entity(f"@{channel_id}")
            
            # Получаем последнее сообщение
            messages = await client.get_messages(entity, limit=1)
            
            if not messages:
                return None
                
            message = messages[0]
            result = {
                'text': message.message or "",
                'media': None,
                'media_type': None,
                'file_path': None
            }
            
            if message.media:
                # Создаем временную директорию для медиафайлов
                temp_dir = "temp_media"
                os.makedirs(temp_dir, exist_ok=True)
                
                # Загружаем медиафайл
                file_path = await client.download_media(
                    message.media,
                    file=os.path.join(temp_dir, f"media_{int(time.time())}")
                )
                
                result['file_path'] = file_path
                
                if hasattr(message.media, 'photo'):
                    result['media_type'] = 'photo'
                elif hasattr(message.media, 'document'):
                    if message.media.document.mime_type == 'video/mp4':
                        result['media_type'] = 'video'
                    elif message.media.document.mime_type.startswith('image/'):
                        result['media_type'] = 'animation'
            
            return result
            
        except Exception as e:
            logging.error(f"Ошибка при получении сообщения: {e}")
            return None
            
    except Exception as e:
        logging.error(f"Ошибка при получении поста: {e}")
        return None

@dp.message(UserChannelStates.waiting_for_message_text)
async def process_message_text(message: Message, state: FSMContext):
    try:
        state_data = await state.get_data()
        channel_id = state_data.get("target_channel")
        scheduled_date = state_data.get("scheduled_date")
        
        if not channel_id or not scheduled_date:
            await message.answer("Ошибка: не найдены данные о канале или дате")
            await state.clear()
            return

        async with get_db() as conn:
            c = conn.cursor()
            # Сохраняем ID сообщения и чата
            c.execute("""
                UPDATE data 
                SET dates = ?,
                    message_id = ?,
                    chat_id = ?,
                    is_scheduled = 1 
                WHERE ID_Chan = ?
            """, (scheduled_date, message.message_id, message.chat.id, channel_id))
            conn.commit()
        
        await message.answer("✅ Сообщение сохранено и будет опубликовано в назначенное время")
        await state.clear()
        
    except Exception as e:
        logging.error(f"Ошибка при сохранении сообщения: {e}")
        await message.answer("Произошла ошибка при сохранении сообщения")
        await state.clear()

@dp.callback_query(lambda c: c.data.startswith('delete_booking:') or c.data == "back_to_calendar")
async def process_booking_actions(callback_query: CallbackQuery):
    if callback_query.data == "back_to_calendar":
        await callback_query.message.edit_text(
            "Выберите дату:",
            reply_markup=await create_calendar()
        )
        return

    try:
        # Обработка удаления брони
        data_parts = callback_query.data.split(':')
        if len(data_parts) != 3:
            logging.error(f"Неверный формат данных: {callback_query.data}")
            await callback_query.answer("Ошибка формата данных")
            return
            
        _, channel_id, scheduled_date = data_parts
        
        conn = sqlite3.connect(db_name)
        c = conn.cursor()
        try:
            c.execute("""
                UPDATE data 
                SET dates = '', 
                    message_id = NULL, 
                    chat_id = NULL, 
                    is_scheduled = 0 
                WHERE ID_Chan = ? AND dates = ?
            """, (channel_id, scheduled_date))
            conn.commit()
            
            await callback_query.message.edit_text(
                f"❌ Бронь удалена\n"
                f"Канал: {channel_id}\n"
                f"Время: {scheduled_date}"
            )
        finally:
            conn.close()
            
    except Exception as e:
        logging.error(f"Ошибка при удалении брони: {e}")
        await callback_query.answer("Произошла ошибка при удалении брони")

def create_keyboard():
    bulder = InlineKeyboardBuilder()
    bulder.button(
        text='Удалить',
        callback_data=MyCallback(delete='delete', add="")
        )
    bulder.button(
        text='Добавить',
        callback_data=MyCallback(delete="", add="add")
        )
    return bulder.as_markup()
def create_keyboard_of_chanells():
    bulder_chan = InlineKeyboardBuilder()
    bulder_chan.button(
        text='Удалить',
        callback_data=MyCallback(delete='delete', add="")
        )
    bulder_chan.button(
        text='Добавить',
        callback_data=MyCallback(delete="", add="add")
        )
    return bulder_chan.as_markup()

@dp.message(Command("add"))
async def command_add(message: Message) -> None:
    try:
        username = message.from_user.username
        if username in ['zolillidosov']:  # Убираем Vladmushroom
            await message.reply(f"Привет, {username}, Что ты хочешь сделать?", reply_markup=create_keyboard())
        else:
            await message.reply('У вас нет доступа к этой команде')
    except Exception as e:
        logging.error(f"Ошибка в command_add: {e}")
        await message.reply("Произошла ошибка при обработке команды")

@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    """
    Обработчик команды /start
    Сохраняет chat_id администратора при первом запуске
    """
    if message.from_user.username in ['zolillidosov']:  # Убираем Vladmushroom
        global ADMIN_CHAT_ID
        ADMIN_CHAT_ID = message.chat.id
        logging.info(f"Admin chat_id updated: {ADMIN_CHAT_ID}")
        await message.answer(
            f"✅ Бот активирован\n"
            f"🆔 Ваш chat_id: {ADMIN_CHAT_ID}\n"
            f"👤 Username: @{message.from_user.username}"
        )
    else:
        await message.answer("Привет! ")

@dp.callback_query(MyCallback.filter(F.delete == "delete"))
async def my_callback_foo_delete(query: CallbackQuery, callback_data: MyCallback, state: FSMContext):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("SELECT ID_Chan, description FROM data")
    channels = c.fetchall()
    conn.close()

    keyboard = InlineKeyboardBuilder()
    for channel_id, desc in channels:
        keyboard.button(
            text=f"{channel_id} - {desc}", 
            callback_data=ChannelCallback(action="delete", id=channel_id).pack()
        )
    keyboard.adjust(1)

    await query.message.answer("Выберите канал для удаления:", reply_markup=keyboard.as_markup())
    await state.set_state(DeleteChannelStates.waiting_for_channel)

@dp.callback_query(ChannelCallback.filter(F.action == "delete"))
async def delete_channel(query: CallbackQuery, callback_data: ChannelCallback):
    channel_id = callback_data.id
    
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("DELETE FROM data WHERE ID_Chan = ?", (channel_id,))
    conn.commit()
    conn.close()
    
    await query.message.answer(f"Канал {channel_id} успешно удален!")



@dp.callback_query(MyCallback.filter(F.add == "add"))
async def my_callback_foo_add(query: CallbackQuery, callback_data: MyCallback, state: FSMContext):
    await query.message.answer("Напишите ID канала")
    await state.set_state(AddChannelStates.waiting_for_id)

@dp.message(AddChannelStates.waiting_for_id)
async def process_channel_id(message: Message, state: FSMContext):
    await state.update_data(channel_id=message.text)
    await message.answer("Напишите описание канала")
    await state.set_state(AddChannelStates.waiting_for_description)

@dp.message(AddChannelStates.waiting_for_description)
async def process_channel_description(message: Message, state: FSMContext):
    user_data = await state.get_data()
    channel_id = user_data['channel_id']
    description = message.text
    dates=''
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("INSERT INTO data (description, ID_Chan,dates) VALUES (?, ?, ?)", 
              (description, channel_id, dates))
    conn.commit()
    conn.close()

    await message.answer(f"Канал с ID {channel_id} успешно добавлен!")
    await state.clear()
 





async def check_scheduled_messages():
    bot = Bot(token=TOKEN)
    while True:
        try:
            now = datetime.now()
            async with aiosqlite.connect(db_name) as db:
                async with db.execute("""
                    SELECT ID_Chan, dates, message_id, chat_id 
                    FROM data 
                    WHERE is_scheduled = 1 
                    AND dates != ''
                """) as cursor:
                    async for channel_id, scheduled_date, message_id, chat_id in cursor:
                        try:
                            scheduled_time = datetime.strptime(scheduled_date, "%d.%m.%Y %H:%M")
                            
                            if now >= scheduled_time:
                                client = await get_telethon_client()
                                target_channel = await client.get_entity(f"@{channel_id}")
                                
                                # Пересылаем сообщение
                                await client.forward_messages(
                                    target_channel,
                                    messages=message_id,
                                    from_peer=chat_id
                                )
                                
                                # Очищаем данные в БД
                                await db.execute("""
                                    UPDATE data 
                                    SET dates = '', 
                                        message_id = NULL,
                                        chat_id = NULL,
                                        is_scheduled = 0 
                                    WHERE ID_Chan = ? AND dates = ?
                                """, (channel_id, scheduled_date))
                                await db.commit()
                                
                        except Exception as e:
                            logging.error(f"Ошибка при отправке сообщения в канал {channel_id}: {e}")
            
            await asyncio.sleep(60)
            
        except Exception as e:
            logging.error(f"Ошибка при проверке запланированных сообщений: {e}")
            await asyncio.sleep(60)
        finally:
            await bot.session.close()

async def main():
    # Инициализируем базу данных
    init_db()
    
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    
    try:
        # Инициализируем клиент
        client = await get_telethon_client()
        logging.info("Telethon client initialized successfully")
        
        # Запускаем проверку запланированных сообщений
        asyncio.create_task(check_scheduled_messages())
        
        # Запускаем бота
        await dp.start_polling(bot)
    finally:
        # Закрываем соединения
        await TelegramClientSingleton.disconnect()
        await bot.session.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped!")