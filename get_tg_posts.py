from telethon import TelegramClient
from datetime import datetime, timedelta
import logging
import random
import asyncio
from environs import Env
from datetime import timezone
from collections import defaultdict
import os
from yadisk_utils import upload_to_yadisk_and_get_url

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('parser.log')
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
env = Env()
env.read_env()

# Инициализация клиента Telegram
telegram_client = TelegramClient(
    'user_session',
    int(env('telegram_api_id')),
    env('telegram_api_hash'),
    system_version="4.16.30-vxCustom"
)

async def normalize_telegram_link(link):
    """
    Нормализует ссылку на Telegram канал/чат
    """
    link = link.strip().lower()
    link = link.split('?')[0]
    if '://' in link:
        link = link.split('://')[1]

    for prefix in ['t.me/', 'telegram.me/', 'web.telegram.org/k/#', 'web.telegram.org/z/#']:
        if link.startswith(prefix):
            link = link[len(prefix):]
            break

    if link.startswith('@'):
        link = link[1:]

    parts = link.split('/')
    chat_username = None
    message_id = None

    if len(parts) >= 1:
        chat_username = parts[0]
        chat_username = ''.join(c for c in chat_username if c.isalnum() or c in ['_'])

    if len(parts) >= 2:
        last_part = parts[-1]
        if last_part.isdigit():
            message_id = int(last_part)
        elif last_part.startswith('s'):
            maybe_id = last_part[1:]
            if maybe_id.isdigit():
                message_id = int(maybe_id)

    return chat_username, message_id

async def get_telegram_posts(chat_username, start_time, end_time):
    logger.info(f"🟢 Начало парсинга Telegram канала: {chat_username}")
    logger.info(f"⌛ Период: {start_time} - {end_time}")
    posts = []
    media_groups = defaultdict(list)
    try:
        chat = await telegram_client.get_entity(chat_username)
        total_messages = 0
        start_date = datetime.strptime(start_time, "%d.%m.%Y").replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)
        end_date = datetime.strptime(end_time, "%d.%m.%Y").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        logger.info(f"Ищем посты с {start_date} по {end_date}")
        async for message in telegram_client.iter_messages(chat, offset_date=end_date):
            if start_date <= message.date <= end_date:
                total_messages += 1
                if total_messages % 10 == 0:
                    logger.info(f"📊 Обработано сообщений: {total_messages} | Последнее: {message.date}")
                if message.text:
                    likes = 0
                    if hasattr(message, 'reactions') and message.reactions:
                        if hasattr(message.reactions, 'results'):
                            likes = sum(reaction.count for reaction in message.reactions.results)
                        elif hasattr(message.reactions, 'recent_reactions'):
                            likes = len(message.reactions.recent_reactions)
                    if hasattr(message, 'grouped_id') and message.grouped_id:
                        media_groups[message.grouped_id].append(message)
                    else:
                        photo_url = None
                        if message.photo:
                            try:
                                file_path = await telegram_client.download_media(message.photo)
                                if file_path:
                                    photo_url = await upload_to_yadisk_and_get_url(file_path)
                                    if not photo_url:
                                        logger.error(f"Не удалось получить ссылку на фото для поста {message.id}")
                            except Exception as e:
                                logger.error(f"Ошибка при загрузке фото: {str(e)}")
                        posts.append({
                            'group_link': f"https://t.me/{chat_username}",
                            'post_link': f"https://t.me/{chat_username}/{message.id}",
                            'text': message.text,
                            'date': message.date.strftime("%d.%m.%Y"),
                            'likes': likes,
                            'views': message.views if hasattr(message, 'views') else 0,
                            'photo_url': photo_url,
                            'additional_photos': []
                        })
            elif message.date < start_date:
                logger.info(f"🔴 Достигнут предел периода для {chat_username}")
                break
            await asyncio.sleep(random.uniform(0.5, 1.5))
        for group_id, messages in media_groups.items():
            messages = sorted(messages, key=lambda m: m.id)
            first_message = messages[0]
            photo_urls = []
            for msg in messages:
                if msg.photo:
                    try:
                        file_path = await telegram_client.download_media(msg.photo)
                        if file_path:
                            photo_url = await upload_to_yadisk_and_get_url(file_path)
                            if photo_url:
                                photo_urls.append(photo_url)
                            else:
                                logger.error(f"Не удалось получить ссылку на фото из медиагруппы для поста {msg.id}")
                    except Exception as e:
                        logger.error(f"Ошибка при загрузке фото из медиагруппы: {str(e)}")
                        continue
            posts.append({
                'group_link': f"https://t.me/{chat_username}",
                'post_link': f"https://t.me/{chat_username}/{first_message.id}",
                'text': first_message.text,
                'date': first_message.date.strftime("%d.%m.%Y"),
                'likes': likes,
                'views': first_message.views if hasattr(first_message, 'views') else 0,
                'photo_url': photo_urls[0] if photo_urls else None,
                'additional_photos': photo_urls[1:] if len(photo_urls) > 1 else []
            })
        logger.info(f"✅ Завершён парсинг {chat_username}. Найдено постов: {len(posts)}")
    except Exception as e:
        logger.error(f"❌ Ошибка при парсинге {chat_username}: {str(e)}")
    return posts

async def parse_telegram_sources(sources):
    """
    Парсит все Telegram источники из списка
    """
    today = datetime.now()
    start_time = today.strftime("%d.%m.%Y")
    end_time = today.strftime("%d.%m.%Y")
    
    all_posts = []
    parsed_links = set()

    for link in sources:
        if 't.me' in link or 'telegram.me' in link:
            if link in parsed_links:
                continue

            try:
                chat_username, _ = await normalize_telegram_link(link)
                if chat_username:
                    posts = await get_telegram_posts(chat_username, start_time, end_time)
                    all_posts.extend(posts)
                    parsed_links.add(link)
                    logger.info(f"✅ Источник {link} успешно обработан")

            except Exception as e:
                logger.error(f"❌ Ошибка при обработке источника {link}: {str(e)}")
                continue

    return all_posts

async def get_tg_posts(client, channel_username, start_time, end_time):
    logger.info(f"Начинаю сбор постов из канала {channel_username}")
    posts = []
    try:
        channel = await client.get_entity(channel_username)
        async for message in client.iter_messages(channel, limit=100):
            logger.info(f"Обрабатываю сообщение: id={message.id}, дата={message.date}")
            if start_time <= message.date <= end_time:
                logger.info(f"Сообщение {message.id} попадает в фильтр по дате")
                post_date = message.date.strftime("%d.%m.%Y")
                photo_url = None
                if message.media:
                    logger.info(f"Сообщение {message.id} содержит медиа")
                    if message.photo:
                        logger.info(f"Сообщение {message.id} содержит фото")
                        photo_url = message.photo.url
                    elif message.media_group_id:
                        logger.info(f"Сообщение {message.id} является частью медиагруппы")
                        # Обработка медиагруппы
                        async for group_message in client.iter_messages(channel, ids=message.media_group_id):
                            if group_message.photo:
                                photo_url = group_message.photo.url
                                break
                posts.append({
                    'post_link': f"https://t.me/{channel_username}/{message.id}",
                    'text': message.text,
                    'date': post_date,
                    'photo_url': photo_url
                })
                logger.info(f"Пост {message.id} добавлен в список")
    except Exception as e:
        logger.error(f"Ошибка при сборе постов из Telegram: {e}")
    return posts
