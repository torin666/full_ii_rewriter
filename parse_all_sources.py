import vk_api
import logging
import pandas as pd
from datetime import datetime, timedelta
from DatabaseManager import Database_Manager
from parse_vk_link import parse_vk_link
from get_vk_comments import get_vk_comments
from get_vk_posts import get_vk_posts
from get_tg_posts import telegram_client, parse_telegram_sources
from environs import Env
import yadisk
import os
from yadisk_utils import upload_to_yadisk_and_get_url

env = Env()
env.read_env()
vk_token = env('vk_token')

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('parser.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

YANDEX_DISK_TOKEN = os.getenv('yandex_api')
y = yadisk.YaDisk(token=YANDEX_DISK_TOKEN)

async def upload_to_yadisk_and_get_url(local_path, remote_folder="/news_photos/"):
    remote_path = remote_folder + os.path.basename(local_path)
    y.upload(local_path, remote_path, overwrite=True)
    # Сделать файл публичным
    if not y.is_published(remote_path):
        y.publish(remote_path)
    # Получить публичную ссылку
    return y.get_download_link(remote_path)

# Функции парсера (полные версии)
def get_today_timestamps():  # Видимо, устанавливаем время парснинга
    today = datetime.now()
    return today.strftime("%d.%m.%Y"), today.strftime("%d.%m.%Y")  # Возвращаем дату в формате ДД.ММ.ГГГГ


# Модифицируем функцию parse_all_sources, чтобы она возвращала данные для Excel
async def parse_all_sources():
    """Полный цикл парсинга с улучшенным логированием"""
    start_time, end_time = get_today_timestamps()
    logger.info(
        f"\n{'=' * 50}\nЗАПУСК ПАРСЕРА | {datetime.now()}\nПериод: {start_time} - {end_time}\n{'=' * 50}")

    vk_session = vk_api.VkApi(token=vk_token)
    db_manager = Database_Manager()
    sources = db_manager.get_links()

    logger.info(f"Источники для парсинга ({len(sources)}):\n" + "\n".join(sources))

    all_comments = []
    all_posts = []
    parsed_links = set()

    # Запускаем клиент Telegram
    await telegram_client.start()
    if not await telegram_client.is_user_authorized():
        logger.error("❌ Требуется авторизация пользователя Telegram!")
        await telegram_client.disconnect()
        return pd.DataFrame()

    try:
        for i, link in enumerate(sources, 1):
            if link in parsed_links:
                continue

            logger.info(f"\nОбработка источника {i}/{len(sources)}: {link}")

            try:
                if 'vk.com' in link:
                    if 'vk.com/club' in link:
                        owner_id = -int(link.split('vk.com/club')[1])
                        post_id = None
                    elif 'vk.com/public' in link:
                        owner_id = -int(link.split('vk.com/public')[1])
                        post_id = None
                    elif 'vk.com/id' in link:
                        owner_id = int(link.split('vk.com/id')[1])
                        post_id = None
                    else:
                        owner_id, post_id = parse_vk_link(link)
                    logger.info(f"VK {'пост' if post_id else 'группа'}: owner_id={owner_id}, post_id={post_id}")

                    if owner_id:
                        if post_id:
                            logger.info("Парсинг комментариев к конкретному посту...")
                            comments = get_vk_comments(vk_session, owner_id, post_id, start_time, end_time)
                            all_comments.extend(comments)
                            logger.info(f"Получено комментариев: {len(comments)}")

                            post_info = vk_session.method('wall.getById', {'posts': f"{owner_id}_{post_id}"})
                            if post_info and post_info[0]:
                                post_date = datetime.fromtimestamp(post_info[0]['date']).strftime("%d.%m.%Y")
                                all_posts.append({
                                    'group_link': f"https://vk.com/public{abs(owner_id)}",
                                    'post_link': link,
                                    'text': post_info[0]['text'],
                                    'date': post_date,
                                    'likes': post_info[0].get('likes', {}).get('count', 0),
                                    'views': post_info[0].get('views', {}).get('count', 0),
                                    'comments_count': len(comments),
                                    'comments_likes': sum(c['likes'] for c in comments),
                                    'photo_url': post_info[0].get('photo_url')
                                })
                                logger.info("Данные поста успешно добавлены")
                        else:
                            logger.info("Парсинг всех постов группы...")
                            posts = get_vk_posts(vk_session, owner_id, start_time, end_time)
                            logger.info(f"Найдено постов: {len(posts)}")

                            for post in posts:
                                post_id_local = post['post_link'].split('_')[-1]
                                logger.info(f"Парсинг комментариев к посту {post_id_local}...")
                                comments = get_vk_comments(vk_session, owner_id, post_id_local, start_time, end_time)
                                all_comments.extend(comments)

                                post_date = datetime.fromtimestamp(post['date']).strftime("%d.%m.%Y") if isinstance(post['date'], int) else post['date']
                                all_posts.append({
                                    'group_link': f"https://vk.com/public{abs(owner_id)}",
                                    'post_link': post['post_link'],
                                    'text': post['text'],
                                    'date': post_date,
                                    'likes': post['likes'],
                                    'views': post['views'],
                                    'comments_count': len(comments),
                                    'comments_likes': sum(c['likes'] for c in comments),
                                    'photo_url': post.get('photo_url')
                                })
                                logger.info(f"Добавлен пост {post_id_local} с {len(comments)} комментариями")

                elif 't.me' in link or 'telegram.me' in link:
                    logger.info("Парсинг Telegram источника...")
                    telegram_posts = await parse_telegram_sources([link])
                    if telegram_posts:
                        for post in telegram_posts:
                            all_posts.append({
                                'group_link': post['group_link'],
                                'post_link': post['post_link'],
                                'text': post['text'],
                                'date': post['date'],  # Теперь это строка в формате ДД.ММ.ГГГГ
                                'likes': post['likes'],
                                'views': post['views'],
                                'comments_count': 0,
                                'comments_likes': 0,
                                'photo_url': post.get('photo_url'),
                                'additional_photos': post.get('additional_photos', [])
                            })
                        logger.info(f"Добавлено {len(telegram_posts)} постов из Telegram")

                parsed_links.add(link)
                logger.info(f"✅ Источник {i} успешно обработан")

            except Exception as e:
                logger.error(f"❌ Ошибка при обработке источника {link}: {str(e)}")
                continue

    finally:
        if telegram_client.is_connected():
            await telegram_client.disconnect()
            logger.info("🔌 Клиент Telegram отключен")

    # Сортировка и сохранение
    all_posts.sort(key=lambda x: x['likes'], reverse=True)

    logger.info("Сохранение в БД...")
    db_manager.save_posts_to_db(all_posts)
    logger.info(f"Сохранено постов: {len(all_posts)}")

    logger.info("\n" + "=" * 50 + "\nПАРСЕР УСПЕШНО ЗАВЕРШИЛ РАБОТУ\n" + "=" * 50)

    return pd.DataFrame(all_posts)