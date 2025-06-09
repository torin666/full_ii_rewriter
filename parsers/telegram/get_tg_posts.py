import psycopg2
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from config.settings import TG_PARSER_API_ID, TG_PARSER_API_HASH
from database.DatabaseManager import DatabaseManager
from datetime import datetime
import logging
import os
import time

logger = logging.getLogger(__name__)

class TelegramPostParser:
    def __init__(self):
        # Создаем отдельную сессию для парсера (второй телефон)
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        parser_session_path = os.path.join(project_root, 'parser_user_session')
        
        logger.info(f"Используем отдельную сессию парсера (второй телефон): {parser_session_path}")
        
        # Создаем пользовательский клиент (НЕ бот) для парсинга каналов
        self.client = TelegramClient(parser_session_path, TG_PARSER_API_ID, TG_PARSER_API_HASH)
        self.db = DatabaseManager()
        self._started = False

    async def ensure_started(self):
        """Убеждаемся, что клиент запущен"""
        if not self._started:
            logger.info("Запускаем Telegram клиент")
            await self.client.start()
            self._started = True
            logger.info("Telegram клиент успешно запущен")

    async def get_channel_posts(self, client, channel):
        """Получает последние посты из канала"""
        try:
            # Получаем посты за вчера (для тестирования)
            from datetime import timedelta
            yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
            messages = []
            
            async for message in client.iter_messages(channel, offset_date=yesterday, reverse=True):
                messages.append(message)
            
            return messages
        except Exception as e:
            logger.error(f"Ошибка при получении постов из канала: {e}")
            return []

    async def save_posts_with_retry(self, posts_data, max_retries=5, delay=1):
        """Сохраняет посты с механизмом повторных попыток при блокировке базы"""
        for attempt in range(max_retries):
            try:
                await self.db.save_posts_to_db(posts_data)
                return True
            except psycopg2.OperationalError as e:
                if attempt < max_retries - 1:
                    wait_time = delay * (attempt + 1)  # Увеличиваем время ожидания с каждой попыткой
                    logger.warning(f"Проблема с подключением к базе данных, ожидаем {wait_time} сек перед повторной попыткой")
                    time.sleep(wait_time)
                else:
                    logger.error("Не удалось сохранить данные после всех попыток")
                    raise
            except Exception as e:
                logger.error(f"Неожиданная ошибка при сохранении данных: {e}")
                raise

    async def get_posts(self, channel_username):
        """Получает посты из канала и возвращает их в нужном формате"""
        try:
            logger.info(f"Начинаем парсинг канала {channel_username}")
            
            # Получаем канал
            channel = await self.client.get_entity(channel_username)
            
            # Получаем посты
            posts = await self.get_channel_posts(self.client, channel)
            logger.info(f"Найдено {len(posts)} постов для обработки в канале {channel_username}")
            
            # Форматируем ссылку на канал
            if not channel_username.startswith('http'):
                channel_link = f"https://t.me/{channel_username}"
            else:
                channel_link = channel_username
            
            formatted_posts = []
            
            for post in posts:
                if post.message:
                    formatted_post = {
                        'text': post.message,
                        'post_link': f"{channel_link}/{post.id}",
                        'group_link': channel_link,
                        'date': post.date.strftime("%Y-%m-%d %H:%M:%S"),
                        'likes': 0,  # У Telegram нет публичного API для лайков
                        'comments_count': 0  # У Telegram нет публичного API для комментариев
                    }
                    
                    # Фото пропускаем (отключено для избежания блокировок)
                    # if post.media:
                    #     try:
                    #         # Скачиваем фото
                    #         path = await self.client.download_media(post.media, file=bytes)
                    #         if path:
                    #             formatted_post['photo_url'] = path
                    #     except Exception as e:
                    #         logger.error(f"Ошибка при скачивании медиа: {e}")
                    formatted_post['photo_url'] = None
                    
                    formatted_posts.append(formatted_post)
            
            return formatted_posts
            
        except Exception as e:
            logger.error(f"Ошибка при получении постов из канала {channel_username}: {e}")
            return []

    async def save_posts(self, channel_username):
        """Получает и сохраняет посты из канала"""
        try:
            posts = await self.get_posts(channel_username)
            
            # Сохраняем все посты с механизмом повторных попыток
            if posts:
                await self.save_posts_with_retry(posts)
                logger.info(f"Успешно сохранено {len(posts)} постов")

            logger.info(f"Завершен парсинг канала {channel_username}")
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге канала {channel_username}: {e}")
        finally:
            # НЕ закрываем клиент здесь, это будет сделано в методе stop()
            pass

    async def stop(self):
        """Останавливаем Telegram клиент"""
        if self._started:
            logger.info("Останавливаем Telegram клиент")
            try:
                await self.client.disconnect()
                self._started = False
                logger.info("Telegram клиент успешно остановлен")
            except Exception as e:
                logger.error(f"Ошибка при остановке Telegram клиента: {e}")

    async def __aenter__(self):
        """Контекстный менеджер для автоматического закрытия клиента"""
        await self.ensure_started()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Автоматическое закрытие клиента при выходе из контекстного менеджера"""
        await self.stop() 