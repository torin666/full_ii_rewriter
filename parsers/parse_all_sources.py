import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any
from database.DatabaseManager import DatabaseManager
from parsers.telegram.get_tg_posts import TelegramPostParser
from parsers.vk.get_vk_posts import VKPostParser
from parsers.vk.get_vk_comments import VKCommentParser
from config.settings import VK_TOKEN

logger = logging.getLogger(__name__)

class SourceParser:
    def __init__(self):
        self.db = DatabaseManager()
        self.vk_parser = VKPostParser(VK_TOKEN)
        self.vk_comment_parser = VKCommentParser(VK_TOKEN)
        self.parse_interval = 300  # 30 минут (1800 секунд)
        
    async def initialize_db(self):
        """Инициализация базы данных"""
        await self.db.init_pool()

    async def parse_telegram_source(self, source: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Парсит один Telegram источник"""
        try:
            channel_name = source['link'].split('/')[-1]
            logger.info(f"Начинаем парсинг Telegram канала: {channel_name}")
            
            # Создаем новый экземпляр парсера для каждого источника
            async with TelegramPostParser() as tg:
                logger.info(f"Получаем посты из канала {channel_name}...")
                posts = await tg.get_posts(channel_name)
                if posts:
                    logger.info(f"Получено {len(posts)} постов из {channel_name}")
                    # Преобразуем формат даты
                    for post in posts:
                        if isinstance(post['date'], str):
                            date_obj = datetime.strptime(post['date'], "%Y-%m-%d %H:%M:%S")
                            post['date'] = date_obj.strftime("%d.%m.%Y")
                    return posts
                else:
                    logger.info(f"Постов не найдено в канале {channel_name}")
                    return []
        except Exception as e:
            logger.error(f"Ошибка при парсинге Telegram источника {source['link']}: {e}", exc_info=True)
        return []

    async def parse_sources(self):
        """Парсит все источники"""
        try:
            # Проверяем состояние пула соединений
            logger.info(f"Состояние пула БД: {self.db._pool}")
            
            if not self.db._pool:
                logger.error("Пул соединений с БД не инициализирован!")
                await self.db.init_pool()
                logger.info("Пул соединений инициализирован")
            
            # Получаем список активных источников
            logger.info("Получение списка активных источников...")
            sources = await self.db.get_active_sources()
            logger.info(f"Найдено источников: {len(sources)}")
            
            if not sources:
                logger.warning("Нет активных источников для парсинга")
                return
            
            # Логируем найденные источники
            for i, source in enumerate(sources):
                logger.info(f"Источник {i+1}: {source.get('link', 'Нет ссылки')} - темы: {source.get('themes', 'Нет тем')}")
            
            all_posts = []
            telegram_sources = []
            vk_sources = []
            
            # Разделяем источники по типу
            for source in sources:
                if 't.me' in source['link']:
                    telegram_sources.append(source)
                elif 'vk.com' in source['link']:
                    vk_sources.append(source)
                else:
                    logger.warning(f"Неподдерживаемый источник: {source['link']}")

            logger.info(f"Telegram источников: {len(telegram_sources)}")
            logger.info(f"VK источников: {len(vk_sources)}")

            # Параллельно обрабатываем Telegram источники
            if telegram_sources:
                logger.info("Начинаем парсинг Telegram источников...")
                telegram_tasks = [self.parse_telegram_source(source) for source in telegram_sources]
                telegram_results = await asyncio.gather(*telegram_tasks, return_exceptions=True)
                for i, result in enumerate(telegram_results):
                    if isinstance(result, list):  # Проверяем, что это не исключение
                        logger.info(f"Telegram источник {i+1}: получено {len(result)} постов")
                        all_posts.extend(result)
                    elif isinstance(result, Exception):
                        logger.error(f"Ошибка в Telegram источнике {i+1}: {result}")

            # Обрабатываем VK источники
            if vk_sources:
                logger.info("Начинаем парсинг VK источников...")
                for i, source in enumerate(vk_sources):
                    try:
                        group_name = source['link'].split('/')[-1]
                        logger.info(f"Парсим VK группу: {group_name}")
                        posts = self.vk_parser.get_posts(group_name)
                        if posts:
                            # Преобразуем формат даты для VK
                            for post in posts:
                                if isinstance(post['date'], int):
                                    date_obj = datetime.fromtimestamp(post['date'])
                                    post['date'] = date_obj.strftime("%d.%m.%Y")
                            logger.info(f"VK источник {i+1}: получено {len(posts)} постов")
                            all_posts.extend(posts)
                        else:
                            logger.info(f"VK источник {i+1}: постов не найдено")
                    except Exception as e:
                        logger.error(f"Ошибка при парсинге VK источника {source['link']}: {e}")
                        continue

            logger.info(f"Всего собрано постов: {len(all_posts)}")
            
            if all_posts:
                logger.info("Сохраняем посты в базу данных...")
                await self.db.save_posts_to_db(all_posts)
                logger.info(f"Сохранено {len(all_posts)} постов")
            else:
                logger.info("Новых постов для сохранения не найдено")
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге источников: {e}", exc_info=True)

    async def start_periodic_parsing(self):
        """Запускает периодический парсинг источников"""
        while True:
            try:
                logger.info("Начало парсинга источников")
                await self.parse_sources()
                logger.info(f"Парсинг завершен. Следующий парсинг через {self.parse_interval} секунд")
                await asyncio.sleep(self.parse_interval)
            except Exception as e:
                logger.error(f"Ошибка в цикле парсинга: {e}")
                await asyncio.sleep(60)  # При ошибке ждем минуту перед следующей попыткой

async def main():
    parser = SourceParser()
    try:
        await parser.initialize_db()
        await parser.start_periodic_parsing()
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")

if __name__ == "__main__":
    asyncio.run(main()) 