import asyncio
import logging
from database.DatabaseManager import DatabaseManager
from parsers.vk.get_vk_posts import VKPostParser
from parsers.vk.get_vk_comments import VKCommentParser
from parsers.telegram.get_tg_posts import TelegramPostParser
from config.settings import VK_TOKEN

logger = logging.getLogger(__name__)

class SourceParser:
    def __init__(self):
        self.db = DatabaseManager()
        self.vk_parser = VKPostParser(VK_TOKEN)
        self.vk_comment_parser = VKCommentParser(VK_TOKEN)
        self.tg_parser = TelegramPostParser()

    async def parse_all_sources(self):
        """Парсинг всех источников"""
        try:
            # Получаем все активные источники
            sources = self.db.get_active_sources()
            
            for source in sources:
                if "vk.com" in source['source_url']:
                    # Парсим VK
                    group_name = source['source_url'].split("/")[-1]
                    self.vk_parser.save_posts(group_name)
                elif "t.me" in source['source_url']:
                    # Парсим Telegram
                    channel_name = source['source_url'].split("/")[-1]
                    await self.tg_parser.start()
                    await self.tg_parser.save_posts(channel_name)
                    await self.tg_parser.stop()
            
            logger.info("Парсинг источников завершен успешно")
        except Exception as e:
            logger.error(f"Ошибка при парсинге источников: {str(e)}")

    async def parse_comments(self, post_url):
        """Парсинг комментариев к посту"""
        try:
            if "vk.com" in post_url:
                self.vk_comment_parser.save_comments(post_url)
            # Для Telegram пока не реализовано
        except Exception as e:
            logger.error(f"Ошибка при парсинге комментариев: {str(e)}")

async def main():
    parser = SourceParser()
    await parser.parse_all_sources()

if __name__ == "__main__":
    asyncio.run(main()) 