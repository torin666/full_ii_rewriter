import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from config.settings import BOT_TOKEN
from bot.handlers.source_handlers import router as source_router
from bot.handlers.file_handlers import router as file_router
from database.DatabaseManager import DatabaseManager
from parsers.parse_all_sources import SourceParser
from autopost_manager import AutopostManager
import os
from config.logging_config import setup_logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='bot.log'
)

logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Регистрация роутеров
dp.include_router(source_router)
dp.include_router(file_router)

async def main():
    try:
        logger.info("Запуск приложения")
        
        # Инициализация базы данных
        logger.info("Инициализация базы данных")
        db = DatabaseManager()
        db.init_db()
        
        # Создаем временную директорию для файлов
        if not os.path.exists('temp'):
            os.makedirs('temp')
            logger.info("Создана временная директория")
        
        # Инициализация парсера
        #logger.info("Инициализация парсера")
        #parser = SourceParser()
        
        # Инициализация автопостинг менеджера
        logger.info("Инициализация автопостинг менеджера")
        try:
            autopost_manager = AutopostManager(bot)
            logger.info("✅ AutopostManager успешно создан")
        except Exception as e:
            logger.error(f"❌ Ошибка создания AutopostManager: {e}")
            raise
        
        # Запускаем парсинг и автопостинг в отдельных тасках
        #logger.info("Запуск периодического парсинга")
        #parsing_task = asyncio.create_task(parser.start_periodic_parsing())
        
        logger.info("Запуск автопостинг менеджера")
        try:
            autopost_task = asyncio.create_task(autopost_manager.start())
            logger.info("✅ AutopostManager task запущен")
        except Exception as e:
            logger.error(f"❌ Ошибка запуска AutopostManager: {e}")
            raise
        
        # Запуск бота
        logger.info("Запуск бота")
        try:
            await dp.start_polling(bot)
        finally:
            # При остановке бота останавливаем все задачи
            logger.info("Остановка автопостинг менеджера")
            await autopost_manager.stop()
            await autopost_task
            
            #logger.info("Остановка парсинга")
            #await parser.stop_parsing()
            #await parsing_task
            logger.info("Все фоновые задачи остановлены")
    
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске приложения: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
