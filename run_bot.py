import asyncio
import logging
from aiogram import Bot, Dispatcher
from config.telegram_config import BOT_TOKEN
from bot.handlers import source_handlers
from utils.telegram_client import TelegramClientManager
from autopost_manager import AutopostManager
from database.DatabaseManager import DatabaseManager

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='bot.log'
)

logger = logging.getLogger(__name__)

async def main():
    # Инициализация бота и диспетчера
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    
    try:
        # Инициализация базы данных
        logger.info("Инициализация базы данных")
        db = DatabaseManager()
        db.init_db()
        
        # Инициализация Telethon клиента для публикации в группы
        await TelegramClientManager.get_client()
        
        # Инициализация автопостинг менеджера
        logger.info("Инициализация автопостинг менеджера")
        try:
            autopost_manager = AutopostManager(bot)
            logger.info("✅ AutopostManager успешно создан")
        except Exception as e:
            logger.error(f"❌ Ошибка создания AutopostManager: {e}")
            raise
        
        # Запуск автопостинг менеджера
        logger.info("Запуск автопостинг менеджера")
        try:
            autopost_task = asyncio.create_task(autopost_manager.start())
            logger.info("✅ AutopostManager task запущен")
        except Exception as e:
            logger.error(f"❌ Ошибка запуска AutopostManager: {e}")
            raise
        
        # Регистрация хендлеров
        dp.include_router(source_handlers.router)
        
        logger.info("Запуск бота")
        try:
            await dp.start_polling(bot)
        finally:
            # При остановке бота останавливаем автопостинг
            logger.info("Остановка автопостинг менеджера")
            await autopost_manager.stop()
            await autopost_task
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")
        raise
    finally:
        # Закрываем соединения
        await TelegramClientManager.close()
        await bot.session.close()
        logger.info("Бот остановлен")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске: {str(e)}") 