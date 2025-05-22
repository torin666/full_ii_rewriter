import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from config.settings import BOT_TOKEN
from bot.handlers.source_handlers import router as source_router
from bot.handlers.file_handlers import router as file_router
from database.DatabaseManager import DatabaseManager

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
    # Инициализация базы данных
    db = DatabaseManager()
    db.init_db()
    
    # Создаем временную директорию для файлов
    import os
    if not os.path.exists('temp'):
        os.makedirs('temp')
    
    # Запуск бота
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
