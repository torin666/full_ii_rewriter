#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import logging
import os
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from bot.handlers import source_handlers
from autopost_manager import AutopostManager
from database.DatabaseManager import DatabaseManager
from utils.telegram_client import TelegramClientManager

# Загружаем переменные окружения
from dotenv import load_dotenv
load_dotenv(override=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

async def main():
    """Основная функция запуска бота"""
    
    # Получаем токен бота
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN не найден в переменных окружения")
        return
    
    # Инициализация бота и диспетчера
    bot = Bot(token=BOT_TOKEN)
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    
    # Инициализация базы данных
    db = DatabaseManager()
    try:
        db.init_db()
        logger.info("База данных инициализирована")
    except Exception as e:
        logger.error(f"Ошибка инициализации БД: {e}")
        return
    
    # Регистрация обработчиков
    dp.include_router(source_handlers.router)
    
    # Инициализация Telegram клиента
    telegram_manager = TelegramClientManager()
    
    # Инициализация и запуск автопостинга
    autopost_manager = AutopostManager(bot, db, telegram_manager)
    autopost_task = asyncio.create_task(autopost_manager.start_autopost_loop())
    
    try:
        logger.info("🚀 Бот запущен")
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logger.info("👋 Получен сигнал остановки")
    finally:
        # Остановка автопостинга
        await autopost_manager.stop()
        autopost_task.cancel()
        
        # Остановка Telegram клиента
        await telegram_manager.stop()
        
        # Закрытие бота
        await bot.session.close()
        logger.info("✅ Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main()) 