import asyncio
import logging
from datetime import datetime

# Настройка логирования
logger = logging.getLogger(__name__)

class ParserManager:
    def __init__(self):
        self.is_running = False
        self.initialized = False
        
    async def initialize(self):
        """Инициализация парсера"""
        if self.initialized:
            return
            
        try:
            logger.info("🔧 Инициализация парсера")
            self.initialized = True
            logger.info("✅ Парсер успешно инициализирован")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации парсера: {e}")
            raise
    
    async def parse_all_sources(self):
        """Парсинг всех источников"""
        if not self.initialized:
            await self.initialize()
            
        try:
            logger.info("🔄 Начинаем парсинг всех источников")
            start_time = datetime.now()
            
            # Заглушка парсинга
            logger.info("ℹ️ Парсинг источников (заглушка - 0 постов найдено)")
            
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"🎉 Парсинг завершен за {elapsed:.1f}с. Всего новых постов: 0")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при парсинге всех источников: {e}")
    
    async def start_periodic_parsing(self):
        """Запуск периодического парсинга каждые 30 минут"""
        self.is_running = True
        logger.info("🚀 Запуск периодического парсинга (каждые 30 минут)")
        
        # Первый запуск сразу
        await self.parse_all_sources()
        
        while self.is_running:
            try:
                if self.is_running:
                    logger.info("⏰ Ожидание следующего цикла парсинга (30 минут)...")
                    # Ждем 30 минут (1800 секунд)
                    await asyncio.sleep(1800)
                
                if self.is_running:
                    await self.parse_all_sources()
                
            except asyncio.CancelledError:
                logger.info("⏹️ Задача парсинга отменена")
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в периодическом парсинге: {e}")
                if self.is_running:
                    # При ошибке ждем 5 минут перед повтором
                    logger.info("⏰ Ожидание перед повтором (5 минут)...")
                    await asyncio.sleep(300)
    
    async def stop_parsing(self):
        """Остановка парсинга"""
        self.is_running = False
        logger.info("⏹️ Парсинг остановлен")

# Глобальный экземпляр парсера
parser_manager = ParserManager()
