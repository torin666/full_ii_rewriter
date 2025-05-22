import asyncio
import logging
from parse_all_sources import parse_all_sources

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='parser.log'
)

logger = logging.getLogger(__name__)

async def main():
    try:
        await parse_all_sources()
    except Exception as e:
        logger.error(f"Ошибка при парсинге: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Скрипт остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске: {str(e)}") 