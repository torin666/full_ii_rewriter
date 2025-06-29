from telethon import TelegramClient
from dotenv import load_dotenv
import os

# Загружаем переменные окружения из .env файла
load_dotenv()

TG_API_ID = os.getenv("TG_API_ID")
TG_API_HASH = os.getenv("TG_API_HASH")

async def create_session():
    """Создание сессии для Telegram клиента"""
    try:
        client = TelegramClient('anon', TG_API_ID, TG_API_HASH)
        await client.start()
        await client.disconnect()
        print("Сессия успешно создана!")
    except Exception as e:
        print(f"Ошибка при создании сессии: {str(e)}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(create_session()) 