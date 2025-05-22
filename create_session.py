from telethon import TelegramClient
from environs import Env
import asyncio

# Загрузка переменных окружения
env = Env()
env.read_env()

# Инициализация клиента Telegram
client = TelegramClient(
    'user_session',
    int(env('telegram_api_id')),
    env('telegram_api_hash'),
    system_version="4.16.30-vxCustom"
)

async def main():
    print("🚀 Запуск создания сессии...")
    
    # Подключаемся к Telegram
    await client.start()
    
    # Проверяем авторизацию
    if await client.is_user_authorized():
        print("✅ Сессия успешно создана!")
        print("📱 Теперь вы можете использовать файл user_session.session на сервере")
    else:
        print("❌ Ошибка авторизации!")
    
    # Отключаемся
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main()) 