from typing import List, Dict
import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

# Настройки бота
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения. Создайте файл .env и добавьте BOT_TOKEN=ваш_токен")

# Настройки OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY не найден в переменных окружения. Добавьте OPENAI_API_KEY=ваш_ключ_api в файл .env")

# Настройки базы данных
DATABASE_PATH = "sources.db"

# Список доступных тематик
THEMES: List[str] = [
    "Новости",
    "Политика",
    "Экономика",
    "Технологии",
    "Спорт",
    "Культура",
    "Искусство",
    "Животные",
    "Авто",
    "Образование",
    "Кино"
]

# Настройки парсеров
VK_API_VERSION = "5.131"
TG_API_ID = os.getenv("TG_API_ID")
TG_API_HASH = os.getenv("TG_API_HASH")
TG_SESSION_PATH = os.getenv('TG_SESSION_PATH', 'bot_session')  # Путь к файлу сессии

# Настройки Telegram API для парсера (отдельное приложение)
TG_PARSER_API_ID = os.getenv("TG_PARSER_API_ID")
TG_PARSER_API_HASH = os.getenv("TG_PARSER_API_HASH")

VK_TOKEN = os.getenv("VK_TOKEN")
# Настройки валидации
ALLOWED_DOMAINS = ["vk.com", "t.me"]

# Настройки обработки файлов
ALLOWED_FILE_TYPES = [".txt", ".xlsx", ".xls"]
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5MB

# Яндекс.Диск настройки
YANDEX_DISK_TOKEN = os.getenv('YANDEX_DISK_TOKEN')

# Настройки для хранения фотографий
TEMP_PHOTO_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'temp', 'photos')
os.makedirs(TEMP_PHOTO_DIR, exist_ok=True)

# Настройки для DALL-E
DALLE_DEFAULT_MODEL = "dall-e-3"  # или "dall-e-2" если нужно использовать более старую версию 