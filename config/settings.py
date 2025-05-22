from typing import List, Dict

# Настройки бота
BOT_TOKEN = "YOUR_BOT_TOKEN"

# Настройки базы данных
DATABASE_PATH = "sources.db"

# Список доступных тематик
THEMES: List[str] = [
    "Новости",
    "Политика и общество",
    "Экономика и бизнес",
    "Технологии и наука",
    "Спорт и ЗОЖ",
    "Культура",
    "Искусство и творчество",
    "Животные и природа",
    "Авто и мото",
    "Образование",
    "Фильмы, сериалы и анимация"
]

# Настройки парсеров
VK_API_VERSION = "5.131"
TG_API_ID = "YOUR_API_ID"
TG_API_HASH = "YOUR_API_HASH"

# Настройки валидации
ALLOWED_DOMAINS = ["vk.com", "t.me"]

# Настройки обработки файлов
ALLOWED_FILE_TYPES = [".txt", ".xlsx", ".xls"]
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5MB 