import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

VK_APP_ID = os.getenv("VK_APP_ID")  # ID приложения ВКонтакте
VK_APP_SECRET = os.getenv("VK_APP_SECRET")  # Секретный ключ приложения
VK_REDIRECT_URI = "https://8b24-80-74-24-141.ngrok-free.app/vk_callback"  # URI для редиректа после авторизации

# Права доступа, которые запрашиваем у пользователя
VK_SCOPE = [
    'wall',  # Доступ к стене
    'groups',  # Доступ к группам
    'offline'  # Бессрочный токен
] 