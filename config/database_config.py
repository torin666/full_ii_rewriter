import os
from dotenv import load_dotenv

load_dotenv(override=True)

DB_HOST = os.getenv('DB_HOST', '80.74.24.141')
DB_PORT = int(os.getenv('DB_PORT', 5432))
DB_NAME = os.getenv('DB_NAME', 'mydb')
DB_USER = os.getenv('USER_DB')
DB_PASSWORD = os.getenv('USER_PWD')

# Проверка наличия необходимых переменных окружения
if not all([DB_USER, DB_PASSWORD]):
    raise ValueError(
        "Отсутствуют необходимые переменные окружения для подключения к базе данных. "
        "Убедитесь, что установлены USER_DB и USER_PWD"
    ) 