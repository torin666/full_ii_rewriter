import os
from dotenv import load_dotenv

load_dotenv()

# API для бота (публикация в группы)
TELEGRAM_API_ID = int(os.getenv('TG_API_ID', '22513001'))
TELEGRAM_API_HASH = os.getenv('TG_API_HASH', '1ef9a7c25dddcca43434ec42a0ea6518')
BOT_TOKEN = os.getenv('BOT_TOKEN', '8094964715:AAGGev7H1dUXA9COzSkSU4j6o6RPIlMw7Wc') 