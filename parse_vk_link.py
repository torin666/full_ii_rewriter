import logging
import re
import vk_api
from environs import Env

env = Env()
vk_token = env('vk_token')


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('parser.log')
    ]
)
logger = logging.getLogger(__name__)

def parse_vk_link(link):
    try:
        # Удаляем параметры и лишние части URL
        clean_link = link.split('?')[0].lower().strip()

        # 1. Проверяем запись на стене (например, wall-12345_67890)
        wall_match = re.search(r'wall(-?\d+)_(\d+)', clean_link)
        if wall_match:
            return int(wall_match.group(1)), int(wall_match.group(2))

        # 2. Извлекаем username (например, "" или "id123456789")
        username_match = re.search(r'vk\.com/([^/]+)', clean_link)
        if not username_match:
            return None, None

        username = username_match.group(1)

        # 3. Если это ID пользователя (например, id123456789)
        if username.startswith('id') and username[2:].isdigit():
            return int(username[2:]), None

        # 4. Если это короткое имя (например, )
        try:
            vk_session = vk_api.VkApi(token=vk_token)

            # Пробуем получить ID пользователя по screen_name
            user_info = vk_session.method('users.get', {
                'user_ids': username,  # Например, 'rektorkomissarov'
                'fields': 'screen_name'
            })

            if user_info and 'id' in user_info[0]:
                return user_info[0]['id'], None

            # Пробуем получить ID группы по screen_name
            group_info = vk_session.method('groups.getById', {
                'group_ids': username,
                'fields': 'screen_name'
            })

            if group_info and 'id' in group_info[0]:
                return -group_info[0]['id'], None

        except Exception as e:
            logger.error(f"VK API Error: {e}")

        return None, None
    except Exception as e:
        logger.error(f"Error in parse_vk_link: {e}")
        return None, None