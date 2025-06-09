import vk_api
from vk_api.exceptions import ApiError
from config.settings import VK_API_VERSION
from database.DatabaseManager import DatabaseManager
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class VKPostParser:
    def __init__(self, token):
        self.vk_session = vk_api.VkApi(token=token)
        self.vk = self.vk_session.get_api()
        self.db = DatabaseManager()

    def get_owner_info(self, name):
        """Получает информацию о владельце (группа или пользователь)"""
        try:
            # Очищаем URL от параметров запроса и протокола
            if '?' in name:
                name = name.split('?')[0]
            if '//' in name:
                name = name.split('//')[-1]
            if 'vk.com/' in name:
                name = name.replace('vk.com/', '')
                
            logger.info(f"Получаем информацию о: {name}")
            
            # Сначала пробуем как группу
            try:
                if name.isdigit():
                    group_info = self.vk.groups.getById(group_id=name)
                else:
                    group_info = self.vk.groups.getById(group_ids=name)
                return {'type': 'group', 'id': -group_info[0]['id']}  # Минус для групп
            except ApiError:
                # Если не группа, пробуем как пользователя
                try:
                    user_info = self.vk.users.get(user_ids=name)
                    if user_info:
                        return {'type': 'user', 'id': user_info[0]['id']}
                except ApiError as e:
                    logger.error(f"Ошибка при получении информации о пользователе {name}: {e}")
                    return None
                    
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении информации о {name}: {e}")
            return None

    def get_posts(self, name):
        owner_info = self.get_owner_info(name)
        if not owner_info:
            logger.error(f"Не удалось получить информацию о {name}, пропускаем парсинг")
            return []

        try:
            logger.info(f"Начинаем получение постов из {'группы' if owner_info['type'] == 'group' else 'страницы'} {name}")
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%d.%m.%Y")
            logger.info(f"Ищем посты за {yesterday}")
            
            # Получаем последние 20 постов
            posts = self.vk.wall.get(
                owner_id=owner_info['id'],
                count=20,
                v=VK_API_VERSION
            )
            
            # Фильтруем только вчерашние посты
            yesterday_posts = []
            for post in posts['items']:
                post_date = datetime.fromtimestamp(post['date']).strftime("%d.%m.%Y")
                if post_date == yesterday:
                    yesterday_posts.append(post)
            
            logger.info(f"Найдено {len(yesterday_posts)} постов за вчера в {'группе' if owner_info['type'] == 'group' else 'на странице'} {name}")
            
            # Преобразуем посты в нужный формат
            formatted_posts = []
            for post in yesterday_posts:
                # Получаем фото из поста, если есть
                photo_url = None
                if 'attachments' in post:
                    for attachment in post['attachments']:
                        if attachment['type'] == 'photo':
                            sizes = attachment['photo']['sizes']
                            photo_url = max(sizes, key=lambda x: x['height'])['url']
                            break

                post_data = {
                    'group_link': f"https://vk.com/{name}",
                    'post_link': f"https://vk.com/wall{owner_info['id']}_{post['id']}",
                    'text': post.get('text', ''),
                    'date': datetime.fromtimestamp(post['date']).strftime("%d.%m.%Y"),
                    'likes': post.get('likes', {}).get('count', 0),
                    'comments_count': post.get('comments', {}).get('count', 0),
                    'photo_url': photo_url
                }
                formatted_posts.append(post_data)
            
            return formatted_posts
            
        except ApiError as e:
            logger.error(f"Ошибка при получении постов из {'группы' if owner_info['type'] == 'group' else 'страницы'} {name}: {e}")
            return []

    def save_posts(self, name):
        posts = self.get_posts(name)
        if posts:
            self.db.save_posts_to_db(posts)
            logger.info(f"Сохранено {len(posts)} постов из {name}") 