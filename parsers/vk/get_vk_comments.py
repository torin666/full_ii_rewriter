import vk_api
from vk_api.exceptions import ApiError
from config.settings import VK_API_VERSION
from database.DatabaseManager import DatabaseManager

class VKCommentParser:
    def __init__(self, token):
        self.vk_session = vk_api.VkApi(token=token)
        self.vk = self.vk_session.get_api()
        self.db = DatabaseManager()

    def get_post_id(self, post_url):
        try:
            # Извлекаем ID поста из URL
            # Пример URL: https://vk.com/wall-123456_789
            parts = post_url.split('wall-')[1].split('_')
            owner_id = parts[0]
            post_id = parts[1]
            return owner_id, post_id
        except Exception as e:
            print(f"Ошибка при получении ID поста: {e}")
            return None, None

    def get_comments(self, post_url, count=100):
        owner_id, post_id = self.get_post_id(post_url)
        if not owner_id or not post_id:
            return []

        try:
            comments = self.vk.wall.getComments(
                owner_id=f"-{owner_id}",
                post_id=post_id,
                count=count,
                v=VK_API_VERSION
            )
            return comments['items']
        except ApiError as e:
            print(f"Ошибка при получении комментариев: {e}")
            return []

    def save_comments(self, post_url, count=100):
        comments = self.get_comments(post_url, count)
        for comment in comments:
            self.db.add_comment(
                post_id=comment.get('post_id'),
                text=comment.get('text', ''),
                likes=comment.get('likes', {}).get('count', 0)
            ) 