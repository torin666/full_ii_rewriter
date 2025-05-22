import vk_api
from vk_api.exceptions import ApiError
from config.settings import VK_API_VERSION
from database.DatabaseManager import DatabaseManager

class VKPostParser:
    def __init__(self, token):
        self.vk_session = vk_api.VkApi(token=token)
        self.vk = self.vk_session.get_api()
        self.db = DatabaseManager()

    def get_group_id(self, group_name):
        try:
            group_info = self.vk.groups.getById(group_id=group_name)
            return group_info[0]['id']
        except ApiError as e:
            print(f"Ошибка при получении ID группы: {e}")
            return None

    def get_posts(self, group_name, count=100):
        group_id = self.get_group_id(group_name)
        if not group_id:
            return []

        try:
            posts = self.vk.wall.get(
                owner_id=f"-{group_id}",
                count=count,
                v=VK_API_VERSION
            )
            return posts['items']
        except ApiError as e:
            print(f"Ошибка при получении постов: {e}")
            return []

    def save_posts(self, group_name, count=100):
        posts = self.get_posts(group_name, count)
        for post in posts:
            self.db.add_post(
                source_url=f"https://vk.com/wall-{group_name}_{post['id']}",
                text=post.get('text', ''),
                likes=post.get('likes', {}).get('count', 0),
                comments_count=post.get('comments', {}).get('count', 0)
            ) 