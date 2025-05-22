from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from config.settings import TG_API_ID, TG_API_HASH
from database.DatabaseManager import DatabaseManager

class TelegramPostParser:
    def __init__(self):
        self.client = TelegramClient('anon', TG_API_ID, TG_API_HASH)
        self.db = DatabaseManager()

    async def get_channel_posts(self, channel_username, limit=100):
        try:
            channel = await self.client.get_entity(channel_username)
            posts = await self.client(GetHistoryRequest(
                peer=channel,
                limit=limit,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))
            return posts.messages
        except Exception as e:
            print(f"Ошибка при получении постов из канала: {e}")
            return []

    async def save_posts(self, channel_username, limit=100):
        posts = await self.get_channel_posts(channel_username, limit)
        for post in posts:
            if post.message:  # Проверяем, что пост содержит текст
                self.db.add_post(
                    source_url=f"https://t.me/{channel_username}/{post.id}",
                    text=post.message,
                    likes=post.reactions.count if hasattr(post, 'reactions') else 0,
                    comments_count=post.replies.replies if hasattr(post, 'replies') else 0
                )

    async def start(self):
        await self.client.start()

    async def stop(self):
        await self.client.disconnect() 