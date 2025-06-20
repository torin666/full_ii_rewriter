
class TGPostParser:
    def __init__(self, api_id=None, api_hash=None, session_name='tg_parser'):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_name = session_name
        self.client = None
        self.db = DatabaseManager()

    async def init_client(self):
        """Инициализация Telegram клиента"""
        try:
            if not self.api_id or not self.api_hash:
                logger.error("API ID или API Hash не установлены")
                return False
                
            self.client = TelegramClient(self.session_name, self.api_id, self.api_hash)
            await self.client.start()
            logger.info("Telegram клиент инициализирован")
            return True
        except Exception as e:
            logger.error(f"Ошибка инициализации Telegram клиента: {e}")
            return False

    async def get_posts(self, channel_username):
        """Получение постов из Telegram канала"""
        if not self.client:
            if not await self.init_client():
                return []

        try:
            logger.info(f"Начинаем получение постов из канала {channel_username}")
            
            # Убираем @ если есть
            if channel_username.startswith('@'):
                channel_username = channel_username[1:]
            
            # Вчерашняя дата (учитываем timezone)
            import pytz
            tz = pytz.UTC
            yesterday = datetime.now(tz) - timedelta(days=1)
            yesterday_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            # Получаем сущность канала
            entity = await self.client.get_entity(channel_username)
            
            # Получаем сообщения за вчера
            messages = []
            async for message in self.client.iter_messages(entity, offset_date=yesterday_end, limit=100):
                if message.date < yesterday_start:
                    break
                if yesterday_start <= message.date <= yesterday_end:
                    messages.append(message)
            
            logger.info(f"Найдено {len(messages)} сообщений за вчера в канале {channel_username}")
            
            # Преобразуем сообщения в нужный формат
            formatted_posts = []
            for message in messages:
                # Получаем фото если есть
                photo_url = None
                if message.photo:
                    try:
                        # Сохраняем фото и получаем URL (упрощенная версия)
                        photo_url = f"tg_photo_{message.id}.jpg"
                    except Exception as e:
                        logger.warning(f"Не удалось обработать фото: {e}")

                post_data = {
                    'group_link': f"https://t.me/{channel_username}",
                    'post_link': f"https://t.me/{channel_username}/{message.id}",
                    'text': message.text or '',
                    'date': message.date.strftime("%d.%m.%Y"),
                    'likes': 0,  # Безопасное получение реакций
                    'comments_count': message.replies.replies if message.replies else 0,
                    'photo_url': photo_url
                }
                
                # Безопасное получение реакций
