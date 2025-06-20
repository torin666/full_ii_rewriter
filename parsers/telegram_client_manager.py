import logging
import os
from telethon import TelegramClient, errors
from telethon.tl.types import ChatAdminRights, User, Chat, Channel
import asyncio

logger = logging.getLogger(__name__)

# Глобальный клиент как в рабочем коде
telegram_client = TelegramClient(
    'tg_parser',
    int(os.getenv('TG_PARSER_API_ID', '25038832')),
    os.getenv('TG_PARSER_API_HASH', 'de5237d9ff5c7da77200b9d55f3eee1a'),
    system_version="4.16.30-vxCustom"
)

class TelegramClientManager:
    def __init__(self):
        self.client = telegram_client
        self._client_started = False
    
    async def start_client(self):
        """Запуск клиента"""
        if not self._client_started:
            await self.client.start()
            self._client_started = True
            logger.info("Telegram клиент инициализирован")
    
    async def check_bot_admin_rights(self, chat_link: str, bot_username: str) -> bool:
        """Проверка прав администратора бота в чате"""
        try:
            await self.start_client()
            
            # Получаем чат по ссылке
            chat = await self.client.get_entity(chat_link)
            chat_id = chat.id
            
            # Получаем информацию о боте
            bot = await self.client.get_entity(bot_username)
            bot_id = bot.id
            
            # Проверяем права участника (бота)
            try:
                participant = await self.client.get_permissions(chat_id, bot_id)
                
                # Проверяем, является ли бот администратором
                if participant.is_admin:
                    logger.info(f"Бот {bot_username} является администратором в чате {chat_link}")
                    return True
                else:
                    logger.warning(f"Бот {bot_username} НЕ является администратором в чате {chat_link}")
                    return False
                    
            except Exception as e:
                logger.error(f"Ошибка при проверке прав участника: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при проверке прав администратора: {e}")
            return False
    
    async def disconnect(self):
        """Отключение клиента"""
        if self._client_started and self.client.is_connected():
            await self.client.disconnect()
            self._client_started = False
            logger.info("Telegram клиент отключен") 