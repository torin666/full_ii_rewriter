import vk_api
import os
from vk_api.exceptions import ApiError
from DatabaseManager import DatabaseManager
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class VKPostParser:
    def __init__(self, token, media_manager=None):
        self.vk_session = vk_api.VkApi(token=token)
        self.vk = self.vk_session.get_api()
        self.db = DatabaseManager()
        self.media_manager = media_manager  # Менеджер для загрузки медиа

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
