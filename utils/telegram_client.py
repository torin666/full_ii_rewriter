from telethon import TelegramClient
import logging
import asyncio
from config.telegram_config import TELEGRAM_API_ID, TELEGRAM_API_HASH, BOT_TOKEN
import os
import aiohttp
import io
from typing import Optional, Dict
from datetime import datetime

logger = logging.getLogger(__name__)

class TelegramClientManager:
    _instance = None
    _clients: Dict[str, TelegramClient] = {}
    _lock = asyncio.Lock()
    _main_client: Optional[TelegramClient] = None
    _session_counter = 0
    _cleanup_task = None

    @classmethod
    async def initialize(cls):
        """Инициализация основного клиента"""
        if cls._main_client is None:
            try:
                cls._main_client = TelegramClient(
                    'bot_session',
                    api_id=TELEGRAM_API_ID,
                    api_hash=TELEGRAM_API_HASH
                )
                
                if not cls._main_client.is_connected():
                    await cls._main_client.start(bot_token=BOT_TOKEN)
                    
                logger.info("Основной Telethon клиент успешно инициализирован")
                
                # Запускаем задачу очистки
                if not cls._cleanup_task:
                    cls._cleanup_task = asyncio.create_task(cls._cleanup_old_sessions())
            except Exception as e:
                logger.error(f"Ошибка при инициализации основного Telethon клиента: {str(e)}")
                raise

    @classmethod
    async def get_client(cls, unique_session=False) -> TelegramClient:
        """
        Получить клиент Telethon
        
        Args:
            unique_session (bool): Если True, создает новую уникальную сессию
        """
        if unique_session:
            async with cls._lock:
                cls._session_counter += 1
                session_name = f'temp_session_{datetime.now().strftime("%Y%m%d_%H%M%S")}_{cls._session_counter}'
                
                try:
                    client = TelegramClient(
                        session_name,
                        api_id=TELEGRAM_API_ID,
                        api_hash=TELEGRAM_API_HASH
                    )
                    
                    await client.start(bot_token=BOT_TOKEN)
                    cls._clients[session_name] = client
                    
                    logger.info(f"Создан временный клиент: {session_name}")
                    return client
                    
                except Exception as e:
                    logger.error(f"Ошибка при создании временного клиента: {str(e)}")
                    if os.path.exists(f"{session_name}.session"):
                        os.remove(f"{session_name}.session")
                    raise
        else:
            if cls._main_client is None:
                await cls.initialize()
            return cls._main_client

    @classmethod
    async def close_temp_client(cls, client: TelegramClient):
        """Закрыть временный клиент и удалить его файл сессии"""
        if client and client.session.filename != 'bot_session.session':
            try:
                session_name = os.path.splitext(os.path.basename(client.session.filename))[0]
                
                # Правильно закрываем все соединения
                if client.is_connected():
                    await client.disconnect()
                await asyncio.sleep(0.1)  # Даем время на закрытие соединений
                
                # Удаляем файл сессии
                if os.path.exists(f"{session_name}.session"):
                    os.remove(f"{session_name}.session")
                
                # Удаляем клиент из словаря
                if session_name in cls._clients:
                    del cls._clients[session_name]
                    
                logger.info(f"Временный клиент {session_name} успешно закрыт и удален")
                
            except Exception as e:
                logger.error(f"Ошибка при закрытии временного клиента: {str(e)}")

    @classmethod
    async def _cleanup_old_sessions(cls):
        """Периодическая очистка старых сессий"""
        while True:
            try:
                await asyncio.sleep(300)  # Проверяем каждые 5 минут
                
                current_time = datetime.now()
                sessions_to_close = []
                
                for session_name, client in cls._clients.items():
                    try:
                        # Извлекаем время создания из имени сессии
                        session_time = datetime.strptime(session_name.split('_')[2], "%Y%m%d%H%M%S")
                        
                        # Если сессия старше 10 минут, закрываем её
                        if (current_time - session_time).total_seconds() > 600:
                            sessions_to_close.append((session_name, client))
                    except (ValueError, IndexError):
                        continue
                
                for session_name, client in sessions_to_close:
                    await cls.close_temp_client(client)
                    
            except Exception as e:
                logger.error(f"Ошибка при очистке старых сессий: {str(e)}")

    @classmethod
    async def close_all(cls):
        """Закрыть все соединения"""
        try:
            # Отменяем задачу очистки
            if cls._cleanup_task:
                cls._cleanup_task.cancel()
                try:
                    await cls._cleanup_task
                except asyncio.CancelledError:
                    pass
            
            # Закрываем все временные клиенты
            for session_name, client in list(cls._clients.items()):
                await cls.close_temp_client(client)
            
            # Закрываем основной клиент
            if cls._main_client and cls._main_client.is_connected():
                await cls._main_client.disconnect()
                cls._main_client = None
                
            logger.info("Все клиенты Telethon успешно закрыты")
            
        except Exception as e:
            logger.error(f"Ошибка при закрытии всех клиентов: {str(e)}")

    @classmethod
    async def close(cls):
        """Алиас для close_all для обратной совместимости"""
        await cls.close_all()

    @classmethod
    async def send_to_group(cls, group_username: str, text: str, photo_url: str = None, is_video: bool = False, is_local: bool = False):
        """
        Отправка поста в группу/канал
        
        Args:
            group_username: username группы/канала (без @)
            text: текст поста
            photo_url: URL фото/видео или путь к локальному файлу (опционально)
            is_video: является ли медиафайл видео
            is_local: является ли файл локальным (True) или URL (False)
        """
        try:
            client = await cls.get_client()
            
            # Убираем @ если есть
            if group_username.startswith('@'):
                group_username = group_username[1:]
            
            # Получаем сущность группы/канала
            entity = await client.get_entity(group_username)
            
            if photo_url:
                # Отправляем с медиафайлом
                if is_local:
                    # Локальный файл - отправляем напрямую
                    try:
                        if is_video:
                            # Для локального видео
                            await client.send_file(
                                entity,
                                photo_url,
                                caption=text,
                                supports_streaming=True,
                                force_document=False,
                                parse_mode='markdown'  # Поддержка разметки для жирных заголовков
                            )
                            logger.info(f"Локальное видео успешно отправлено в {group_username}")
                        else:
                            # Для локального фото
                            await client.send_file(
                                entity,
                                photo_url,
                                caption=text,
                                force_document=False,
                                parse_mode='markdown'  # Поддержка разметки для жирных заголовков
                            )
                            logger.info(f"Локальное фото успешно отправлено в {group_username}")
                    except Exception as local_error:
                        logger.error(f"Ошибка при отправке локального файла {photo_url}: {local_error}")
                        # Отправляем только текст
                        await client.send_message(entity, text, parse_mode='markdown')
                        logger.info(f"Отправлен только текст в {group_username}")
                else:
                    # URL файл - скачиваем и отправляем
                    if is_video:
                        # Для видео указываем supports_streaming=True и force_document=False
                        await client.send_file(
                            entity,
                            photo_url,
                            caption=text,
                            supports_streaming=True,
                            force_document=False,
                            mime_type='video/mp4',
                            parse_mode='markdown'  # Поддержка разметки для жирных заголовков
                        )
                        logger.info(f"Видео успешно отправлено в {group_username}")
                    else:
                        # Для фото всегда скачиваем и отправляем как изображение
                        try:
                            async with aiohttp.ClientSession() as session:
                                async with session.get(photo_url) as response:
                                    if response.status == 200:
                                        photo_bytes = await response.read()
                                        
                                        # Определяем формат изображения по заголовкам
                                        content_type = response.headers.get('content-type', '').lower()
                                        if 'jpeg' in content_type or 'jpg' in content_type:
                                            filename = "photo.jpg"
                                        elif 'png' in content_type:
                                            filename = "photo.png"
                                        elif 'webp' in content_type:
                                            filename = "photo.webp"
                                        else:
                                            filename = "photo.jpg"  # По умолчанию
                                        
                                        photo_io = io.BytesIO(photo_bytes)
                                        photo_io.name = filename
                                        
                                        # Отправляем как фото, используя специальный метод
                                        try:
                                            # Используем send_file с caption для корректной отправки фото с текстом
                                            await client.send_file(
                                                entity,
                                                photo_io,
                                                caption=text,
                                                force_document=False,
                                                attributes=[],
                                                parse_mode='markdown'  # Поддержка разметки для жирных заголовков
                                            )
                                        except:
                                            # Если не получилось, используем send_file с принудительными параметрами
                                            await client.send_file(
                                                entity,
                                                photo_io,
                                                caption=text,
                                                force_document=False,
                                                attributes=[],
                                                parse_mode='markdown'  # Поддержка разметки для жирных заголовков
                                            )
                                        logger.info(f"Фото успешно отправлено как изображение в {group_username}")
                                    else:
                                        raise Exception(f"Не удалось скачать изображение: HTTP {response.status}")
                        except Exception as download_error:
                            logger.error(f"Ошибка при скачивании и отправке изображения: {download_error}")
                            # В крайнем случае отправляем только текст
                            await client.send_message(entity, f"{text}\n\n📷 Изображение: {photo_url}", parse_mode='markdown')
                            logger.info(f"Отправлен только текст с ссылкой на изображение в {group_username}")
            else:
                # Отправляем только текст
                await client.send_message(entity, text, parse_mode='markdown')
                logger.info(f"Текст успешно отправлен в {group_username}")
                
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при отправке поста в {group_username}: {e}")
            return False

    @classmethod
    async def check_bot_admin_rights(cls, group_username: str) -> dict:
        """
        Проверка админских прав бота в группе/канале
        
        Args:
            group_username: username группы/канала (без @)
            
        Returns:
            dict: {"is_admin": bool, "can_post": bool, "error": str}
        """
        try:
            client = await cls.get_client()
            
            # Убираем @ если есть
            if group_username.startswith('@'):
                group_username = group_username[1:]
            
            # Получаем сущность группы/канала
            entity = await client.get_entity(group_username)
            
            # Получаем информацию о боте
            bot_info = await client.get_me()
            
            # Получаем права участника (бота) в группе/канале
            try:
                from telethon.tl.functions.channels import GetParticipantRequest
                from telethon.tl.types import ChannelParticipantAdmin, ChannelParticipantCreator
                
                try:
                    # Используем правильный метод для получения участника
                    result = await client(GetParticipantRequest(
                        channel=entity,
                        participant=bot_info.id
                    ))
                    participant = result.participant
                    
                    # Проверяем, является ли бот администратором
                    if isinstance(participant, (ChannelParticipantAdmin, ChannelParticipantCreator)):
                        if isinstance(participant, ChannelParticipantCreator):
                            # Создатель канала имеет все права
                            return {
                                "is_admin": True,
                                "can_post": True,
                                "error": None
                            }
                        elif isinstance(participant, ChannelParticipantAdmin):
                            # Проверяем права администратора
                            admin_rights = participant.admin_rights
                            can_post = admin_rights.post_messages if admin_rights else False
                            return {
                                "is_admin": True,
                                "can_post": can_post,
                                "error": None
                            }
                    else:
                        # Обычный участник
                        return {
                            "is_admin": False,
                            "can_post": False,
                            "error": "Бот не является администратором группы/канала"
                        }
                        
                except Exception as get_participant_error:
                    # Если не удалось получить участника, возможно бот не добавлен
                    return {
                        "is_admin": False,
                        "can_post": False,
                        "error": f"Бот не найден в группе/канале или нет доступа: {str(get_participant_error)}"
                    }
                    
            except Exception as participant_error:
                return {
                    "is_admin": False,
                    "can_post": False,
                    "error": f"Ошибка при получении информации об участнике: {str(participant_error)}"
                }
                
        except Exception as e:
            logger.error(f"Ошибка при проверке прав в {group_username}: {e}")
            return {
                "is_admin": False,
                "can_post": False,
                "error": f"Ошибка при проверке: {str(e)}"
            } 