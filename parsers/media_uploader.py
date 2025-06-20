#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import logging
import os
import tempfile
import yadisk
import yt_dlp
import requests
from datetime import datetime, timedelta
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

# Настройка логирования
logger = logging.getLogger(__name__)

class YandexMediaUploader:
    """Базовый класс для загрузки медиа на Яндекс.Диск"""
    
    def __init__(self, ya_token, folder_name="/media"):
        """Инициализация загрузчика"""
        self.ya_disk = yadisk.YaDisk(token=ya_token)
        self.upload_folder = folder_name
        
    def init_yandex_folder(self):
        """Создание папки на Яндекс.Диске если её нет"""
        try:
            if not self.ya_disk.exists(self.upload_folder):
                self.ya_disk.mkdir(self.upload_folder)
                logger.info(f"✅ Создана папка {self.upload_folder} на Яндекс.Диске")
            else:
                logger.debug(f"📁 Папка {self.upload_folder} уже существует")
        except Exception as e:
            logger.error(f"❌ Ошибка создания папки: {e}")
            raise
    
    def upload_to_yandex_and_get_direct_link(self, local_file_path, remote_filename):
        """Загрузка файла на Яндекс.Диск и получение прямой ссылки"""
        try:
            # Создаем папку с датой
            date_folder = datetime.now().strftime("%Y_%m_%d")
            date_path = f"{self.upload_folder}/{date_folder}"
            
            # Создаем папку с датой если её нет
            if not self.ya_disk.exists(date_path):
                self.ya_disk.mkdir(date_path)
                logger.info(f"📁 Создана папка {date_path}")
            
            remote_path = f"{date_path}/{remote_filename}"
            logger.info(f"☁️ Загружаем на Яндекс.Диск: {date_folder}/{remote_filename}")
            
            # Загружаем файл
            self.ya_disk.upload(local_file_path, remote_path, overwrite=True)
            
            # Делаем файл публичным
            self.ya_disk.publish(remote_path)
            
            # Получаем публичную ссылку
            public_info = self.ya_disk.get_meta(remote_path)
            public_url = public_info.public_url
            
            logger.info(f"✅ Файл загружен и опубликован")
            logger.debug(f"🔗 Публичная ссылка: {public_url}")
            
            # Получаем прямую ссылку на файл
            direct_link = self.get_direct_download_link(public_url)
            if direct_link:
                logger.info(f"📂 ПРЯМАЯ ссылка получена: {direct_link[:60]}...")
                return direct_link
            else:
                logger.warning("⚠️ Не удалось получить прямую ссылку, возвращаем публичную")
                return public_url
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки на Яндекс.Диск: {e}")
            return None
    
    def get_direct_download_link(self, public_url):
        """Получение прямой ссылки на скачивание файла"""
        try:
            # Яндекс.Диск API для получения прямой ссылки
            api_url = "https://cloud-api.yandex.net/v1/disk/public/resources/download"
            params = {"public_key": public_url}
            
            response = requests.get(api_url, params=params)
            if response.status_code == 200:
                data = response.json()
                direct_url = data.get('href')
                return direct_url
            else:
                logger.error(f"Ошибка получения прямой ссылки: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка при получении прямой ссылки: {e}")
            return None


class VKVideoUploader(YandexMediaUploader):
    """Загрузчик видео из VK на Яндекс.Диск"""
    
    def __init__(self, ya_token):
        super().__init__(ya_token, "/vk_videos")
        
    def download_and_upload_vk_video(self, post_link):
        """Скачивание VK видео и загрузка на Яндекс.Диск"""
        temp_dir = None
        try:
            logger.info(f"🎬 Начинаем загрузку VK видео: {post_link}")
            
            # Создаем временную папку
            temp_dir = tempfile.mkdtemp()
            
            # Настройки для yt-dlp
            ydl_opts = {
                'outtmpl': os.path.join(temp_dir, '%(title)s.%(ext)s'),
                'format': 'best[height<=720]',  # Максимум 720p для экономии места
                'quiet': True,
                'no_warnings': True,
            }
            
            # Скачиваем видео
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(post_link, download=True)
                video_path = ydl.prepare_filename(info_dict)
                
                if os.path.exists(video_path):
                    file_size = os.path.getsize(video_path) / (1024 * 1024)  # В MB
                    logger.info(f"✅ VK видео скачано: {file_size:.2f} MB")
                    
                    # Генерируем имя для загрузки
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    remote_filename = f"vk_video_{timestamp}.mp4"
                    
                    # Загружаем на Яндекс.Диск
                    direct_url = self.upload_to_yandex_and_get_direct_link(video_path, remote_filename)
                    return direct_url
                else:
                    logger.error("❌ VK видео файл не найден после скачивания")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки VK видео: {e}")
            return None
        finally:
            # Очищаем временные файлы
            if temp_dir and os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir)
                logger.debug("🗑️ Временные файлы VK видео удалены")


class TelegramMediaUploader(YandexMediaUploader):
    """Загрузчик медиа из Telegram на Яндекс.Диск"""
    
    def __init__(self, ya_token):
        super().__init__(ya_token, "/tg_media")

    async def process_telegram_media(self, client, message):
        """Обработка медиа из Telegram сообщения"""
        temp_dir = None
        try:
            # Определяем тип медиа
            media_type = None
            file_extension = None
            
            if message.photo:
                media_type = 'photo'
                file_extension = '.jpg'
                logger.info(f"🖼️ Найдено фото в сообщении {message.id}")
            elif message.video:
                media_type = 'video'
                file_extension = '.mp4'
                logger.info(f"🎬 Найдено видео в сообщении {message.id}")
            elif message.document:
                media_type = 'document'
                if message.document.mime_type:
                    if 'image' in message.document.mime_type:
                        media_type = 'photo'
                        file_extension = '.jpg'
                        logger.info(f"🖼️ Найдено изображение-документ в сообщении {message.id}")
                    elif 'video' in message.document.mime_type:
                        media_type = 'video'
                        file_extension = '.mp4'
                        logger.info(f"🎬 Найдено видео-документ в сообщении {message.id}")
                    elif 'gif' in message.document.mime_type:
                        media_type = 'gif'
                        file_extension = '.gif'
                        logger.info(f"🎭 Найдена GIF в сообщении {message.id}")
                    else:
                        media_type = 'file'
                        file_extension = '.bin'
                        logger.info(f"📄 Найден файл в сообщении {message.id}")
                else:
                    media_type = 'file'
                    file_extension = '.bin'
            else:
                return None, None
            
            logger.info(f"📥 Обрабатываем {media_type} из сообщения {message.id}")
            
            # Создаем временную папку
            temp_dir = tempfile.mkdtemp()
            
            # Генерируем имя файла
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            local_filename = f"tg_{media_type}_{message.id}_{timestamp}{file_extension}"
            local_path = os.path.join(temp_dir, local_filename)
            
            # Скачиваем файл
            logger.info(f"⬇️ Скачиваем {media_type}...")
            await client.download_media(message, local_path)
            
            if os.path.exists(local_path):
                file_size = os.path.getsize(local_path) / (1024 * 1024)  # В MB
                logger.info(f"✅ Файл скачан: {local_filename} ({file_size:.2f} MB)")
                
                # Загружаем на Яндекс.Диск с датой в названии
                date_folder = datetime.now().strftime("%Y%m%d")
                remote_filename = f"{date_folder}_tg_{media_type}_{timestamp}{file_extension}"
                direct_url = self.upload_to_yandex_and_get_direct_link(local_path, remote_filename)
                
                return direct_url, media_type
            else:
                logger.error("❌ Файл не был скачан")
                return None, None
                
        except Exception as e:
            logger.error(f"❌ Ошибка обработки медиа: {e}")
            return None, None
        finally:
            # Очищаем временные файлы
            if temp_dir and os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir)
                logger.debug("🗑️ Временные файлы Telegram удалены")


class MediaUploaderManager:
    """Менеджер для всех загрузчиков медиа"""
    
    def __init__(self, ya_token):
        self.ya_token = ya_token
        self.vk_uploader = None
        self.tg_uploader = None
        
        if ya_token:
            try:
                self.vk_uploader = VKVideoUploader(ya_token)
                self.vk_uploader.init_yandex_folder()
                
                self.tg_uploader = TelegramMediaUploader(ya_token)  
                self.tg_uploader.init_yandex_folder()
                
                logger.info("✅ Загрузчики медиа инициализированы")
            except Exception as e:
                logger.error(f"❌ Ошибка инициализации загрузчиков: {e}")
        else:
            logger.warning("⚠️ Токен Яндекс.Диска не найден, медиа не будет загружаться")
    
    def process_vk_video_link(self, video_link):
        """Обработка VK видео ссылки"""
        if not self.vk_uploader:
            return video_link  # Возвращаем исходную ссылку
            
        try:
            if 'vk.com/video' in video_link:
                direct_url = self.vk_uploader.download_and_upload_vk_video(video_link)
                return direct_url if direct_url else video_link
        except Exception as e:
            logger.error(f"Ошибка обработки VK видео: {e}")
        
        return video_link  # Возвращаем исходную ссылку при ошибке
    
    async def process_tg_media(self, client, message):
        """Обработка Telegram медиа"""
        if not self.tg_uploader:
            return None, None
            
        try:
            return await self.tg_uploader.process_telegram_media(client, message)
        except Exception as e:
            logger.error(f"Ошибка обработки Telegram медиа: {e}")
            return None, None 