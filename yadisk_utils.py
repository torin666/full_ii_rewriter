import yadisk
import os
import logging

logger = logging.getLogger(__name__)

YANDEX_DISK_TOKEN = os.getenv('yandex_api')
y = yadisk.YaDisk(token=YANDEX_DISK_TOKEN)

def ensure_yadisk_folder(y, remote_folder):
    if not y.exists(remote_folder):
        y.mkdir(remote_folder)

async def upload_to_yadisk_and_get_url(local_path, remote_folder="/news_photos/"):
    try:
        # Синхронно проверяем и создаём папку
        ensure_yadisk_folder(y, remote_folder)
        remote_path = remote_folder + os.path.basename(local_path)
        
        # Загружаем файл
        y.upload(local_path, remote_path, overwrite=True)
        
        # Публикуем файл
        try:
            y.publish(remote_path)
        except Exception as e:
            logger.error(f"Ошибка при публикации файла: {str(e)}")
        
        # Получаем ссылку
        try:
            return y.get_download_link(remote_path)
        except Exception as e:
            logger.error(f"Ошибка при получении ссылки: {str(e)}")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка при загрузке на Яндекс.Диск: {str(e)}")
        return None 