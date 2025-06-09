import yadisk
import os
import logging
from config.settings import YANDEX_DISK_TOKEN

logger = logging.getLogger(__name__)

class YandexDiskManager:
    def __init__(self):
        self.disk = yadisk.YaDisk(token=YANDEX_DISK_TOKEN)
        
    def upload_photo(self, local_path, remote_path):
        """
        Загружает фото на Яндекс.Диск
        
        Args:
            local_path (str): Локальный путь к файлу
            remote_path (str): Путь на Яндекс.Диске
            
        Returns:
            str: Публичная ссылка на файл или None в случае ошибки
        """
        try:
            # Проверяем существование локального файла
            if not os.path.exists(local_path):
                logger.error(f"Локальный файл не найден: {local_path}")
                return None
                
            # Создаем директорию на диске, если её нет
            remote_dir = os.path.dirname(remote_path)
            if not self.disk.exists(remote_dir):
                self.disk.mkdir(remote_dir)
                
            # Загружаем файл
            self.disk.upload(local_path, remote_path, overwrite=True)
            
            # Публикуем файл и получаем ссылку
            if not self.disk.is_public(remote_path):
                public_link = self.disk.publish(remote_path)
                return self.disk.get_download_link(remote_path)
            else:
                return self.disk.get_download_link(remote_path)
                
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла на Яндекс.Диск: {e}")
            return None
            
    def cleanup_old_files(self, directory, days=7):
        """
        Удаляет старые файлы из указанной директории
        
        Args:
            directory (str): Путь к директории на Яндекс.Диске
            days (int): Количество дней, после которых файлы считаются устаревшими
        """
        try:
            if not self.disk.exists(directory):
                return
                
            for item in self.disk.listdir(directory):
                if (datetime.now() - item.modified).days > days:
                    try:
                        self.disk.remove(item.path)
                        logger.info(f"Удален устаревший файл: {item.path}")
                    except Exception as e:
                        logger.error(f"Ошибка при удалении файла {item.path}: {e}")
                        
        except Exception as e:
            logger.error(f"Ошибка при очистке старых файлов: {e}") 