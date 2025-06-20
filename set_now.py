#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from database.DatabaseManager import DatabaseManager
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def set_now():
    """Устанавливает время следующего автопоста на текущий момент"""
    try:
        db = Database()
        
        # Получаем текущее время
        current_time = datetime.now().strftime('%d.%m.%Y %H:%M')
        
        print(f"🕐 Устанавливаем время автопоста на: {current_time}")
        
        # Устанавливаем время для всех активных групп
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE autopost_settings 
                    SET next_post_time = NOW() 
                    WHERE is_active = true
                """)
                updated_count = cur.rowcount
                
        print(f"✅ Обновлено {updated_count} активных групп")
        print("🚀 Автопост должен сработать в течение ближайших 5 минут")
        
    except Exception as e:
        logger.error(f"Ошибка при установке времени: {e}")
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    set_now() 