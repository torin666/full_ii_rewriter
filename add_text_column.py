#!/usr/bin/env python3
"""
Скрипт для добавления поля blocked_topics в таблицу autopost_settings
"""

import psycopg2
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv(override=True)

def add_blocked_topics_column():
    """Добавляет поле blocked_topics в таблицу autopost_settings"""
    
    # Параметры подключения к базе данных
    conn_params = {
        "host": "80.74.24.141",
        "port": 5432,
        "database": "mydb",
        "user": os.getenv('USER_DB'),
        "password": os.getenv('USER_PWD')
    }
    
    schema = "ii_rewriter"
    
    try:
        # Подключаемся к базе данных
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                
                # Проверяем, существует ли поле blocked_topics
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema}' 
                    AND table_name = 'autopost_settings' 
                    AND column_name = 'blocked_topics'
                """)
                
                has_blocked_topics = cur.fetchone() is not None
                
                if not has_blocked_topics:
                    # Добавляем поле blocked_topics
                    cur.execute(f"""
                        ALTER TABLE {schema}.autopost_settings 
                        ADD COLUMN blocked_topics TEXT
                    """)
                    print("✅ Поле blocked_topics добавлено в таблицу autopost_settings")
                else:
                    print("ℹ️ Поле blocked_topics уже существует в таблице autopost_settings")
                
                # Проверяем, существует ли поле text в таблице published_posts
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema}' 
                    AND table_name = 'published_posts' 
                    AND column_name = 'text'
                """)
                
                has_text_column = cur.fetchone() is not None
                
                if not has_text_column:
                    # Добавляем поле text
                    cur.execute(f"""
                        ALTER TABLE {schema}.published_posts 
                        ADD COLUMN text TEXT NOT NULL DEFAULT ''
                    """)
                    print("✅ Поле text добавлено в таблицу published_posts")
                else:
                    print("ℹ️ Поле text уже существует в таблице published_posts")
                
                conn.commit()
                print("✅ Все изменения успешно применены")
                
    except Exception as e:
        print(f"❌ Ошибка при добавлении полей: {e}")

if __name__ == "__main__":
    print("🔧 Добавление полей в базу данных...")
    add_blocked_topics_column()
    print("✅ Скрипт завершен") 