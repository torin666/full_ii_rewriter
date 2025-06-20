import psycopg2
import logging
from typing import List, Dict, Optional
from aiogram.fsm.state import State, StatesGroup
from datetime import datetime, timedelta
import pytz  # Добавляем импорт для работы с часовыми поясами
import random
import os
from dotenv import load_dotenv
import asyncio
import concurrent.futures

# Загружаем переменные окружения
load_dotenv(override=True)

# Импорт для сравнения текстов
try:
    import spacy
    # Заменили модель на 'md' для большей точности
    nlp = spacy.load("ru_core_news_md")
except (ImportError, OSError):
    # Если spacy не установлен или модель не найдена, используем простое сравнение
    nlp = None

logger = logging.getLogger(__name__)

def env(key):
    import os
    return os.environ.get(key)

class DatabaseManager:
    def __init__(self):
        self.conn_params = {
            "host": "80.74.24.141",
            "port": 5432,
            "database": "mydb",
            "user": os.getenv('USER_DB'),
            "password": os.getenv('USER_PWD')
        }
        self.schema = "ii_rewriter"

    def init_db(self):
        """Инициализация базы данных - создание схемы и необходимых таблиц"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Создаем схему, если её нет
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
                
                # Проверяем существование старой таблицы links
                cur.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = '{self.schema}' 
                        AND table_name = 'links'
                    )
                """)
                table_exists = cur.fetchone()[0]
                
                if table_exists:
                    # Проверяем структуру таблицы
                    cur.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = '{self.schema}' 
                        AND table_name = 'links' 
                        AND column_name = 'user_id'
                    """)
                    has_user_id = cur.fetchone() is not None
                    
                    if not has_user_id:
                        # Удаляем временную таблицу, если она существует
                        cur.execute(f"""
                            DROP TABLE IF EXISTS {self.schema}.links_new
                        """)
                        
                        # Создаем временную таблицу с новой структурой
                        cur.execute(f"""
                            CREATE TABLE {self.schema}.links_new (
                                id SERIAL PRIMARY KEY,
                                user_id BIGINT NOT NULL,
                                link TEXT NOT NULL,
                                themes TEXT[],
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            )
                        """)
                        
                        # Копируем данные из старой таблицы в новую
                        cur.execute(f"""
                            INSERT INTO {self.schema}.links_new (user_id, link, themes, created_at)
                            SELECT id, link, themes, CURRENT_TIMESTAMP 
                            FROM {self.schema}.links
                        """)
                        
                        # Удаляем старую таблицу и переименовываем новую
                        cur.execute(f"DROP TABLE {self.schema}.links")
                        cur.execute(f"ALTER TABLE {self.schema}.links_new RENAME TO links")
                        
                        # Создаем индекс для быстрого поиска по user_id
                        cur.execute(f"""
                            CREATE INDEX IF NOT EXISTS links_user_id_idx 
                            ON {self.schema}.links (user_id)
                        """)
                else:
                    # Создаем таблицу с новой структурой
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {self.schema}.links (
                            id SERIAL PRIMARY KEY,
                            user_id BIGINT NOT NULL,
                            link TEXT NOT NULL,
                            themes TEXT[],
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # Создаем индекс для быстрого поиска по user_id
                    cur.execute(f"""
                        CREATE INDEX IF NOT EXISTS links_user_id_idx 
                        ON {self.schema}.links (user_id)
                    """)
                
                # Создаем таблицу для постов
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}.posts (
                        id SERIAL PRIMARY KEY,
                        group_link TEXT NOT NULL,
                        post_link TEXT NOT NULL,
                        text TEXT NOT NULL,
                        date TEXT NOT NULL,
                        likes INTEGER DEFAULT 0,
                        views INTEGER DEFAULT 0,
                        comments_count INTEGER DEFAULT 0,
                        comments_likes INTEGER DEFAULT 0,
                        photo_url TEXT,
                        using_post TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # Создаем таблицу для GPT ролей
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}.gpt_roles (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        role_text TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(user_id)
                    )
                """)

                # Создаем таблицу для пабликов пользователей
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}.user_groups (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        group_link TEXT NOT NULL,
                        themes TEXT[],
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(user_id, group_link)
                    )
                """)

                # Создаем таблицу для автопостинга
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}.autopost_settings (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        group_link TEXT NOT NULL,
                        mode TEXT NOT NULL CHECK (mode IN ('automatic', 'controlled')),
                        is_active BOOLEAN DEFAULT true,
                        next_post_time TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        source_selection_mode TEXT DEFAULT 'auto' CHECK (source_selection_mode IN ('auto', 'manual')),
                        selected_sources TEXT,
                        autopost_role TEXT,
                        posts_count INTEGER DEFAULT 5 CHECK (posts_count BETWEEN 1 AND 10),
                        UNIQUE(user_id, group_link)
                    )
                """)

                # Добавляем поле autopost_role если его нет (для существующих таблиц)
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{self.schema}' 
                    AND table_name = 'autopost_settings' 
                    AND column_name = 'autopost_role'
                """)
                has_autopost_role = cur.fetchone() is not None
                
                if not has_autopost_role:
                    cur.execute(f"""
                        ALTER TABLE {self.schema}.autopost_settings 
                        ADD COLUMN autopost_role TEXT
                    """)
                    logger.info("Добавлено поле autopost_role в таблицу autopost_settings")

                # Добавляем поле posts_count если его нет (для существующих таблиц)
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{self.schema}' 
                    AND table_name = 'autopost_settings' 
                    AND column_name = 'posts_count'
                """)
                has_posts_count = cur.fetchone() is not None
                
                if not has_posts_count:
                    cur.execute(f"""
                        ALTER TABLE {self.schema}.autopost_settings 
                        ADD COLUMN posts_count INTEGER DEFAULT 5 CHECK (posts_count BETWEEN 1 AND 10)
                    """)
                    logger.info("Добавлено поле posts_count в таблицу autopost_settings")

                # Добавляем поле blocked_topics если его нет (для существующих таблиц)
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{self.schema}' 
                    AND table_name = 'autopost_settings' 
                    AND column_name = 'blocked_topics'
                """)
                has_blocked_topics = cur.fetchone() is not None
                
                if not has_blocked_topics:
                    cur.execute(f"""
                        ALTER TABLE {self.schema}.autopost_settings 
                        ADD COLUMN blocked_topics TEXT
                    """)
                    logger.info("Добавлено поле blocked_topics в таблицу autopost_settings")

                # Создаем таблицу для очереди автопостинга
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}.autopost_queue (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        group_link TEXT NOT NULL,
                        post_text TEXT NOT NULL,
                        post_image TEXT,
                        scheduled_time TIMESTAMP NOT NULL,
                        status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'sent_for_approval', 'approved', 'published', 'cancelled', 'publishing', 'failed', 'expired')),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_video BOOLEAN DEFAULT false,
                        mode TEXT DEFAULT 'controlled' CHECK (mode IN ('automatic', 'controlled')),
                        original_post_url TEXT
                    )
                """)
                
                # Добавляем поле original_post_url если его нет (для существующих таблиц)
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{self.schema}' 
                    AND table_name = 'autopost_queue' 
                    AND column_name = 'original_post_url'
                """)
                if not cur.fetchone():
                    cur.execute(f"""
                        ALTER TABLE {self.schema}.autopost_queue 
                        ADD COLUMN original_post_url TEXT
                    """)
                    logger.info("Добавлено поле original_post_url в таблицу autopost_queue")

                # Создаем таблицу для опубликованных постов в соответствии с вашей структурой
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}.published_posts (
                        id SERIAL PRIMARY KEY,
                        group_link TEXT NOT NULL,
                        text TEXT,
                        post_link TEXT,
                        post_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Добавляем логику миграции для переименования колонок, если существует старая версия таблицы
                cur.execute(f"SELECT 1 FROM information_schema.columns WHERE table_schema = '{self.schema}' AND table_name = 'published_posts' AND column_name = 'published_at'")
                if cur.fetchone():
                    cur.execute(f"ALTER TABLE {self.schema}.published_posts RENAME COLUMN published_at TO post_date")
                    logger.info("Миграция: колонка 'published_at' переименована в 'post_date'")

                cur.execute(f"SELECT 1 FROM information_schema.columns WHERE table_schema = '{self.schema}' AND table_name = 'published_posts' AND column_name = 'published_text'")
                if cur.fetchone():
                    cur.execute(f"ALTER TABLE {self.schema}.published_posts RENAME COLUMN published_text TO text")
                    logger.info("Миграция: колонка 'published_text' переименована в 'text'")
                
                cur.execute(f"SELECT 1 FROM information_schema.columns WHERE table_schema = '{self.schema}' AND table_name = 'published_posts' AND column_name = 'original_post_url'")
                if cur.fetchone():
                    cur.execute(f"ALTER TABLE {self.schema}.published_posts RENAME COLUMN original_post_url TO post_link")
                    logger.info("Миграция: колонка 'original_post_url' переименована в 'post_link'")

                # Создаем индекс для быстрого поиска
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_published_posts_group_date 
                    ON {self.schema}.published_posts(group_link, post_date)
                """)

                conn.commit()
                logger.info("База данных успешно инициализирована")

    def get_connection(self):
        return psycopg2.connect(**self.conn_params)

    def get_active_autopost_groups(self):
        """Получает список активных групп для автопостинга"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Получаем текущее время в Москве
                moscow_tz = pytz.timezone('Europe/Moscow')
                now_moscow = datetime.now(moscow_tz)
                
                # Проверяем рабочее время (6:00 - 23:00)
                if not (6 <= now_moscow.hour < 23):
                    logger.info(f"⏰ Сейчас не рабочее время: {now_moscow.strftime('%H:%M')}")
                    return []
                
                # Получаем группы, у которых время следующего поста наступило
                query = f"""
                    SELECT user_id, group_link, mode 
                    FROM {self.schema}.autopost_settings
                    WHERE is_active = true 
                    AND next_post_time <= NOW()
                    AND next_post_time IS NOT NULL
                """
                cur.execute(query)
                columns = ['user_id', 'group_link', 'mode']
                groups = [dict(zip(columns, row)) for row in cur.fetchall()]
                
                # Проверяем, что прошло достаточно времени с последнего поста
                for group in groups:
                    last_post_query = f"""
                        SELECT MAX(date) 
                        FROM {self.schema}.posts 
                        WHERE group_link = %s
                    """
                    cur.execute(last_post_query, (group['group_link'],))
                    last_post_time = cur.fetchone()[0]
                    
                    if last_post_time:
                        last_post_time = last_post_time.replace(tzinfo=pytz.UTC)
                        last_post_time = last_post_time.astimezone(moscow_tz)
                        time_diff = now_moscow - last_post_time
                        
                        # Если прошло меньше 40 минут, пропускаем группу
                        if time_diff.total_seconds() < 2400:  # 40 минут = 2400 секунд
                            logger.info(f"⏰ Пропускаем группу {group['group_link']}: прошло только {time_diff.total_seconds()/60:.1f} минут с последнего поста")
                            groups.remove(group)
                
                return groups

    def has_pending_autopost(self, user_id: int, group_link: str) -> bool:
        """Проверяет есть ли ожидающие автопосты для данной группы"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                query = f"""
                    SELECT 1 
                    FROM {self.schema}.autopost_queue
                    WHERE user_id = %s AND group_link = %s AND status = 'pending'
                    LIMIT 1
                """
                cur.execute(query, (user_id, group_link))
                return cur.fetchone() is not None

    def get_links(self):
        query = f"SELECT * FROM {self.schema}.links"
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                links = [row[0] for row in cur.fetchall()]
                return links

    def save_posts_to_db(self, posts):
        # Проверка существования поста
        check_query = f"""
            SELECT 1 FROM {self.schema}.posts 
            WHERE text = %s AND post_link = %s
            LIMIT 1
        """

        # Запрос на вставку нового поста
        insert_query = f"""
            INSERT INTO {self.schema}.posts 
            (group_link, post_link, text, date, likes, views, comments_count, comments_likes, photo_url, using_post)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
        """

        # Запрос на обновление метрик существующего поста
        update_query = f"""
            UPDATE {self.schema}.posts 
            SET 
                likes = %s,
                views = %s,
                comments_count = %s,
                comments_likes = %s,
                date = %s,
                photo_url = %s
            WHERE text = %s AND post_link = %s
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for post in posts:
                    # Проверяем, существует ли пост
                    cur.execute(check_query, (post['text'], post['post_link']))
                    exists = cur.fetchone()

                    # Проверяем текущее значение using_post для этого поста
                    cur.execute(f"""
                        SELECT using_post FROM {self.schema}.posts 
                        WHERE text = %s AND post_link = %s
                    """, (post['text'], post['post_link']))
                    current_using_post = cur.fetchone()
                    logger.info(f"Текущее значение using_post для поста {post['post_link']}: {current_using_post[0] if current_using_post else 'пост новый'}")

                    if exists:
                        # Обновляем метрики существующего поста, но НЕ трогаем using_post
                        cur.execute(update_query, (
                            post['likes'],
                            post['views'],
                            post['comments_count'],
                            post['comments_likes'],
                            post['date'],
                            post.get('photo_url'),
                            post['text'],
                            post['post_link']
                        ))
                        logger.info(f"Обновлен пост {post['post_link']}, значение using_post не изменено")
                    else:
                        # Добавляем новый пост с using_post = NULL
                        cur.execute(insert_query, (
                            post['group_link'],
                            post['post_link'],
                            post['text'],
                            post['date'],
                            post['likes'],
                            post['views'],
                            post['comments_count'],
                            post['comments_likes'],
                            post.get('photo_url')
                        ))
                        logger.info(f"Добавлен новый пост {post['post_link']} с using_post = NULL")

                    # Проверяем значение после вставки/обновления
                    cur.execute(f"""
                        SELECT using_post FROM {self.schema}.posts 
                        WHERE text = %s AND post_link = %s
                    """, (post['text'], post['post_link']))
                    after_using_post = cur.fetchone()
                    logger.info(f"Значение using_post после операции для поста {post['post_link']}: {after_using_post[0] if after_using_post else 'ошибка'}")

                conn.commit()

    def compare_texts(self, text1, text2, threshold=0.9):
        """Сравнивает два текста и возвращает True, если их схожесть >= threshold."""
        if nlp is not None:
            # Используем spacy для семантического сравнения
            try:
                doc1 = nlp(text1)
                doc2 = nlp(text2)
                similarity = doc1.similarity(doc2)
                return similarity >= threshold
            except Exception as e:
                logger.warning(f"Ошибка в spacy сравнении: {e}, используем простое сравнение")
        
        # Простое сравнение на основе общих слов (fallback)
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())
        
        if not words1 or not words2:
            return False
        
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        # Jaccard similarity
        jaccard_similarity = len(intersection) / len(union) if len(union) > 0 else 0
        return jaccard_similarity >= threshold

    def get_post(self):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                today = datetime.now().strftime("%d.%m.%Y")
                print(f"Ищем посты за дату: {today}")
                check_query = f"""
                    SELECT date, text, using_post, photo_url FROM {self.schema}.posts
                    WHERE date = %s
                """
                cur.execute(check_query, (today,))
                all_posts = cur.fetchall()
                print(f"Все посты за сегодня в базе: {all_posts}")

                query = f"""
                    SELECT text, photo_url, post_link FROM {self.schema}.posts
                    WHERE (using_post IS NULL OR using_post != 'True')
                    AND date = %s
                    ORDER BY likes, comments_count desc
                """
                cur.execute(query, (today,))
                top_posts = cur.fetchall()
                print("Найдены посты для обработки:", top_posts)

                if not top_posts:
                    print("Нет новых постов для обработки")
                    return None

                used_posts_query = f"""
                    SELECT text, photo_url FROM {self.schema}.posts
                    WHERE using_post = 'True'
                """
                cur.execute(used_posts_query)
                used_posts = [(post[0], post[1]) for post in cur.fetchall()]
                print("Использованные посты:", used_posts)

                top_post = None
                for candidate in top_posts:
                    candidate_text = candidate[0]
                    candidate_photo = candidate[1]
                    if not used_posts:
                        top_post = candidate
                        break
                    is_unique = True
                    for used_text, used_photo in used_posts:
                        if self.compare_texts(candidate_text, used_text):
                            is_unique = False
                            break
                    if is_unique:
                        top_post = candidate
                        break
                if top_post is None and top_posts:
                    top_post = top_posts[0]
                if not top_post:
                    print("Не удалось найти подходящий пост")
                    return None
                for post in top_posts:
                    if self.compare_texts(top_post[0], post[0]):
                        mark_as_used_query = f"""
                            UPDATE {self.schema}.posts
                            SET using_post = 'True'
                            WHERE text = %s AND photo_url = %s
                        """
                        cur.execute(mark_as_used_query, (post[0], post[1]))
                conn.commit()
                # Возвращаем (text, photo_url, post_link)
                return top_post

    def get_post_link(self, text):
        """Получает ссылку на пост по его тексту"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                query = f"""
                    SELECT post_link FROM {self.schema}.posts
                    WHERE text = %s
                    LIMIT 1
                """
                cur.execute(query, (text,))
                result = cur.fetchone()
                return result[0] if result else None

    def add_source(self, user_id, link, themes):
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {self.schema}.links (user_id, link, themes) VALUES (%s, %s, %s)",
                    (user_id, link, themes)
                )
                conn.commit()

    def get_user_sources(self, user_id):
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id, link, themes FROM {self.schema}.links WHERE user_id = %s",
                    (user_id,)
                )
                columns = ['id', 'link', 'themes']
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_gpt_role(self, user_id: int) -> str:
        """Получает текущую GPT роль пользователя или создает дефолтную для нового пользователя"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Проверяем, есть ли уже роль у пользователя в таблице gpt_roles
                cur.execute(
                    f"SELECT role_text FROM {self.schema}.gpt_roles WHERE user_id = %s",
                    (user_id,)
                )
                result = cur.fetchone()
                
                if result:
                    # Если роль уже есть, возвращаем её
                    return result[0]
                else:
                    # Если роли нет (новый пользователь), создаем дефолтную
                    default_role = "Ты — журналист и редактор."
                    cur.execute(f"""
                        INSERT INTO {self.schema}.gpt_roles (user_id, role_text)
                        VALUES (%s, %s)
                        ON CONFLICT (user_id) DO NOTHING
                    """, (user_id, default_role))
                    conn.commit()
                    return default_role

    def set_gpt_role(self, user_id: int, role_text: str) -> None:
        """Устанавливает новую GPT роль для пользователя"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {self.schema}.gpt_roles (user_id, role_text)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id) 
                    DO UPDATE SET role_text = EXCLUDED.role_text
                """, (user_id, role_text))
                conn.commit()

    def delete_gpt_role(self, user_id: int) -> None:
        """Удаляет GPT роль пользователя (вернется к дефолтной)"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self.schema}.gpt_roles WHERE user_id = %s",
                    (user_id,)
                )
                conn.commit()

    def add_user_group(self, user_id: int, group_link: str, themes: list) -> None:
        """Добавляет паблик пользователя"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {self.schema}.user_groups (user_id, group_link, themes)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id, group_link) 
                    DO UPDATE SET themes = EXCLUDED.themes
                """, (user_id, group_link, themes))
                conn.commit()

    def add_group(self, user_id: int, group_link: str, themes: list) -> None:
        """Алиас для add_user_group для обратной совместимости"""
        return self.add_user_group(user_id, group_link, themes)

    def get_user_groups(self, user_id: int):
        """Получить группы пользователя"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT DISTINCT group_link, themes 
                    FROM {self.schema}.user_groups 
                    WHERE user_id = %s
                """, (user_id,))
                        
                    groups = []
                    for row in cur.fetchall():
                        groups.append({
                            'group_link': row[0],
                            'themes': row[1] or []
                        })
                        
                    return groups
                    
        except Exception as e:
            logger.error(f"Ошибка получения групп пользователя {user_id}: {e}")
            return []
    
    def get_user_sources(self, user_id: int):
        """Получить источники пользователя"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT id, link, themes 
                        FROM {self.schema}.links 
                        WHERE user_id = %s
                        ORDER BY id DESC
                    """, (user_id,))
                    
                    sources = []
                    for row in cur.fetchall():
                        sources.append({
                            'id': row[0],
                            'link': row[1],
                            'themes': row[2] or []
                        })
                    
                    return sources
                    
        except Exception as e:
            logger.error(f"Ошибка при получении источников пользователя: {e}")
            return []
    
    def add_autopost_setting(self, user_id: int, group_link: str, mode: str):
        """Добавить или обновить настройку автопостинга"""
        try:
            from datetime import datetime, timedelta
            import random
            
            # Используем новую функцию для расчета времени следующего поста
            next_post_time = self.calculate_next_post_time()
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Проверяем, существует ли уже настройка
                    cur.execute(f"""
                        SELECT id FROM {self.schema}.autopost_settings 
                        WHERE user_id = %s AND group_link = %s
                    """, (user_id, group_link))
                    
                    if cur.fetchone():
                        # Обновляем существующую настройку
                        cur.execute(f"""
                            UPDATE {self.schema}.autopost_settings 
                            SET mode = %s, is_active = %s, source_selection_mode = %s, 
                                next_post_time = %s, updated_at = CURRENT_TIMESTAMP
                            WHERE user_id = %s AND group_link = %s
                        """, (mode, True, 'auto', next_post_time, user_id, group_link))
                        logger.info(f"Настройка автопостинга обновлена для {group_link}, следующий пост: {next_post_time}")
                    else:
                        # Создаем новую настройку
                        cur.execute(f"""
                            INSERT INTO {self.schema}.autopost_settings 
                            (user_id, group_link, mode, is_active, source_selection_mode, next_post_time)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (user_id, group_link, mode, True, 'auto', next_post_time))
                        logger.info(f"Настройка автопостинга создана для {group_link}, следующий пост: {next_post_time}")
                    
        except Exception as e:
            logger.error(f"Ошибка при добавлении настройки автопостинга: {e}")
            raise

    def toggle_autopost_status(self, user_id: int, group_link: str, is_active: bool):
        """Включить/выключить автопостинг для группы"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        UPDATE {self.schema}.autopost_settings 
                        SET is_active = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND group_link = %s
                    """, (is_active, user_id, group_link))
                    
                    logger.info(f"Статус автопостинга обновлен для {group_link}: {is_active}")
                    
        except Exception as e:
            logger.error(f"Ошибка при обновлении статуса автопостинга: {e}")
    
    def save_selected_sources(self, user_id: int, group_link: str, sources_json: str):
        """Сохранить выбранные источники для группы"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        UPDATE {self.schema}.autopost_settings 
                        SET selected_sources = %s, 
                            source_selection_mode = 'manual',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND group_link = %s
                    """, (sources_json, user_id, group_link))
                    
                    logger.info(f"Выбранные источники сохранены для {group_link}")
                    
        except Exception as e:
            logger.error(f"Ошибка при сохранении выбранных источников: {e}")
    
    def get_autopost_settings(self, user_id: int):
        """Получает все настройки автопостинга для пользователя."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT id, group_link, mode, is_active, source_selection_mode, selected_sources, autopost_role, posts_count, blocked_topics 
                    FROM {self.schema}.autopost_settings 
                    WHERE user_id = %s
                """, (user_id,))
                settings = cur.fetchall()
                
                # Преобразуем результат в список словарей для удобства
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in settings]

    def get_autopost_settings_for_group(self, user_id: int, group_link: str) -> Optional[Dict]:
        """Получает настройки автопостинга для конкретной группы"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT group_link, mode, is_active, source_selection_mode, selected_sources, autopost_role, posts_count, blocked_topics
                        FROM {self.schema}.autopost_settings 
                        WHERE user_id = %s AND group_link = %s
                    """, (user_id, group_link))
                    
                    settings = cur.fetchone()
                    
                    if settings:
                        columns = ['group_link', 'mode', 'is_active', 'source_selection_mode', 'selected_sources', 'autopost_role', 'posts_count', 'blocked_topics']
                        return dict(zip(columns, settings))
                    else:
                        logger.warning(f"Настройки автопостинга не найдены для user_id={user_id}, group_link={group_link}")
                        return None
                        
        except Exception as e:
            logger.error(f"Ошибка при получении настроек автопостинга для группы {group_link}: {e}")
            return None

    def delete_user_group(self, user_id: int, group_link: str) -> None:
        """Удаляет паблик пользователя"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    DELETE FROM {self.schema}.user_groups 
                    WHERE user_id = %s AND group_link = %s
                """, (user_id, group_link))
                conn.commit()

    def get_active_sources(self) -> list:
        """Получает все активные источники для парсинга"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT DISTINCT link, themes 
                    FROM {self.schema}.links
                """)
                sources = []
                for row in cur.fetchall():
                    sources.append({
                        'source_url': row[0],
                        'themes': row[1]
                    })
                return sources

    def normalize_group_link(self, link: str) -> str:
        """Нормализует ссылку на группу для единообразного сравнения"""
        if not link:
            return link
            
        # Приводим к нижнему регистру и убираем пробелы
        link = link.lower().strip()
        
        # Убираем протокол и www
        link = link.replace('https://', '').replace('http://', '')
        link = link.replace('www.', '')
        
        # Убираем точку после vk если есть
        link = link.replace('vk.', 'vk')
        
        # Убираем точки в конце
        link = link.rstrip('.')
        
        # Извлекаем ID группы из разных форматов
        if 'public' in link:
            group_id = link.split('public')[-1]
            if group_id.isdigit():
                return f"vk.com/club{group_id}"
        elif 'club' in link:
            group_id = link.split('club')[-1]
            if group_id.isdigit():
                return f"vk.com/club{group_id}"
        elif 'wall-' in link:
            # Извлекаем ID группы из ссылки на стену
            parts = link.split('wall-')[1].split('_')
            if len(parts) > 0:
                group_id = parts[0]
                if group_id.isdigit():
                    return f"vk.com/club{group_id}"
                
        # Для обычных имен групп
        parts = link.split('/')
        if len(parts) > 1:
            group_name = parts[-1]
            # Если это числовой ID группы
            if group_name.isdigit():
                return f"vk.com/club{group_name}"
            return f"vk.com/{group_name}"
            
        return link

    def get_similar_theme_posts(self, user_id: int, group_link: str) -> list:
        """Получает посты с похожими темами учитывая настройки источников"""
        logger.info(f"🔍 Начинаем поиск постов для user_id={user_id}, group_link={group_link}")
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Получаем настройки автопостинга для группы
                settings_query = f"""
                    SELECT source_selection_mode, selected_sources 
                    FROM {self.schema}.autopost_settings 
                    WHERE user_id = %s AND group_link = %s
                """
                cur.execute(settings_query, (user_id, group_link))
                settings = cur.fetchone()
                
                source_mode = 'auto'  # по умолчанию
                selected_sources = None
                
                if settings:
                    source_mode = settings[0] or 'auto'
                    selected_sources = settings[1]
                    
                logger.info(f"📋 Режим источников: {source_mode}")
                
                source_links = []
                
                if source_mode == 'manual' and selected_sources:
                    # Получаем выбранные источники
                    try:
                        import json
                        source_ids = json.loads(selected_sources)
                        logger.info(f"📌 Выбранные источники IDs: {source_ids}")
                        
                        if source_ids:
                            placeholders = ', '.join(['%s' for _ in source_ids])
                            manual_query = f"""
                                SELECT link FROM {self.schema}.links 
                                WHERE id IN ({placeholders}) AND user_id = %s
                            """
                            cur.execute(manual_query, source_ids + [user_id])
                            source_links = [row[0] for row in cur.fetchall()]
                            logger.info(f"🎯 Получены выбранные источники: {source_links}")
                        
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.error(f"❌ Ошибка парсинга выбранных источников: {e}")
                        source_mode = 'auto'  # fallback на автоматический режим
                
                if source_mode == 'auto' or not source_links:
                    # Автоматический подбор по темам
                    logger.info("🤖 Используем автоматический подбор источников")
                    
                    # Получаем темы паблика для которого ищем посты
                    themes_query = f"""
                        SELECT themes FROM {self.schema}.user_groups 
                        WHERE user_id = %s AND group_link = %s
                    """
                    cur.execute(themes_query, (user_id, group_link))
                    result = cur.fetchone()
                    
                    if not result or not result[0]:
                        logger.warning(f"⚠️ Не найдены темы для паблика {group_link}")
                        # Возвращаем все источники пользователя
                        all_sources_query = f"""
                            SELECT link FROM {self.schema}.links 
                            WHERE user_id = %s
                        """
                        cur.execute(all_sources_query, (user_id,))
                        source_links = [row[0] for row in cur.fetchall()]
                        logger.info(f"🔄 Используем все источники пользователя: {len(source_links)}")
                    else:
                        group_themes = result[0]
                        logger.info(f"🏷️ Найдены темы паблика: {group_themes}")
                        
                        # Получаем источники с похожими темами
                        similar_sources_query = f"""
                            SELECT DISTINCT link 
                            FROM {self.schema}.links 
                            WHERE user_id = %s AND themes && %s::text[]
                        """
                        cur.execute(similar_sources_query, (user_id, group_themes))
                        source_links = [row[0] for row in cur.fetchall()]
                        
                        if not source_links:
                            logger.warning(f"⚠️ Не найдены источники с похожими темами")
                            # Fallback: используем все источники пользователя
                            all_sources_query = f"""
                                SELECT link FROM {self.schema}.links 
                                WHERE user_id = %s
                            """
                            cur.execute(all_sources_query, (user_id,))
                            source_links = [row[0] for row in cur.fetchall()]
                            logger.info(f"🔄 Fallback: используем все источники пользователя: {len(source_links)}")
                        else:
                            logger.info(f"✅ Найдены источники с похожими темами: {source_links}")
                
                if not source_links:
                    logger.error(f"❌ Не найдены источники для поиска постов")
                    return []
                
                # Нормализуем ссылки для поиска в постах
                normalized_links = []
                for link in source_links:
                    logger.info(f"🔗 Обрабатываем источник: {link}")
                    # Учитываем разные форматы ссылок
                    if 't.me/' in link or 'telegram.me/' in link:
                        # Телеграм каналы
                        if link.startswith('https://t.me/') or link.startswith('http://t.me/'):
                            normalized_links.append(link)
                            normalized_links.append(link.replace('https://', '').replace('http://', ''))
                            logger.info(f"   ➕ Добавлены варианты: {link}, {link.replace('https://', '').replace('http://', '')}")
                        elif link.startswith('t.me/'):
                            normalized_links.append(link)
                            normalized_links.append(f"https://{link}")
                            logger.info(f"   ➕ Добавлены варианты: {link}, https://{link}")
                        else:
                            normalized_links.append(link)
                            logger.info(f"   ➕ Добавлен как есть: {link}")
                    elif 'vk.com' in link:
                        # ВК группы
                        normalized = self.normalize_group_link(link)
                        normalized_links.append(normalized)
                        normalized_links.append(link)
                        logger.info(f"   ➕ ВК нормализация: {link} -> {normalized}")
                    else:
                        normalized_links.append(link)
                        logger.info(f"   ➕ Другой тип ссылки: {link}")
                
                # Убираем дубликаты
                normalized_links = list(set(normalized_links))
                logger.info(f"🔗 Нормализованные ссылки для поиска: {normalized_links}")
                
                # Ищем посты из этих источников
                if normalized_links:
                    placeholders = ', '.join(['%s' for _ in normalized_links])
                    posts_query = f"""
                        SELECT DISTINCT 
                            id, group_link, post_link, text, date, 
                            COALESCE(likes, 0) as likes, 
                            COALESCE(views, 0) as views, 
                            COALESCE(comments_count, 0) as comments_count, 
                            photo_url,
                            (COALESCE(likes, 0) + COALESCE(comments_count, 0)) as engagement
                        FROM {self.schema}.posts
                        WHERE group_link IN ({placeholders})
                        AND (using_post IS NULL OR using_post != 'True')
                        AND LENGTH(text) > 100
                        AND date = TO_CHAR(CURRENT_DATE, 'DD.MM.YYYY')
                        ORDER BY engagement DESC, id DESC
                        LIMIT 20
                    """
                    
                    cur.execute(posts_query, normalized_links)
                    posts = cur.fetchall()
                    
                    logger.info(f"📊 Найдено {len(posts)} постов за сегодня из {len(source_links)} источников")
                    for post in posts[:3]:  # Показываем первые 3 для отладки
                        logger.info(f"   📄 Пост: {post[1]} | дата: {post[4]} | лайки: {post[5]} | комменты: {post[7]} | текст: {post[3][:50]}...")
                    
                    # Форматируем результат
                    columns = ['id', 'group_link', 'post_link', 'text', 'date', 'likes', 'views', 'comments_count', 'photo_url']
                    return [dict(zip(columns, row[:-1])) for row in posts]  # Убираем engagement из результата
                else:
                    logger.error(f"❌ Не удалось нормализовать ссылки источников")
                    return []

    def get_multiple_theme_posts(self, user_id: int, group_link: str, limit: int = 5) -> list:
        """Получает несколько лучших постов с похожими темами для создания уникального контента"""
        logger.info(f"🔍 Начинаем поиск лучших постов для user_id={user_id}, group_link={group_link}")
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Получаем настройки автопостинга для группы, включая posts_count
                settings_query = f"""
                    SELECT source_selection_mode, selected_sources, posts_count 
                    FROM {self.schema}.autopost_settings 
                    WHERE user_id = %s AND group_link = %s
                """
                cur.execute(settings_query, (user_id, group_link))
                settings = cur.fetchone()
                
                source_mode = 'auto'
                selected_sources = None
                # Используем posts_count из настроек, или значение по умолчанию
                posts_to_fetch = limit 

                if settings:
                    source_mode = settings[0] or 'auto'
                    selected_sources = settings[1]
                    # Используем настройку пользователя, если она есть
                    posts_to_fetch = settings[2] or limit
                    
                logger.info(f"📋 Режим источников: {source_mode}, будем искать до {posts_to_fetch} постов-кандидатов.")
                
                source_links = []
                
                if source_mode == 'manual' and selected_sources:
                    # Получаем выбранные источники
                    try:
                        import json
                        source_ids = json.loads(selected_sources)
                        logger.info(f"📌 Выбранные источники IDs: {source_ids}")
                        
                        if source_ids:
                            placeholders = ', '.join(['%s' for _ in source_ids])
                            manual_query = f"""
                                SELECT link FROM {self.schema}.links 
                                WHERE id IN ({placeholders}) AND user_id = %s
                            """
                            cur.execute(manual_query, source_ids + [user_id])
                            source_links = [row[0] for row in cur.fetchall()]
                            logger.info(f"🎯 Получены выбранные источники: {source_links}")
                        
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.error(f"❌ Ошибка парсинга выбранных источников: {e}")
                        source_mode = 'auto'  # fallback на автоматический режим
                
                if source_mode == 'auto' or not source_links:
                    # Автоматический подбор по темам
                    logger.info("🤖 Используем автоматический подбор источников")
                    
                    # Получаем темы паблика для которого ищем посты
                    themes_query = f"""
                        SELECT themes FROM {self.schema}.user_groups 
                        WHERE user_id = %s AND group_link = %s
                    """
                    cur.execute(themes_query, (user_id, group_link))
                    result = cur.fetchone()
                    
                    if not result or not result[0]:
                        logger.warning(f"⚠️ Не найдены темы для паблика {group_link}")
                        # Возвращаем все источники пользователя
                        all_sources_query = f"""
                            SELECT link FROM {self.schema}.links 
                            WHERE user_id = %s
                        """
                        cur.execute(all_sources_query, (user_id,))
                        source_links = [row[0] for row in cur.fetchall()]
                        logger.info(f"🔄 Используем все источники пользователя: {len(source_links)}")
                    else:
                        group_themes = result[0]
                        logger.info(f"🏷️ Найдены темы паблика: {group_themes}")
                        
                        # Получаем источники с похожими темами
                        similar_sources_query = f"""
                            SELECT DISTINCT link 
                            FROM {self.schema}.links 
                            WHERE user_id = %s AND themes && %s::text[]
                        """
                        cur.execute(similar_sources_query, (user_id, group_themes))
                        source_links = [row[0] for row in cur.fetchall()]
                        
                        if not source_links:
                            logger.warning(f"⚠️ Не найдены источники с похожими темами")
                            # Fallback: используем все источники пользователя
                            all_sources_query = f"""
                                SELECT link FROM {self.schema}.links 
                                WHERE user_id = %s
                            """
                            cur.execute(all_sources_query, (user_id,))
                            source_links = [row[0] for row in cur.fetchall()]
                            logger.info(f"🔄 Fallback: используем все источники пользователя: {len(source_links)}")
                        else:
                            logger.info(f"✅ Найдены источники с похожими темами: {source_links}")
                
                if not source_links:
                    logger.error(f"❌ Не найдены источники для поиска постов")
                    return []
                
                # Нормализуем ссылки для поиска в постах
                normalized_links = []
                for link in source_links:
                    logger.info(f"🔗 Обрабатываем источник: {link}")
                    # Учитываем разные форматы ссылок
                    if 't.me/' in link or 'telegram.me/' in link:
                        # Телеграм каналы
                        if link.startswith('https://t.me/') or link.startswith('http://t.me/'):
                            normalized_links.append(link)
                            normalized_links.append(link.replace('https://', '').replace('http://', ''))
                            logger.info(f"   ➕ Добавлены варианты: {link}, {link.replace('https://', '').replace('http://', '')}")
                        elif link.startswith('t.me/'):
                            normalized_links.append(link)
                            normalized_links.append(f"https://{link}")
                            logger.info(f"   ➕ Добавлены варианты: {link}, https://{link}")
                        else:
                            normalized_links.append(link)
                            logger.info(f"   ➕ Добавлен как есть: {link}")
                    elif 'vk.com' in link:
                        # ВК группы
                        normalized = self.normalize_group_link(link)
                        normalized_links.append(normalized)
                        normalized_links.append(link)
                        logger.info(f"   ➕ ВК нормализация: {link} -> {normalized}")
                    else:
                        normalized_links.append(link)
                        logger.info(f"   ➕ Другой тип ссылки: {link}")
                
                # Убираем дубликаты
                normalized_links = list(set(normalized_links))
                logger.info(f"🔗 Нормализованные ссылки для поиска: {normalized_links}")
                
                # Ищем посты из этих источников
                if normalized_links:
                    posts_query = f"""
                        SELECT DISTINCT 
                            id, group_link, post_link, text, date, 
                            COALESCE(likes, 0) as likes, 
                            COALESCE(views, 0) as views, 
                            COALESCE(comments_count, 0) as comments_count, 
                            photo_url,
                            (COALESCE(likes, 0) + COALESCE(comments_count, 0)) as engagement
                        FROM {self.schema}.posts
                        WHERE group_link = ANY(%s)
                        AND (using_post IS NULL OR using_post != 'True')
                        AND LENGTH(text) > 100
                        AND date = TO_CHAR(CURRENT_DATE, 'DD.MM.YYYY')
                        ORDER BY engagement DESC, id DESC
                        LIMIT %s
                    """
                    
                    cur.execute(posts_query, (normalized_links, posts_to_fetch))
                    posts = cur.fetchall()
                    
                    logger.info(f"📊 Найдено {len(posts)} лучших постов за сегодня из {len(source_links)} источников")
                    for i, post in enumerate(posts, 1):
                        logger.info(f"   📄 Пост {i}: {post[1]} | лайки: {post[5]} | комменты: {post[7]} | текст: {post[3][:50]}...")
                    
                    # Форматируем результат
                    columns = ['id', 'group_link', 'post_link', 'text', 'date', 'likes', 'views', 'comments_count', 'photo_url']
                    return [dict(zip(columns, row[:-1])) for row in posts]  # Убираем engagement из результата
                else:
                    logger.error(f"❌ Не удалось нормализовать ссылки источников")
                    return []

    def get_published_posts_today(self, group_link: str) -> list:
        """
        Получает тексты ОРИГИНАЛЬНЫХ постов, опубликованных сегодня в указанной группе.
        Это нужно для корректного сравнения на дубликаты.
        """
        logger.info(f"📅 Получаем ОРИГИНАЛЫ опубликованных сегодня постов для группы: {group_link}")
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # 1. Получаем ссылки на оригинальные посты, опубликованные сегодня
                get_original_links_query = f"""
                    SELECT post_link
                    FROM {self.schema}.published_posts
                    WHERE group_link = %s
                    AND post_date >= CURRENT_DATE AT TIME ZONE 'Europe/Moscow'
                    AND post_link IS NOT NULL
                """
                cur.execute(get_original_links_query, (group_link,))
                original_links = [row[0] for row in cur.fetchall()]
                
                if not original_links:
                    logger.info(f"📊 Не найдено опубликованных постов за сегодня в группе {group_link}")
                    return []
                
                logger.info(f"🔗 Найдены оригинальные ссылки ({len(original_links)}): {original_links}")

                # 2. Получаем тексты этих оригинальных постов из таблицы 'posts'
                placeholders = ', '.join(['%s'] * len(original_links))
                get_original_texts_query = f"""
                    SELECT text, post_link FROM {self.schema}.posts
                    WHERE post_link IN ({placeholders})
                """
                cur.execute(get_original_texts_query, tuple(original_links))
                
                posts = cur.fetchall()
                logger.info(f"📊 Найдено {len(posts)} текстов оригинальных постов.")

                # Форматируем результат
                result = []
                for post_text, post_link in posts:
                    result.append({'text': post_text, 'post_link': post_link})
                    logger.info(f"   📄 Оригинальный текст: {post_text[:60].strip()}...")
                
                return result

    def mark_multiple_posts_as_used(self, post_texts: list):
        """Помечает несколько постов как использованные"""
        if not post_texts:
            return
            
        logger.info(f"🔒 Помечаем {len(post_texts)} постов как использованные")
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for text in post_texts:
                    cur.execute(f"""
                        UPDATE {self.schema}.posts
                        SET using_post = 'True'
                        WHERE text = %s
                    """, (text,))
                    logger.info(f"   ✅ Помечен как использованный: {text[:50]}...")
                conn.commit()

    def mark_post_as_used(self, post_link: str):
        """Помечает пост как использованный после публикации"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.schema}.posts
                    SET using_post = 'True'
                    WHERE post_link = %s
                """, (post_link,))
                conn.commit()

    def cancel_autopost_in_queue(self, user_id: int, group_link: str) -> bool:
        """Отменяет автопост в очереди"""
        logger.info(f"🔍 Пытаемся отменить пост для user_id={user_id}, group_link={group_link}")
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Сначала проверим, есть ли такой пост в очереди
                    check_query = f"""
                        SELECT id, post_text FROM {self.schema}.autopost_queue
                        WHERE user_id = %s AND group_link = %s AND status = 'pending'
                    """
                    cur.execute(check_query, (user_id, group_link))
                    existing_posts = cur.fetchall()
                    
                    logger.info(f"📊 Найдено pending постов для отмены: {len(existing_posts)}")
                    for post in existing_posts:
                        logger.info(f"   - ID: {post[0]}, текст: {post[1][:50]}...")
                    
                    if not existing_posts:
                        logger.warning(f"❌ Не найдено pending постов для отмены (user={user_id}, group={group_link})")
                        return False
                    
                    # Помечаем все найденные посты как отмененные
                    cancel_query = f"""
                        UPDATE {self.schema}.autopost_queue
                        SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND group_link = %s AND status = 'pending'
                    """
                    cur.execute(cancel_query, (user_id, group_link))
                    affected_rows = cur.rowcount
                    
                    conn.commit()
                    
                    logger.info(f"✅ Отменено {affected_rows} постов для user_id={user_id}, group_link={group_link}")
                    return affected_rows > 0
        except Exception as e:
            logger.error(f"Ошибка при отмене автопоста: {e}")
            return False
    
    def update_autopost_in_queue(self, user_id: int, group_link: str, new_text: str) -> bool:
        """Обновляет текст автопоста в очереди"""
        logger.info(f"🔍 Пытаемся обновить пост для user_id={user_id}, group_link={group_link}")
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Находим последний pending пост для этого пользователя и группы
                    update_query = f"""
                        UPDATE {self.schema}.autopost_queue
                        SET post_text = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND group_link = %s AND status = 'pending'
                    """
                    cur.execute(update_query, (new_text, user_id, group_link))
                    affected_rows = cur.rowcount
                    
                    conn.commit()
                    
                    logger.info(f"📝 Обновлено {affected_rows} постов для user_id={user_id}, group_link={group_link}")
                    return affected_rows > 0
        except Exception as e:
            logger.error(f"Ошибка при обновлении автопоста: {e}")
            return False

    def update_queue_status(self, queue_id: int, status: str):
        """Обновляет статус поста в очереди автопостинга"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query = f"""
                        UPDATE {self.schema}.autopost_queue 
                        SET status = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """
                    cur.execute(query, (status, queue_id))
                    conn.commit()
                    logger.info(f"✅ Статус поста ID={queue_id} обновлен на '{status}'")
        except Exception as e:
            logger.error(f"Ошибка при обновлении статуса очереди: {e}")

    def add_autopost_to_queue(self, user_id: int, group_link: str, text: str, image_url: str, scheduled_time: datetime, is_video: bool = False, mode: str = 'controlled', original_post_url: str = None):
        """Добавляет автопост в очередь"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query = f"""
                        INSERT INTO {self.schema}.autopost_queue 
                        (user_id, group_link, post_text, post_image, is_video, scheduled_time, mode, original_post_url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """
                    cur.execute(query, (user_id, group_link, text, image_url, is_video, scheduled_time, mode, original_post_url))
                    queue_id = cur.fetchone()[0]
                    conn.commit()
                    logger.info(f"✅ Пост добавлен в очередь для {group_link} с ID={queue_id}")
                    return queue_id
        except Exception as e:
            logger.error(f"Ошибка при добавлении автопоста в очередь: {e}")
            return None

    def get_pending_autopost_queue(self, status_filter: Optional[str] = None):
        """
        Получает все ожидающие автопосты.
        Можно отфильтровать по статусу, например 'approved'.
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query = f"""
                        SELECT id, user_id, group_link, post_text, post_image, is_video, scheduled_time, status, mode, original_post_url
                        FROM {self.schema}.autopost_queue 
                        WHERE scheduled_time <= CURRENT_TIMESTAMP
                    """
                    params = []

                    if status_filter:
                        query += " AND status = %s"
                        params.append(status_filter)
                    else:
                        # По умолчанию получаем все посты, готовые к обработке
                        query += " AND status IN ('pending', 'approved')"

                    query += " ORDER BY scheduled_time ASC"
                    
                    cur.execute(query, tuple(params))
                    columns = ['id', 'user_id', 'group_link', 'post_text', 'post_image', 'is_video', 'scheduled_time', 'status', 'mode', 'original_post_url']
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Ошибка при получении очереди автопостинга: {e}")
            return []

    def update_autopost_status(self, autopost_id: int, status: str):
        """Обновляет статус автопоста (алиас для update_queue_status)"""
        return self.update_queue_status(autopost_id, status)
    
    def approve_autopost_in_queue(self, user_id: int, group_link: str) -> bool:
        """Одобряет автопост в очереди (меняет статус с pending на approved)"""
        logger.info(f"🔍 Пытаемся одобрить пост для user_id={user_id}, group_link={group_link}")
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Находим pending пост для этого пользователя и группы
                    approve_query = f"""
                        UPDATE {self.schema}.autopost_queue 
                        SET status = 'approved', updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND group_link = %s AND status = 'pending'
                    """
                    cur.execute(approve_query, (user_id, group_link))
                    affected_rows = cur.rowcount
                    
                    conn.commit()
                    
                    logger.info(f"✅ Одобрено {affected_rows} постов для user_id={user_id}, group_link={group_link}")
                    return affected_rows > 0
        except Exception as e:
            logger.error(f"Ошибка при одобрении автопоста: {e}")
            return False

    def update_autopost_mode(self, user_id: int, group_link: str, mode: str):
        """Обновляет режим автопостинга для группы"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        UPDATE {self.schema}.autopost_settings 
                        SET mode = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND group_link = %s
                    """, (mode, user_id, group_link))
                    
                    affected_rows = cur.rowcount
                    logger.info(f"Режим автопостинга обновлен для {group_link}: {mode} (затронуто строк: {affected_rows})")
                    
        except Exception as e:
            logger.error(f"Ошибка при обновлении режима автопостинга: {e}")
            raise

    def delete_autopost_setting(self, user_id: int, group_link: str):
        """Удаляет настройки автопостинга"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                query = f"""
                    DELETE FROM {self.schema}.autopost_settings
                    WHERE user_id = %s AND group_link = %s
                """
                cur.execute(query, (user_id, group_link))
                conn.commit()
                
    def set_autopost_role(self, user_id: int, group_link: str, role: str):
        """Устанавливает роль GPT для конкретного автопостинга"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query = f"""
                        UPDATE {self.schema}.autopost_settings 
                        SET autopost_role = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND group_link = %s
                    """
                    cur.execute(query, (role, user_id, group_link))
                    conn.commit()
                    logger.info(f"Роль автопостинга обновлена для {group_link}: {role[:50]}...")
                
        except Exception as e:
            logger.error(f"Ошибка при установке роли автопостинга: {e}")
            raise

    def get_autopost_role(self, user_id: int, group_link: str) -> str:
        """Получает роль GPT для конкретного автопостинга"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query = f"""
                        SELECT autopost_role FROM {self.schema}.autopost_settings
                        WHERE user_id = %s AND group_link = %s 
                    """
                    cur.execute(query, (user_id, group_link))
                    result = cur.fetchone()
                    if result and result[0]:
                        return result[0]
                    else:
                        # Если роль автопостинга не установлена, возвращаем обычную роль пользователя
                        return self.get_gpt_role(user_id)
        except Exception as e:
            logger.error(f"Ошибка при получении роли автопостинга: {e}")
            raise

    def calculate_next_post_time(self) -> datetime:
        """
        Вычисляет время следующего поста:
        - 10 постов в день с 8:00 до 23:00 по московскому времени
        - Первый пост: случайное время между 8:10 и 8:50
        - Последующие посты: равномерно распределены с рандомизацией ±15 минут
        - Средний интервал: ~98 минут (1 час 38 минут)
        """
        from datetime import timedelta
        
        # Московская временная зона
        moscow_tz = pytz.timezone('Europe/Moscow')
        
        # Текущее время в Москве
        now_moscow = datetime.now(moscow_tz)
        
        # Рабочие часы (6:00 - 23:00)
        work_start = 6
        work_end = 23
        
        # Параметры для 10 постов в день
        posts_per_day = 10
        first_post_start_minutes = 10  # 8:10
        first_post_end_minutes = 50    # 8:50
        
        # Рассчитываем базовый интервал между постами
        # От 8:10 до 23:00 = 14 часов 50 минут = 890 минут
        # 10 постов = 9 интервалов между ними
        total_work_minutes = (work_end - work_start) * 60 - first_post_start_minutes  # 890 минут
        intervals_count = posts_per_day - 1  # 9 интервалов
        base_interval_minutes = total_work_minutes // intervals_count  # ~98 минут
        
        # Рандомизация интервала: ±15 минут
        jitter_minutes = 15
        interval_minutes = random.randint(
            base_interval_minutes - jitter_minutes,
            base_interval_minutes + jitter_minutes
        )
        
        logger.info(f"🕐 Текущее время в Москве: {now_moscow.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📊 Планируем {posts_per_day} постов в день")
        logger.info(f"⏱️ Базовый интервал: {base_interval_minutes} минут, с рандомом: {interval_minutes} минут")
        
        # Если сейчас рабочее время (8:00-23:00)
        if work_start <= now_moscow.hour < work_end:
            # Проверяем, был ли уже первый пост сегодня
            first_post_window_start = moscow_tz.localize(
                datetime.combine(now_moscow.date(), datetime.min.time()) + 
                timedelta(hours=work_start, minutes=first_post_start_minutes)
            )
            first_post_window_end = moscow_tz.localize(
                datetime.combine(now_moscow.date(), datetime.min.time()) + 
                timedelta(hours=work_start, minutes=first_post_end_minutes)
            )
            
            # Если сейчас время первого поста (8:10-8:50) или раньше
            if now_moscow <= first_post_window_end:
                # Планируем первый пост в случайное время в окне 8:10-8:50
                random_minutes = random.randint(first_post_start_minutes, first_post_end_minutes)
                next_time_moscow = moscow_tz.localize(
                    datetime.combine(now_moscow.date(), datetime.min.time()) + 
                    timedelta(hours=work_start, minutes=random_minutes)
                )
                
                # Если это время уже прошло, планируем через интервал
                if next_time_moscow <= now_moscow:
                    next_time_moscow = now_moscow + timedelta(minutes=interval_minutes)
                
                logger.info(f"🌅 Планируем первый пост дня: {next_time_moscow.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                # Планируем следующий пост через интервал
                next_time_moscow = now_moscow + timedelta(minutes=interval_minutes)
                logger.info(f"⏰ Планируем следующий пост через {interval_minutes} минут: {next_time_moscow.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Проверяем, не выходит ли запланированное время за рабочие часы
            if next_time_moscow.hour >= work_end:
                # Если выходит за 23:00, планируем первый пост следующего дня
                next_day = now_moscow.date() + timedelta(days=1)
                random_minutes = random.randint(first_post_start_minutes, first_post_end_minutes)
                next_time_moscow = moscow_tz.localize(
                    datetime.combine(next_day, datetime.min.time()) + 
                    timedelta(hours=work_start, minutes=random_minutes)
                )
                logger.info(f"📅 Следующий пост запланирован на следующий день: {next_time_moscow.strftime('%Y-%m-%d %H:%M:%S')}")
                
        else:
            # Если сейчас не рабочее время
            if now_moscow.hour >= work_end:
                # После 23:00 - планируем первый пост следующего дня
                next_day = now_moscow.date() + timedelta(days=1)
                random_minutes = random.randint(first_post_start_minutes, first_post_end_minutes)
                next_time_moscow = moscow_tz.localize(
                    datetime.combine(next_day, datetime.min.time()) + 
                    timedelta(hours=work_start, minutes=random_minutes)
                )
                logger.info(f"🌙 После рабочих часов, первый пост завтра: {next_time_moscow.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                # До 8:00 - планируем первый пост сегодня в окне 8:10-8:50
                random_minutes = random.randint(first_post_start_minutes, first_post_end_minutes)
                next_time_moscow = moscow_tz.localize(
                    datetime.combine(now_moscow.date(), datetime.min.time()) + 
                    timedelta(hours=work_start, minutes=random_minutes)
                )
                logger.info(f"🌅 До рабочих часов, первый пост сегодня: {next_time_moscow.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Конвертируем в UTC для сохранения в базе данных
        next_time_utc = next_time_moscow.astimezone(pytz.UTC).replace(tzinfo=None)
        
        logger.info(f"💾 Время следующего поста в UTC (для БД): {next_time_utc.strftime('%Y-%m-%d %H:%M:%S')}")
        
        return next_time_utc

    def update_next_post_time(self, group_link: str) -> None:
        """Обновляет время следующего поста для группы"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Получаем текущее время в Москве
                moscow_tz = pytz.timezone('Europe/Moscow')
                now_moscow = datetime.now(moscow_tz)
                
                # Проверяем, есть ли посты за сегодня
                posts_today = self.get_posts_today(group_link)
                
                if posts_today == 0:
                    # Если это первый пост дня, планируем между 8:10 и 8:50
                    random_minutes = random.randint(10, 50)  # случайные минуты от 10 до 50
                    next_post = now_moscow.replace(hour=8, minute=random_minutes, second=0, microsecond=0)
                    if next_post < now_moscow:
                        next_post = next_post + timedelta(days=1)
                    logger.info(f"🌅 Планируем первый пост дня: {next_post.strftime('%Y-%m-%d %H:%M')}")
                else:
                    # Если уже есть посты, планируем следующий с интервалом
                    base_interval = 98  # базовый интервал в минутах
                    random_interval = random.randint(-10, 10)  # случайное отклонение
                    total_interval = base_interval + random_interval
                    
                    next_post = now_moscow + timedelta(minutes=total_interval)
                    logger.info(f"⏱️ Базовый интервал: {base_interval} минут, с рандомом: {total_interval} минут")
                
                # Конвертируем в UTC для сохранения в БД
                next_post_utc = next_post.astimezone(pytz.UTC)
                
                # Обновляем время следующего поста
                update_query = f"""
                    UPDATE {self.schema}.autopost_settings
                    SET next_post_time = %s
                    WHERE group_link = %s
                """
                cur.execute(update_query, (next_post_utc, group_link))
                conn.commit()
                
                logger.info(f"⏰ Время следующего поста обновлено для {group_link}: {next_post_utc}")

    def set_next_post_time_now(self, user_id: int, group_link: str):
        """Устанавливает время следующего поста на текущее время (для тестирования)"""
        try:
            # Используем UTC время
            now_utc = datetime.utcnow()
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        UPDATE {self.schema}.autopost_settings 
                        SET next_post_time = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND group_link = %s
                    """, (now_utc, user_id, group_link))
                    
                    affected_rows = cur.rowcount
                    if affected_rows > 0:
                        logger.info(f"⏰ Время следующего поста установлено на СЕЙЧАС для {group_link}: {now_utc}")
                        return True
                    else:
                        logger.warning(f"⚠️ Не найдены настройки автопостинга для обновления времени (user={user_id}, group={group_link})")
                        return False
                        
        except Exception as e:
            logger.error(f"Ошибка при установке времени следующего поста на сейчас: {e}")
            return False

    def init_tables(self):
        """Инициализирует необходимые таблицы"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Создаем таблицу для отслеживания опубликованных постов
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}.published_posts (
                        id SERIAL PRIMARY KEY,
                        group_link TEXT NOT NULL,
                        post_link TEXT NOT NULL,
                        text TEXT NOT NULL,
                        post_date TIMESTAMP WITH TIME ZONE NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Проверяем наличие колонки text
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{self.schema}' 
                    AND table_name = 'published_posts' 
                    AND column_name = 'text'
                """)
                has_text_column = cur.fetchone() is not None
                
                if not has_text_column:
                    # Добавляем колонку text
                    cur.execute(f"""
                        ALTER TABLE {self.schema}.published_posts 
                        ADD COLUMN text TEXT NOT NULL DEFAULT ''
                    """)
                    logger.info("Добавлена колонка text в таблицу published_posts")
                
                # Создаем индекс для быстрого поиска
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_published_posts_group_date 
                    ON {self.schema}.published_posts(group_link, post_date)
                """)
                
                conn.commit()

    def add_published_post(self, group_link: str, text: str, post_link: str):
        """Добавляет запись об опубликованном посте в таблицу 'published_posts'."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                query = f"""
                    INSERT INTO {self.schema}.published_posts (group_link, text, post_link, post_date)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                """
                cur.execute(query, (group_link, text, post_link))
                conn.commit()
                logger.info(f"✅ В 'published_posts' добавлена запись для {group_link}")

    def get_posts_today(self, group_link: str) -> int:
        """Возвращает количество постов, опубликованных сегодня"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                moscow_tz = pytz.timezone('Europe/Moscow')
                now_moscow = datetime.now(moscow_tz)
                cur.execute(f"""
                    SELECT COUNT(*) 
                    FROM {self.schema}.published_posts 
                    WHERE group_link = %s 
                    AND post_date >= %s
                """, (group_link, now_moscow.date()))
                return cur.fetchone()[0]

    def set_posts_count(self, user_id: int, group_link: str, posts_count: int):
        """Устанавливает количество постов для анализа при автопостинге"""
        if not (1 <= posts_count <= 10):
            raise ValueError("Количество постов должно быть от 1 до 10")
            
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.schema}.autopost_settings 
                    SET posts_count = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = %s AND group_link = %s
                """, (posts_count, user_id, group_link))
                
                if cur.rowcount == 0:
                    logger.warning(f"Настройки автопостинга не найдены для user_id={user_id}, group_link={group_link}")
                    return False
                    
                conn.commit()
                logger.info(f"✅ Установлено количество постов для анализа: {posts_count} для группы {group_link}")
                return True

    def get_posts_count(self, user_id: int, group_link: str) -> int:
        """Получает настройку количества постов для анализа"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT posts_count 
                    FROM {self.schema}.autopost_settings 
                    WHERE user_id = %s AND group_link = %s
                """, (user_id, group_link))
                
                result = cur.fetchone()
                return result[0] if result else 5  # по умолчанию 5 постов

    def filter_posts_by_similarity(self, candidate_posts: list, published_posts: list, threshold: float = 0.7) -> list:
        """
        Фильтрует посты-кандидаты, исключая те, которые слишком похожи на уже опубликованные
        
        Args:
            candidate_posts: список постов-кандидатов
            published_posts: список уже опубликованных постов
            threshold: порог схожести (0.7 = 70% схожести)
            
        Returns:
            list: отфильтрованные уникальные посты
        """
        if not candidate_posts:
            return []
            
        logger.info(f"🔍 Фильтруем {len(candidate_posts)} постов-кандидатов через spaCy (порог схожести: {threshold})")
        
        # ЭТАП 1: Убираем дубликаты внутри самих кандидатов
        unique_candidates = self.filter_internal_duplicates(candidate_posts, threshold)
        logger.info(f"📊 После удаления внутренних дубликатов: {len(unique_candidates)} постов")
        
        # ЭТАП 2: Проверяем схожесть с уже опубликованными постами
        if not published_posts:
            return unique_candidates  # Если нет опубликованных постов, возвращаем уникальные кандидаты
            
        unique_posts = []
        published_texts = [post.get('text', '') for post in published_posts]
        
        for candidate in unique_candidates:
            candidate_text = candidate.get('text', '')
            if not candidate_text:
                continue
                
            is_unique = True
            max_similarity = 0.0
            
            # Проверяем схожесть с каждым опубликованным постом
            for published_text in published_texts:
                if self.compare_texts(candidate_text, published_text, threshold):
                    # Если найдено совпадение, пост не уникален
                    is_unique = False
                    
                    # Для логирования вычисляем точную схожесть
                    if nlp is not None:
                        try:
                            doc1 = nlp(candidate_text)
                            doc2 = nlp(published_text)
                            similarity = doc1.similarity(doc2)
                            max_similarity = max(max_similarity, similarity)
                        except:
                            pass
                    break
            
            if is_unique:
                unique_posts.append(candidate)
                logger.info(f"   ✅ Уникальный пост: {candidate_text[:50]}...")
            else:
                logger.info(f"   ❌ Дубликат с опубликованным (схожесть: {max_similarity:.2f}): {candidate_text[:50]}...")
        
        logger.info(f"📊 Результат полной фильтрации: {len(unique_posts)} уникальных постов из {len(candidate_posts)}")
        return unique_posts

    def filter_internal_duplicates(self, posts: list, threshold: float = 0.7) -> list:
        """
        Фильтрует дубликаты внутри списка постов, оставляя только самую "вовлекающую" версию.
        Сортирует посты по вовлеченности и проходит по ним, добавляя в итоговый список только те,
        которые не похожи на уже добавленные.
        """
        if not posts or nlp is None:
            if nlp is None:
                logger.warning("spaCy не загружен, пропускаем фильтрацию внутренних дубликатов.")
            return posts

        logger.info(f"🔍 Проверяем внутренние дубликаты среди {len(posts)} постов")

        # Сортируем посты по "вовлеченности" (лайки + комменты) в убывающем порядке
        sorted_posts = sorted(
            posts, 
            key=lambda p: p.get('likes', 0) + p.get('comments_count', 0), 
            reverse=True
        )

        unique_posts = []
        docs = {post['text']: nlp(post['text']) for post in sorted_posts}

        for candidate_post in sorted_posts:
            is_duplicate = False
            candidate_doc = docs.get(candidate_post['text'])

            if not (candidate_doc and candidate_doc.has_vector):
                unique_posts.append(candidate_post)
                continue

            for unique_post in unique_posts:
                unique_doc = docs.get(unique_post['text'])
                
                if not (unique_doc and unique_doc.has_vector):
                    continue

                try:
                    similarity = candidate_doc.similarity(unique_doc)
                    if similarity > threshold:
                        is_duplicate = True
                        logger.info(f"   - Отбрасываем внутренний дубликат (схожесть: {similarity:.2f})")
                        logger.info(f"     Кандидат: {candidate_post['text'][:60].strip()}...")
                        logger.info(f"     Уже есть: {unique_post['text'][:60].strip()}...")
                        break 
                except UserWarning as e:
                    logger.warning(f"[!] UserWarning при сравнении текстов: {e}")

            if not is_duplicate:
                unique_posts.append(candidate_post)

        removed_count = len(posts) - len(unique_posts)
        if removed_count > 0:
            logger.info(f"📊 Убрано {removed_count} внутренних дубликатов")
        
        logger.info(f"📊 После удаления внутренних дубликатов: {len(unique_posts)} постов")
        return unique_posts

    def get_gpt_roles(self, user_id: int, group_link: str = None) -> str:
        """
        Получает текущую GPT роль пользователя или создает дефолтную для нового пользователя
        
        Args:
            user_id: ID пользователя
            group_link: ссылка на группу (опционально)
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                if group_link:
                    # Если указана группа, ищем роль для конкретной группы
                    cur.execute(
                        f"SELECT autopost_role FROM {self.schema}.autopost_settings WHERE user_id = %s AND group_link = %s",
                        (user_id, group_link)
                    )
                else:
                    # Если группа не указана, берем первую найденную роль
                    cur.execute(
                        f"SELECT autopost_role FROM {self.schema}.autopost_settings WHERE user_id = %s AND autopost_role IS NOT NULL LIMIT 1",
                        (user_id,)
                    )
                
                result = cur.fetchone()
                
                if result and result[0]:  # Проверяем, что результат не None и не пустая строка
                    # Если роль уже есть, возвращаем её
                    return result[0]
                else:
                    # Если роли нет, возвращаем дефолтную
                    return "Ты — журналист и редактор."

    def set_blocked_topics(self, user_id: int, group_link: str, blocked_topics: str) -> None:
        """Устанавливает заблокированные темы для группы"""
        try:
            # Очищаем и нормализуем темы перед сохранением
            if blocked_topics:
                # Разбиваем по запятой, убираем пробелы и пустые значения
                topics = [topic.strip() for topic in blocked_topics.split(',') if topic.strip()]
                # Объединяем обратно в строку
                blocked_topics = ', '.join(topics) if topics else None
            else:
                blocked_topics = None  # Если передана пустая строка, сохраняем NULL
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        UPDATE {self.schema}.autopost_settings 
                        SET blocked_topics = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND group_link = %s
                    """, (blocked_topics, user_id, group_link))
                    
                    if cur.rowcount == 0:
                        # Если настройки не существуют, создаем их
                        cur.execute(f"""
                            INSERT INTO {self.schema}.autopost_settings 
                            (user_id, group_link, blocked_topics, mode, is_active)
                            VALUES (%s, %s, %s, 'automatic', true)
                        """, (user_id, group_link, blocked_topics))
                    
                    conn.commit()
                    logger.info(f"Сохранены заблокированные темы для {group_link}: {blocked_topics}")
                    
        except Exception as e:
            logger.error(f"Ошибка при сохранении заблокированных тем: {e}")
            raise

    def get_blocked_topics(self, user_id: int, group_link: str) -> str:
        """Получает заблокированные темы для группы"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT blocked_topics 
                        FROM {self.schema}.autopost_settings 
                        WHERE user_id = %s AND group_link = %s
                    """, (user_id, group_link))
                    result = cur.fetchone()
                    
                    # Возвращаем пустую строку вместо None
                    blocked_topics = result[0] if result and result[0] else ""
                    logger.info(f"Получены заблокированные темы для {group_link}: {blocked_topics}")
                    return blocked_topics
                    
        except Exception as e:
            logger.error(f"Ошибка при получении заблокированных тем: {e}")
            return ""

    async def check_content_blocked(self, text: str, blocked_topics: str) -> bool:
        """
        Проверяет, содержит ли текст заблокированные темы используя GPT для анализа.
        Эта функция теперь полностью асинхронна и безопасна для вызова.
        """
        if not blocked_topics or not text:
            return False
            
        try:
            from openai import AsyncOpenAI
            
            client = AsyncOpenAI(
                api_key=os.getenv('OPENAI_API_KEY'),
                base_url="https://api.openai.com/v1",
            )
            
            topics_list = [topic.strip() for topic in blocked_topics.split(',') if topic.strip()]
            topics_text = ', '.join(topics_list)
            
            logger.info(f"Проверяемый текст на блокировку: '{text}'")

            prompt = f"""
            Твоя задача — проанализировать текст и определить, соответствует ли его ОСНОВНАЯ СУТЬ заблокированным темам. Игнорируй стандартные подписи в конце поста, такие как "Прислать новость" или "Подписаться на канал".

            Заблокированные темы: {topics_text}

            Текст для анализа:
            ---
            {text}
            ---

            Инструкции по анализу:
            1.  **Анализируй суть**: Определи главную тему поста. Блокируй, только если эта главная тема совпадает с одной из запрещенных.
            2.  **Реклама**: Запрещена только сторонняя коммерческая реклама (продажа товаров, услуг). **Не считай рекламой призывы подписаться на исходный канал, предложения прислать новость или ссылки на другие посты этого же канала.**
            3.  **Гороскопы**: Блокируй только астрологические прогнозы по знакам зодиака. **Прогноз погоды не является гороскопом.**

            Ответь только "ДА", если ОСНОВНАЯ СУТЬ текста соответствует заблокированным темам. В противном случае ответь "НЕТ".
            """
            
            response = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=10,
                temperature=0
            )
            
            result = response.choices[0].message.content.strip().upper()
            is_blocked = "ДА" in result
            
            logger.info(f"GPT анализ текста на заблокированные темы: {result} (заблокирован: {is_blocked})")
            await client.close()
            return is_blocked
                
        except Exception as e:
            logger.error(f"Ошибка при GPT анализе заблокированных тем: {e}")
            # Fallback на старую логику поиска слов
            return self._simple_check_content_blocked(text, blocked_topics)

    def _simple_check_content_blocked(self, text: str, blocked_topics: str) -> bool:
        """Простая проверка на наличие слов в тексте (fallback)"""
        if not blocked_topics or not text:
            return False
            
        try:
            text = text.lower()
            topics = [topic.strip().lower() for topic in blocked_topics.split(',') if topic.strip()]
            
            logger.info(f"Простая проверка текста на заблокированные темы: {topics}")
            
            for topic in topics:
                if topic in text:
                    logger.info(f"Найдена заблокированная тема '{topic}' в тексте")
                    return True
                    
            logger.info("Заблокированные темы не найдены в тексте")
            return False
                
        except Exception as e:
            logger.error(f"Ошибка при простой проверке заблокированных тем: {e}")
            return False

    def set_autopost_settings(self, user_id: int, group_link: str, source_selection_mode: str, selected_sources: list = None, autopost_role: str = None, blocked_topics: str = None) -> None:
        """Сохраняет настройки автопостинга"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Проверяем существующие настройки
                    cur.execute(f"""
                        SELECT id FROM {self.schema}.autopost_settings 
                        WHERE user_id = %s AND group_link = %s
                    """, (user_id, group_link))
                    exists = cur.fetchone()
                    
                    if exists:
                        # Обновляем существующие настройки
                        cur.execute(f"""
                            UPDATE {self.schema}.autopost_settings 
                            SET source_selection_mode = %s,
                                selected_sources = %s,
                                autopost_role = %s,
                                blocked_topics = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE user_id = %s AND group_link = %s
                        """, (source_selection_mode, selected_sources, autopost_role, blocked_topics, user_id, group_link))
                    else:
                        # Создаем новые настройки
                        cur.execute(f"""
                            INSERT INTO {self.schema}.autopost_settings 
                            (user_id, group_link, source_selection_mode, selected_sources, autopost_role, blocked_topics, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """, (user_id, group_link, source_selection_mode, selected_sources, autopost_role, blocked_topics))
                    
                    logger.info(f"Настройки автопостинга сохранены для {group_link}")
                    
        except Exception as e:
            logger.error(f"Ошибка при сохранении настроек автопостинга: {e}")
            raise

    def set_source_selection_mode(self, user_id: int, group_link: str, mode: str):
        """Устанавливает режим выбора источников (auto/manual)"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        UPDATE {self.schema}.autopost_settings
                        SET source_selection_mode = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND group_link = %s
                    """, (mode, user_id, group_link))
                    conn.commit()
                    logger.info(f"Режим выбора источников для {group_link} изменен на {mode}")
        except Exception as e:
            logger.error(f"Ошибка при установке режима выбора источников: {e}")
            raise

    def get_post_from_queue(self, queue_id: int) -> dict:
        """Получает пост из очереди по ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query = f"""
                        SELECT id, user_id, group_link, post_text, post_image, is_video, scheduled_time, status, mode, original_post_url
                        FROM {self.schema}.autopost_queue 
                        WHERE id = %s
                    """
                    cur.execute(query, (queue_id,))
                    result = cur.fetchone()
                    
                    if result:
                        columns = ['id', 'user_id', 'group_link', 'post_text', 'post_image', 'is_video', 'scheduled_time', 'status', 'mode', 'original_post_url']
                        return dict(zip(columns, result))
                    else:
                        logger.warning(f"Пост с ID {queue_id} не найден в очереди")
                        return None
        except Exception as e:
            logger.error(f"Ошибка при получении поста из очереди: {e}")
            return None

    def update_queued_post_text(self, queue_id: int, new_text: str) -> bool:
        """Обновляет текст поста в очереди"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query = f"""
                        UPDATE {self.schema}.autopost_queue 
                        SET post_text = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """
                    cur.execute(query, (new_text, queue_id))
                    affected_rows = cur.rowcount
                    
                    conn.commit()
                    
                    logger.info(f"✅ Текст поста ID {queue_id} обновлен")
                    return affected_rows > 0
        except Exception as e:
            logger.error(f"Ошибка при обновлении текста поста в очереди: {e}")
            return False

    def approve_post_in_queue(self, queue_id: int) -> bool:
        """
        Одобряет пост в очереди:
        1. Меняет статус на 'approved'.
        2. Устанавливает время публикации на текущее, чтобы он был опубликован немедленно.
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query = f"""
                        UPDATE {self.schema}.autopost_queue 
                        SET status = 'approved', 
                            scheduled_time = CURRENT_TIMESTAMP, 
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s AND status = 'sent_for_approval'
                    """
                    cur.execute(query, (queue_id,))
                    affected_rows = cur.rowcount
                    
                    conn.commit()
                    
                    if affected_rows > 0:
                        logger.info(f"✅ Пост ID {queue_id} одобрен и готов к немедленной публикации.")
                        return True
                    else:
                        logger.warning(f"⚠️ Не удалось одобрить пост ID {queue_id}. Возможно, он уже был обработан или имеет неверный статус.")
                        return False
        except Exception as e:
            logger.error(f"Ошибка при одобрении поста в очереди: {e}")
            return False

    def get_user_sources(self, user_id):
        # ... (существующий код функции)
        return sources

    def update_sources_themes(self, user_id: int, source_ids: List[int], themes: List[str]) -> bool:
        """Обновляет темы для нескольких источников одного пользователя."""
        if not source_ids:
            return False
        
        query = f"""
            UPDATE {self.schema}.links
            SET themes = %s
            WHERE user_id = %s AND id = ANY(%s)
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (themes, user_id, source_ids))
                    updated_rows = cur.rowcount
                    conn.commit()
            logger.info(f"Для пользователя {user_id} обновлены темы у {updated_rows} источников.")
            return updated_rows > 0
        except Exception as e:
            logger.error(f"Ошибка при обновлении тем источников для пользователя {user_id}: {e}")
            return False

    def delete_source(self, user_id: int, source_id: int) -> bool:
        """Удаляет источник по его ID для конкретного пользователя."""
        query = f"DELETE FROM {self.schema}.links WHERE user_id = %s AND id = %s"