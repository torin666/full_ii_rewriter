import psycopg2
import logging
from typing import List, Dict, Optional
from aiogram.fsm.state import State, StatesGroup
from datetime import datetime
import json

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
            "user": env('USER_DB'),
            "password": env('USER_PWD')
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
                        is_active BOOLEAN DEFAULT TRUE,
                        source_selection_mode TEXT DEFAULT 'auto' CHECK (source_selection_mode IN ('auto', 'manual')),
                        selected_sources TEXT, -- JSON массив ID источников для ручного режима
                        next_post_time TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(user_id, group_link)
                    )
                """)

                # Создаем таблицу для очереди автопостинга
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}.autopost_queue (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        group_link TEXT NOT NULL,
                        post_text TEXT NOT NULL,
                        post_image TEXT,
                        scheduled_time TIMESTAMP NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent_for_approval', 'approved', 'published', 'cancelled', 'publishing', 'failed')),
                        is_video BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Проверяем и добавляем недостающие колонки
                try:
                    # Проверяем есть ли колонка is_video в autopost_queue
                    cur.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = '{self.schema}' 
                        AND table_name = 'autopost_queue' 
                        AND column_name = 'is_video'
                    """)
                    
                    if not cur.fetchone():
                        logger.info("Добавляем недостающую колонку is_video в autopost_queue")
                        cur.execute(f"""
                            ALTER TABLE {self.schema}.autopost_queue 
                            ADD COLUMN is_video BOOLEAN DEFAULT FALSE
                        """)
                    
                    # Проверяем есть ли колонки для выбора источников в autopost_settings
                    cur.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = '{self.schema}' 
                        AND table_name = 'autopost_settings' 
                        AND column_name = 'source_selection_mode'
                    """)
                    
                    if not cur.fetchone():
                        logger.info("Добавляем недостающие колонки для выбора источников в autopost_settings")
                        cur.execute(f"""
                            ALTER TABLE {self.schema}.autopost_settings 
                            ADD COLUMN source_selection_mode TEXT DEFAULT 'auto' CHECK (source_selection_mode IN ('auto', 'manual'))
                        """)
                        cur.execute(f"""
                            ALTER TABLE {self.schema}.autopost_settings 
                            ADD COLUMN selected_sources TEXT
                        """)
                        
                except Exception as e:
                    logger.error(f"Ошибка при добавлении колонок: {e}")
                
                # Обновляем constraint для status чтобы разрешить новые статусы
                try:
                    logger.info("🔄 Начинаем обновление constraint для autopost_queue...")
                    
                    # Сначала удаляем старый constraint
                    cur.execute(f"""
                        ALTER TABLE {self.schema}.autopost_queue 
                        DROP CONSTRAINT IF EXISTS autopost_queue_status_check
                    """)
                    logger.info("🗑️ Старый constraint удален")
                    
                    # Добавляем новый constraint с расширенным списком статусов
                    cur.execute(f"""
                        ALTER TABLE {self.schema}.autopost_queue 
                        ADD CONSTRAINT autopost_queue_status_check 
                        CHECK (status IN ('pending', 'sent_for_approval', 'approved', 'published', 'cancelled', 'publishing', 'failed'))
                    """)
                    logger.info("✅ Новый constraint добавлен")
                    
                    logger.info("✅ Обновлен constraint для статусов autopost_queue")
                except Exception as e:
                    logger.error(f"❌ Ошибка при обновлении constraint: {e}")
                    import traceback
                    logger.error(f"Traceback: {traceback.format_exc()}")
                
                conn.commit()
                logger.info("База данных успешно инициализирована")

    def get_connection(self):
        return psycopg2.connect(**self.conn_params)

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
        doc1 = nlp(text1)
        doc2 = nlp(text2)
        similarity = doc1.similarity(doc2)
        return similarity >= threshold

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
                    SELECT text, photo_url FROM {self.schema}.posts
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
                    SELECT text FROM {self.schema}.posts
                    WHERE using_post = 'True'
                """
                cur.execute(used_posts_query)
                used_posts = [post[0] for post in cur.fetchall()]
                print("Использованные посты:", used_posts)

                top_post = None
                for candidate in top_posts:
                    candidate_text = candidate[0]
                    if not used_posts:
                        top_post = candidate
                        break
                    is_unique = True
                    for used in used_posts:
                        if self.compare_texts(candidate_text, used):
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
                            WHERE text = %s
                        """
                        cur.execute(mark_as_used_query, (post[0],))
                conn.commit()
                # Возвращаем (text, photo_url)
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
                # Проверяем, есть ли уже роль у пользователя
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

    def get_user_groups(self, user_id: int) -> list:
        """Получает все паблики пользователя"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT id, group_link, themes 
                    FROM {self.schema}.user_groups 
                    WHERE user_id = %s
                """, (user_id,))
                columns = ['id', 'group_link', 'themes']
                return [dict(zip(columns, row)) for row in cur.fetchall()]

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
        """
        Получает посты с похожими темами для группы пользователя
        Учитывает настройки выбора источников (автоматический или ручной)
        """
        try:
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                    # Сначала получаем настройки автопостинга для этой группы
                    cur.execute(f"""
                        SELECT source_selection_mode, selected_sources 
                        FROM {self.schema}.autopost_settings 
                        WHERE user_id = %s AND group_link = %s
                    """, (user_id, group_link))
                    
                    autopost_settings = cur.fetchone()
                    
                    if autopost_settings and autopost_settings[0] == 'manual' and autopost_settings[1]:
                        # Ручной режим - используем выбранные источники
                        selected_sources = json.loads(autopost_settings[1])
                        logger.info(f"Используем ручной выбор источников: {selected_sources}")
                        
                        if not selected_sources:
                            logger.warning(f"Нет выбранных источников для группы {group_link}")
                            return []
                        
                        # Создаем плейсхолдеры для IN запроса
                        placeholders = ','.join(['%s'] * len(selected_sources))
                        
                query = f"""
                            SELECT p.text, p.post_link, p.likes, p.views, p.comments_count, 
                                   p.comments_likes, p.photo_url, p.group_link
                            FROM {self.schema}.posts p
                            JOIN {self.schema}.links l ON p.group_link = l.link
                            WHERE l.user_id = %s 
                            AND l.id = ANY(%s)
                            AND (p.using_post IS NULL OR p.using_post != 'True')
                            ORDER BY (p.likes + p.comments_count) DESC
                            LIMIT 50
                        """
                        
                        cur.execute(query, (user_id, selected_sources))
                        
                    else:
                        # Автоматический режим - используем похожие темы (старая логика)
                        logger.info(f"Используем автоматический выбор источников по темам")
                        
                        # Получаем темы группы пользователя
                        cur.execute(f"""
                    SELECT themes FROM {self.schema}.user_groups 
                    WHERE user_id = %s AND group_link = %s
                        """, (user_id, group_link))
                        
                result = cur.fetchone()
                if not result:
                            logger.warning(f"Группа {group_link} не найдена для пользователя {user_id}")
                    return []
                
                group_themes = result[0]
                        if isinstance(group_themes, str):
                            try:
                                group_themes = json.loads(group_themes)
                            except:
                                group_themes = [group_themes]
                
                        logger.info(f"Темы группы {group_link}: {group_themes}")
                
                        # Получаем посты из источников с похожими темами
                        query = f"""
                            SELECT p.text, p.post_link, p.likes, p.views, p.comments_count, 
                                   p.comments_likes, p.photo_url, p.group_link
                            FROM {self.schema}.posts p
                            JOIN {self.schema}.links l ON p.group_link = l.link
                            WHERE l.user_id = %s 
                            AND (p.using_post IS NULL OR p.using_post != 'True')
                """
                        
                        # Добавляем условия для каждой темы
                        theme_conditions = []
                        params = [user_id]
                        
                        for theme in group_themes:
                            # Поскольку themes в links хранится как массив
                            theme_conditions.append("%s = ANY(l.themes)")
                            params.append(theme)
                        
                        if theme_conditions:
                            query += " AND (" + " OR ".join(theme_conditions) + ")"
                        
                        query += """
                            ORDER BY (p.likes + p.comments_count) DESC
                            LIMIT 50
                        """
                        
                        cur.execute(query, params)
                    
                    # Обрабатываем результаты одинаково для обоих режимов
                    posts = []
                    for row in cur.fetchall():
                        posts.append({
                            'text': row[0],
                            'post_link': row[1],
                            'likes': row[2] or 0,
                            'views': row[3] or 0,
                            'comments_count': row[4] or 0,
                            'comments_likes': row[5] or 0,
                            'photo_url': row[6],
                            'group_link': row[7]
                        })
                    
                    logger.info(f"Найдено {len(posts)} подходящих постов для группы {group_link}")
                    return posts
                    
        except Exception as e:
            logger.error(f"Ошибка при получении постов для группы {group_link}: {e}")
            return []

    def mark_post_as_used(self, text: str) -> None:
        """Помечает пост как использованный после переписывания через GPT"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Проверяем текущее значение
                cur.execute(f"""
                    SELECT id, using_post, group_link FROM {self.schema}.posts
                    WHERE text = %s
                """, (text,))
                current_post = cur.fetchone()
                
                if current_post:
                    logger.info(f"🔒 Помечаем пост как использованный: ID={current_post[0]}, группа={current_post[2]}, using_post={current_post[1]} -> TRUE")
                else:
                    logger.warning(f"⚠️ Пост для пометки как использованный не найден")
                    return

                # Обновляем значение  
                cur.execute(f"""
                    UPDATE {self.schema}.posts
                    SET using_post = 'True'
                    WHERE text = %s
                """, (text,))
                
                if cur.rowcount > 0:
                    logger.info(f"✅ Пост успешно помечен как использованный")
                else:
                    logger.error(f"❌ Не удалось пометить пост как использованный")
                
                conn.commit()

    # ===== МЕТОДЫ ДЛЯ АВТОПОСТИНГА =====

    def add_autopost_setting(self, user_id: int, group_link: str, mode: str, 
                           source_selection_mode: str = 'auto', selected_sources: list = None) -> None:
        """
        Добавить настройки автопостинга
        
        Args:
            user_id: ID пользователя
            group_link: ссылка на группу
            mode: режим ('automatic' или 'controlled')
            source_selection_mode: режим выбора источников ('auto' или 'manual')
            selected_sources: список ID выбранных источников (для manual режима)
        """
        try:
            # Преобразуем список источников в JSON
            selected_sources_json = json.dumps(selected_sources) if selected_sources else None
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        INSERT INTO {self.schema}.autopost_settings 
                        (user_id, group_link, mode, source_selection_mode, selected_sources)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id, group_link) 
                        DO UPDATE SET 
                            mode = EXCLUDED.mode,
                            source_selection_mode = EXCLUDED.source_selection_mode,
                            selected_sources = EXCLUDED.selected_sources,
                            updated_at = CURRENT_TIMESTAMP
                    """, (user_id, group_link, mode, source_selection_mode, selected_sources_json))
                    
                    logger.info(f"Настройки автопостинга добавлены/обновлены для user {user_id}, group {group_link}")
                    
        except Exception as e:
            logger.error(f"Ошибка при добавлении настроек автопостинга: {e}")
            raise

    def get_autopost_settings(self, user_id: int) -> list:
        """
        Получает все настройки автопостинга пользователя
        
        Args:
            user_id: ID пользователя
            
        Returns:
            list: Список настроек автопостинга
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT group_link, mode, is_active, next_post_time, created_at 
                        FROM {self.schema}.autopost_settings 
                        WHERE user_id = %s
                        ORDER BY created_at DESC
                    """, (user_id,))
                    
                    results = cur.fetchall()
                    return [
                        {
                            'group_link': row[0],
                            'mode': row[1],
                            'is_active': row[2],
                            'next_post_time': row[3],
                            'created_at': row[4]
                        }
                        for row in results
                    ]
                    
        except Exception as e:
            logger.error(f"Ошибка при получении настроек автопостинга: {e}")
            return []

    def update_autopost_mode(self, user_id: int, group_link: str, mode: str) -> None:
        """
        Обновляет режим автопостинга для группы
        
        Args:
            user_id: ID пользователя
            group_link: Ссылка на группу
            mode: Новый режим автопостинга
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        UPDATE {self.schema}.autopost_settings 
                        SET mode = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND group_link = %s
                    """, (mode, user_id, group_link))
                    
                    logger.info(f"Режим автопостинга изменен на {mode} для группы {group_link}")
                    
        except Exception as e:
            logger.error(f"Ошибка при обновлении режима автопостинга: {e}")
            raise

    def toggle_autopost_status(self, user_id: int, group_link: str, is_active: bool) -> None:
        """
        Включает/выключает автопостинг для группы
        
        Args:
            user_id: ID пользователя  
            group_link: Ссылка на группу
            is_active: Статус активности
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        UPDATE {self.schema}.autopost_settings 
                        SET is_active = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND group_link = %s
                    """, (is_active, user_id, group_link))
                    
                    status = "включен" if is_active else "выключен"
                    logger.info(f"Автопостинг {status} для группы {group_link}")
                    
        except Exception as e:
            logger.error(f"Ошибка при изменении статуса автопостинга: {e}")
            raise

    def delete_autopost_setting(self, user_id: int, group_link: str) -> None:
        """
        Удаляет настройки автопостинга для группы
        
        Args:
            user_id: ID пользователя
            group_link: Ссылка на группу
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        DELETE FROM {self.schema}.autopost_settings 
                        WHERE user_id = %s AND group_link = %s
                    """, (user_id, group_link))
                    
                    logger.info(f"Автопостинг удален для группы {group_link}")
                    
        except Exception as e:
            logger.error(f"Ошибка при удалении настроек автопостинга: {e}")
            raise

    def add_autopost_to_queue(self, user_id: int, group_link: str, post_text: str, 
                             post_image: str, scheduled_time, is_video: bool = False) -> int:
        """
        Добавляет пост в очередь автопостинга
        
        Args:
            user_id: ID пользователя
            group_link: Ссылка на группу
            post_text: Текст поста
            post_image: URL изображения
            scheduled_time: Время публикации
            is_video: Является ли медиафайл видео
            
        Returns:
            int: ID созданной записи в очереди
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        INSERT INTO {self.schema}.autopost_queue 
                        (user_id, group_link, post_text, post_image, scheduled_time, is_video) 
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (user_id, group_link, post_text, post_image, scheduled_time, is_video))
                    
                    queue_id = cur.fetchone()[0]
                    logger.info(f"Пост добавлен в очередь автопостинга с ID {queue_id}")
                    return queue_id
                    
        except Exception as e:
            logger.error(f"Ошибка при добавлении поста в очередь: {e}")
            raise

    def update_queue_status(self, queue_id: int, status: str) -> None:
        """
        Обновляет статус поста в очереди
        
        Args:
            queue_id: ID записи в очереди
            status: Новый статус
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Атомарно обновляем статус
                    if status in ('published', 'failed'):
                        # Для финальных статусов разрешаем переход из любого статуса кроме финальных
                        cur.execute(f"""
                            UPDATE {self.schema}.autopost_queue 
                            SET status = %s
                            WHERE id = %s AND status NOT IN ('published', 'failed', 'cancelled')
                        """, (status, queue_id))
                    else:
                        # Для промежуточных статусов блокируем изменение финальных
                        cur.execute(f"""
                            UPDATE {self.schema}.autopost_queue 
                            SET status = %s
                            WHERE id = %s AND status NOT IN ('published', 'failed', 'cancelled')
                        """, (status, queue_id))
                    
                    if cur.rowcount > 0:
                        logger.info(f"✅ Статус поста {queue_id} изменен на {status}")
                    else:
                        logger.warning(f"⚠️ Пост {queue_id} уже был обработан или не найден")
                    
        except Exception as e:
            logger.error(f"Ошибка при обновлении статуса очереди: {e}")
            raise

    def get_active_autopost_groups(self) -> list:
        """
        Получает все активные группы для автопостинга
        
        Returns:
            list: Список активных групп с настройками
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT user_id, group_link, mode, next_post_time 
                        FROM {self.schema}.autopost_settings 
                        WHERE is_active = TRUE
                    """)
                    
                    results = cur.fetchall()
                    return [
                        {
                            'user_id': row[0],
                            'group_link': row[1],
                            'mode': row[2],
                            'next_post_time': row[3]
                        }
                        for row in results
                    ]
                    
        except Exception as e:
            logger.error(f"Ошибка при получении активных групп автопостинга: {e}")
            return []
    
    def get_pending_autopost_queue(self) -> list:
        """
        Получает посты из очереди готовые к публикации
        
        Returns:
            list: Список постов готовых к публикации
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT id, user_id, group_link, post_text, post_image, 
                               scheduled_time, status, is_video, created_at
                        FROM {self.schema}.autopost_queue 
                        WHERE status IN ('pending', 'approved')
                        ORDER BY scheduled_time ASC
                        FOR UPDATE SKIP LOCKED
                    """)
                    
                    results = cur.fetchall()
                    return [
                        {
                            'id': row[0],
                            'user_id': row[1],
                            'group_link': row[2],
                            'post_text': row[3],
                            'post_image': row[4],
                            'scheduled_time': row[5],
                            'status': row[6],
                            'is_video': row[7],
                            'created_at': row[8]
                        }
                        for row in results
                    ]
                    
        except Exception as e:
            logger.error(f"Ошибка при получении очереди автопостинга: {e}")
            return []
    
    def approve_autopost_in_queue(self, user_id: int, group_link: str) -> bool:
        """
        Одобряет автопост в очереди
        
        Args:
            user_id: ID пользователя
            group_link: Ссылка на группу
            
        Returns:
            bool: Успешность операции
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Сначала находим и блокируем пост
                    cur.execute(f"""
                        SELECT id FROM {self.schema}.autopost_queue 
                        WHERE user_id = %s AND group_link = %s AND status = 'pending'
                        ORDER BY scheduled_time DESC 
                        LIMIT 1
                        FOR UPDATE
                    """, (user_id, group_link))
                    
                    result = cur.fetchone()
                    if not result:
                        logger.warning(f"Не найден pending пост для одобрения: user={user_id}, group={group_link}")
                        return False
                    
                    post_id = result[0]
                    
                    # Теперь обновляем статус конкретного поста
                    cur.execute(f"""
                        UPDATE {self.schema}.autopost_queue 
                        SET status = 'approved'
                        WHERE id = %s AND status = 'pending'
                    """, (post_id,))
                    
                    success = cur.rowcount > 0
                    if success:
                        logger.info(f"✅ Пост {post_id} одобрен для группы {group_link}")
                    else:
                        logger.warning(f"❌ Не удалось одобрить пост {post_id} (возможно уже одобрен)")
                    
                    return success
                    
        except Exception as e:
            logger.error(f"Ошибка при одобрении автопоста: {e}")
            return False
    
    def update_autopost_in_queue(self, user_id: int, group_link: str, new_text: str) -> bool:
        """
        Обновляет текст автопоста в очереди
        
        Args:
            user_id: ID пользователя
            group_link: Ссылка на группы
            new_text: Новый текст поста
            
        Returns:
            bool: Успешность операции
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        UPDATE {self.schema}.autopost_queue 
                        SET post_text = %s
                        WHERE id = (
                            SELECT id FROM {self.schema}.autopost_queue 
                            WHERE user_id = %s AND group_link = %s AND status = 'pending'
                            ORDER BY scheduled_time DESC 
                            LIMIT 1
                        )
                    """, (new_text, user_id, group_link))
                    
                    return cur.rowcount > 0
                    
        except Exception as e:
            logger.error(f"Ошибка при обновлении автопоста: {e}")
            return False
    
    def cancel_autopost_in_queue(self, user_id: int, group_link: str) -> bool:
        """
        Отменяет автопост в очереди
        
        Args:
            user_id: ID пользователя
            group_link: Ссылка на группу
            
        Returns:
            bool: Успешность операции
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Сначала проверяем, есть ли посты для отмены
                    cur.execute(f"""
                        SELECT id, status, scheduled_time, created_at 
                        FROM {self.schema}.autopost_queue 
                        WHERE user_id = %s AND group_link = %s
                        ORDER BY scheduled_time DESC 
                    """, (user_id, group_link))
                    
                    all_posts = cur.fetchall()
                    logger.info(f"🔍 Найдено {len(all_posts)} постов для user={user_id}, group={group_link}")
                    
                    for post in all_posts:
                        logger.info(f"   Пост ID={post[0]}, status={post[1]}, scheduled={post[2]}, created={post[3]}")
                    
                    # Ищем pending пост для отмены
                    cur.execute(f"""
                            SELECT id FROM {self.schema}.autopost_queue 
                            WHERE user_id = %s AND group_link = %s AND status = 'pending'
                            ORDER BY scheduled_time DESC 
                            LIMIT 1
                    """, (user_id, group_link))
                    
                    result = cur.fetchone()
                    if not result:
                        logger.warning(f"❌ Не найден pending пост для отмены: user={user_id}, group={group_link}")
                        return False
                    
                    post_id = result[0]
                    logger.info(f"🎯 Найден pending пост для отмены: ID={post_id}")
                    
                    # Обновляем статус конкретного поста
                    cur.execute(f"""
                        UPDATE {self.schema}.autopost_queue 
                        SET status = 'cancelled'
                        WHERE id = %s AND status = 'pending'
                    """, (post_id,))
                    
                    success = cur.rowcount > 0
                    if success:
                        logger.info(f"✅ Пост {post_id} успешно отменен")
                    else:
                        logger.warning(f"❌ Не удалось отменить пост {post_id} (возможно уже обработан)")
                    
                    return success
                    
        except Exception as e:
            logger.error(f"❌ Ошибка при отмене автопоста: {e}")
            return False
    
    def has_pending_autopost(self, user_id: int, group_link: str) -> bool:
        """
        Проверяет есть ли ожидающий автопост для группы
        
        Args:
            user_id: ID пользователя
            group_link: Ссылка на группу
            
        Returns:
            bool: Есть ли ожидающий пост
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT COUNT(*) 
                        FROM {self.schema}.autopost_queue 
                        WHERE user_id = %s AND group_link = %s 
                        AND status IN ('pending', 'approved')
                    """, (user_id, group_link))
                    
                    result = cur.fetchone()
                    return result[0] > 0
                    
        except Exception as e:
            logger.error(f"Ошибка при проверке ожидающих автопостов: {e}")
            return False