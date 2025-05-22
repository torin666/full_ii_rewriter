import sqlite3
import logging
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.conn = sqlite3.connect('database.db')
        self.cursor = self.conn.cursor()
        self.init_db()

    def init_db(self):
        """Инициализация базы данных"""
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                source_url TEXT,
                theme TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_url TEXT,
                text TEXT,
                likes INTEGER DEFAULT 0,
                comments_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER,
                text TEXT,
                likes INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (post_id) REFERENCES posts (id)
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS gpt_roles (
                user_id INTEGER PRIMARY KEY,
                role TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        self.conn.commit()

    def add_user(self, user_id: int, username: str = None):
        """Добавление пользователя"""
        try:
            self.cursor.execute(
                'INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)',
                (user_id, username)
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при добавлении пользователя: {str(e)}")

    def add_source(self, source: Dict):
        """Добавление источника"""
        try:
            self.cursor.execute(
                'INSERT INTO sources (user_id, source_url, theme) VALUES (?, ?, ?)',
                (source['user_id'], source['source_url'], source['theme'])
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при добавлении источника: {str(e)}")

    def add_post(self, source_url: str, text: str, likes: int = 0, comments_count: int = 0):
        """Добавление поста"""
        try:
            self.cursor.execute(
                'INSERT INTO posts (source_url, text, likes, comments_count) VALUES (?, ?, ?, ?)',
                (source_url, text, likes, comments_count)
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при добавлении поста: {str(e)}")

    def add_comment(self, post_id: int, text: str, likes: int = 0):
        """Добавление комментария"""
        try:
            self.cursor.execute(
                'INSERT INTO comments (post_id, text, likes) VALUES (?, ?, ?)',
                (post_id, text, likes)
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при добавлении комментария: {str(e)}")

    def get_user_sources(self, user_id: int) -> List[Dict]:
        """Получение источников пользователя"""
        try:
            self.cursor.execute(
                'SELECT * FROM sources WHERE user_id = ?',
                (user_id,)
            )
            sources = self.cursor.fetchall()
            return [
                {
                    'id': source[0],
                    'user_id': source[1],
                    'source_url': source[2],
                    'theme': source[3],
                    'created_at': source[4]
                }
                for source in sources
            ]
        except Exception as e:
            logger.error(f"Ошибка при получении источников пользователя: {str(e)}")
            return []

    def get_active_sources(self) -> List[Dict]:
        """Получение всех активных источников"""
        try:
            self.cursor.execute('SELECT * FROM sources')
            sources = self.cursor.fetchall()
            return [
                {
                    'id': source[0],
                    'user_id': source[1],
                    'source_url': source[2],
                    'theme': source[3],
                    'created_at': source[4]
                }
                for source in sources
            ]
        except Exception as e:
            logger.error(f"Ошибка при получении активных источников: {str(e)}")
            return []

    def get_posts_since(self, since: datetime) -> List[Dict]:
        """Получение постов с определенной даты"""
        try:
            self.cursor.execute(
                'SELECT * FROM posts WHERE created_at >= ?',
                (since.isoformat(),)
            )
            posts = self.cursor.fetchall()
            return [
                {
                    'id': post[0],
                    'source_url': post[1],
                    'text': post[2],
                    'likes': post[3],
                    'comments_count': post[4],
                    'created_at': post[5]
                }
                for post in posts
            ]
        except Exception as e:
            logger.error(f"Ошибка при получении постов: {str(e)}")
            return []

    def get_comments_since(self, since: datetime) -> List[Dict]:
        """Получение комментариев с определенной даты"""
        try:
            self.cursor.execute(
                'SELECT * FROM comments WHERE created_at >= ?',
                (since.isoformat(),)
            )
            comments = self.cursor.fetchall()
            return [
                {
                    'id': comment[0],
                    'post_id': comment[1],
                    'text': comment[2],
                    'likes': comment[3],
                    'created_at': comment[4]
                }
                for comment in comments
            ]
        except Exception as e:
            logger.error(f"Ошибка при получении комментариев: {str(e)}")
            return []

    def get_all_users(self) -> List[Dict]:
        """Получение всех пользователей"""
        try:
            self.cursor.execute('SELECT * FROM users')
            users = self.cursor.fetchall()
            return [
                {
                    'user_id': user[0],
                    'username': user[1],
                    'created_at': user[2]
                }
                for user in users
            ]
        except Exception as e:
            logger.error(f"Ошибка при получении пользователей: {str(e)}")
            return []

    def set_gpt_role(self, user_id: int, role: str):
        """Устанавливает роль GPT для пользователя"""
        with self.conn as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO gpt_roles (user_id, role)
                VALUES (?, ?)
                ON CONFLICT(user_id) DO UPDATE SET role = ?
            """, (user_id, role, role))
            conn.commit()

    def get_gpt_role(self, user_id: int) -> str:
        """Получает роль GPT для пользователя"""
        with self.conn as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT role FROM gpt_roles WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            return result[0] if result else None

    def get_post(self, user_id: int = None) -> Dict:
        """Получает пост для анализа"""
        try:
            with self.conn as conn:
                cursor = conn.cursor()
                
                # Получаем роль GPT для пользователя
                role = self.get_gpt_role(user_id) if user_id else None
                
                # Базовый запрос
                query = """
                    SELECT p.*, s.theme 
                    FROM posts p
                    JOIN sources s ON p.source_url = s.source_url
                    WHERE p.analyzed = 0
                    ORDER BY p.created_at DESC
                    LIMIT 1
                """
                
                cursor.execute(query)
                post = cursor.fetchone()
                
                if not post:
                    return None
                
                # Формируем результат
                result = {
                    'id': post[0],
                    'source_url': post[1],
                    'text': post[2],
                    'likes': post[3],
                    'comments_count': post[4],
                    'created_at': post[5],
                    'theme': post[6],
                    'gpt_role': role
                }
                
                # Помечаем пост как проанализированный
                cursor.execute("UPDATE posts SET analyzed = 1 WHERE id = ?", (post[0],))
                conn.commit()
                
                return result
        except Exception as e:
            logger.error(f"Ошибка при получении поста: {str(e)}")
            return None

    def __del__(self):
        """Закрытие соединения с базой данных"""
        self.conn.close() 