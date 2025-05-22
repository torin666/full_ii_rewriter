from datetime import datetime
from environs import Env
import psycopg2
import spacy

env = Env()
env.read_env()
nlp = spacy.load("ru_core_news_md")



class Database_Manager:
    def __init__(self):
        self.conn_params = {
            "host": "80.74.24.141",
            "port": 5432,
            "database": "mydb",
            "user": env('USER_DB'),
            "password": env('USER_PWD')
        }
        self.schema = "ii_rewriter"

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
            (group_link, post_link, text, date, likes, views, comments_count, comments_likes, photo_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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

                    if exists:
                        # Обновляем метрики существующего поста
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
                    else:
                        # Добавляем новый пост
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