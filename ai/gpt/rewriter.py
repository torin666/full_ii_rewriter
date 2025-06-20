import asyncio
import os
import logging
from config.settings import OPENAI_API_KEY
from database.DatabaseManager import DatabaseManager

logger = logging.getLogger(__name__)

class TextRewriter:
    def __init__(self):
        # Оставляем старый клиент для обратной совместимости
        import openai
        self.client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

    def rewrite_text(self, text, post_link):
        """
        Переписывает текст с помощью GPT-4o
        """
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "Ты - помощник для переписывания текстов. Перепиши текст, сохраняя основной смысл, но делая его более интересным и читаемым. В конце добавь ссылку на оригинальный пост в формате 'Источник: [ссылка]'."},
                    {"role": "user", "content": f"Перепиши этот текст: {text}\n\nДобавь в конец ссылку на оригинальный пост: {post_link}"}
                ]
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Ошибка при переписывании текста: {str(e)}")
            return None

async def generate_image_with_dalle(client, prompt):
    """
    Генерирует изображение с помощью DALL-E
    """
    try:
        response = client.images.generate(
            model="dall-e-3",  # Используем DALL-E 3
            prompt=prompt,
            size="1024x1024",
            quality="standard",
            n=1,
        )
        return response.data[0].url
    except Exception as e:
        logger.error(f"Ошибка при генерации изображения DALL-E 3: {str(e)}")
        try:
            # Пробуем DALL-E 2 как запасной вариант
            response = client.images.generate(
                model="dall-e-2",
                prompt=prompt,
                size="1024x1024",
                n=1,
            )
            return response.data[0].url
        except Exception as e2:
            logger.error(f"Ошибка при генерации изображения DALL-E 2: {str(e2)}")
            return None

async def rewriter(text, post_link, user_id, photo_url=None, group_link=None):
    """
    Переписывает текст и обрабатывает медиафайлы для поста.
    
    Args:
        text (str): Исходный текст для переписывания
        post_link (str): Ссылка на оригинальный пост
        user_id (int): ID пользователя для получения его роли
        photo_url (str, optional): URL фото или видео из оригинального поста
        group_link (str, optional): Ссылка на группу для получения роли
        
    Returns:
        dict: Словарь с результатами:
            - text: переписанный текст
            - image_url: URL медиафайла (если есть)
            - is_original: True если используется оригинальный медиафайл
            - is_video: True если это видео
            - blocked: True если контент заблокирован
    """
    try:
        # Получаем роль пользователя для конкретной группы
        db = DatabaseManager()
        role_text = db.get_gpt_roles(user_id, group_link)
        
        # Проверяем заблокированные темы
        if group_link:
            blocked_topics = db.get_blocked_topics(user_id, group_link)
            if blocked_topics and await db.check_content_blocked(text, blocked_topics):
                logger.info(f"🚫 Контент заблокирован по темам: {blocked_topics}")
                return {
                    "text": None,
                    "blocked": True,
                    "blocked_reason": f"Контент содержит заблокированные темы: {blocked_topics}"
                }
        
        # Используем АСИНХРОННЫЙ OpenAI клиент!
        from openai import AsyncOpenAI
        client = AsyncOpenAI(
            api_key=OPENAI_API_KEY,
            base_url="https://api.openai.com/v1",
        )
        
        # Генерируем новый текст с учетом роли пользователя
        messages = [
            {
                "role": "system",  # Первое сообщение всегда system
                "content": role_text + "\n\n" + f" Перепиши новость, сохраняя смысл, делая её интересной и читаемой. "
                          f"ВАЖНО: текст должен быть не длиннее 1000 символов, чтобы поместиться в Telegram. "
                          f"Не добавляй ссылок и не упоминай источник. Создай законченный, читаемый текст. "
                          f"ОБЯЗАТЕЛЬНО добавь жирный заголовок в начале текста, используя разметку *Заголовок* "
                          f"и затем основной текст с новой строки."
            },
            {
                "role": "user",  # Второе сообщение всегда user
                "content": f"Новость: {text}"
            }
        ]
        
        # АСИНХРОННЫЙ запрос к GPT
        response = await client.chat.completions.create(
            model="gpt-4o", 
            messages=messages
        )
        new_text = response.choices[0].message.content.strip()
        
        result = {"text": new_text, "blocked": False}
        
        # Проверяем наличие медиафайла
        if photo_url:
            result["image_url"] = photo_url
            result["is_original"] = True
            # Проверяем, является ли файл видео по его пути
            result["is_video"] = '/videos/' in photo_url
        
        return result
        
    except Exception as e:
        logger.error(f"Ошибка при переписывании текста: {str(e)}")
        return {"text": new_text if 'new_text' in locals() else f"Ошибка при переписывании текста: {str(e)}", "blocked": False}

