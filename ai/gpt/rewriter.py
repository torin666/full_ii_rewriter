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

async def rewriter(text, post_link, user_id, photo_url=None):
    """
    Переписывает текст и обрабатывает медиафайлы для поста.
    
    Args:
        text (str): Исходный текст для переписывания
        post_link (str): Ссылка на оригинальный пост
        user_id (int): ID пользователя для получения его роли
        photo_url (str, optional): URL фото или видео из оригинального поста
        
    Returns:
        dict: Словарь с результатами:
            - text: переписанный текст
            - image_url: URL медиафайла (если есть)
            - is_original: True если используется оригинальный медиафайл
            - is_video: True если это видео
    """
    try:
        # Используем АСИНХРОННЫЙ OpenAI клиент!
        from openai import AsyncOpenAI
        client = AsyncOpenAI(
            api_key=OPENAI_API_KEY,
            base_url="https://api.openai.com/v1",
        )
        
        # Получаем роль пользователя
        db = DatabaseManager()
        role = db.get_gpt_role(user_id)

        # Генерируем новый текст с учетом роли пользователя
        messages = [
            {
                "role": "system", 
                "content": f"{role} Перепиши новость, сохраняя смысл, делая её интересной и читаемой. "
                          f"ВАЖНО: текст должен быть не длиннее 1000 символов, чтобы поместиться в Telegram. "
                          f"Не добавляй ссылок и не упоминай источник. Создай законченный, читаемый текст."
            },
            {
                "role": "user", 
                "content": f"Новость: {text}"
            }
        ]
        
        # АСИНХРОННЫЙ запрос к GPT
        response = await client.chat.completions.create(
            model="gpt-4-1106-preview", 
            messages=messages
        )
        new_text = response.choices[0].message.content.strip()
        
        result = {"text": new_text}
        
        # Проверяем наличие медиафайла
        if photo_url:
            result["image_url"] = photo_url
            result["is_original"] = True
            # Проверяем, является ли файл видео
            result["is_video"] = photo_url.lower().endswith(('.mp4', '.mov', '.avi', '.mkv'))
        # ЗАКОММЕНТИРОВАНО: Генерация изображений отключена
        # Теперь используем только оригинальные фото/видео, если они есть
        # Если нет медиафайла, публикуем без изображения
        
        # # Если нет медиафайла, генерируем новое изображение
        # else:
        #     # Создаем промпт для DALL-E
        #     image_prompt_messages = [
        #         {
        #             "role": "system", 
        #             "content": "Создай короткий, но детальный промпт на английском языке для DALL-E. "
        #                       "Промпт должен описывать реалистичное изображение, подходящее к новости. "
        #                       "ВАЖНО: НЕ ВКЛЮЧАЙ людей, лица, персонажей или человеческие фигуры в изображение. "
        #                       "Используй только предметы, пейзажи, здания, технику, природу, абстракции. "
        #                       "КРИТИЧЕСКИ ВАЖНО: НЕ ВКЛЮЧАЙ любой текст, надписи, буквы, слова, вывески, "
        #                       "билборды, таблички, знаки с текстом или любые читаемые символы на изображении. "
        #                       "Изображение должно быть полностью БЕЗ ТЕКСТА. "
        #                       "Не используй запрещенные элементы. Верни только сам промпт."
        #         },
        #         {
        #             "role": "user", 
        #             "content": f"Создай промпт для новости БЕЗ людей и БЕЗ ТЕКСТА: {new_text}"
        #         }
        #     ]
            
        #     # АСИНХРОННЫЙ запрос для промпта изображения
        #     prompt_response = await client.chat.completions.create(
        #         model="gpt-4-1106-preview",
        #         messages=image_prompt_messages
        #     )
        #     image_prompt = prompt_response.choices[0].message.content.strip()
            
        #     # Генерируем изображение
        #     try:
        #         # Добавляем дополнительную инструкцию к промпту
        #         enhanced_prompt = f"{image_prompt}. NO TEXT, NO WRITING, NO LETTERS, NO SIGNS, NO BILLBOARDS, completely text-free image."
                
        #         # АСИНХРОННЫЙ запрос к DALL-E
        #         image_response = await client.images.generate(
        #             model="dall-e-3",
        #             prompt=enhanced_prompt,
        #             size="1024x1024",
        #             quality="standard",
        #             n=1
        #         )
        #         generated_image_url = image_response.data[0].url
        #         if generated_image_url:
        #             result["image_url"] = generated_image_url
        #             result["is_original"] = False
        #             result["is_video"] = False
        #     except Exception as e:
        #         logger.error(f"Ошибка при генерации изображения через DALL-E: {str(e)}")
        
        return result
        
    except Exception as e:
        logger.error(f"Ошибка при переписывании текста: {str(e)}")
        return {"text": new_text if 'new_text' in locals() else f"Ошибка при переписывании текста: {str(e)}"}

