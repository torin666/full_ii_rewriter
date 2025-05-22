import openai
import os
import logging

logger = logging.getLogger(__name__)

class TextRewriter:
    def __init__(self):
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

async def rewriter(text, post_link, photo_url=None):
    try:
        client = openai.OpenAI(
            api_key=os.getenv('OPENAI_API_KEY'),
            base_url="https://api.openai.com/v1",
        )

        if photo_url:
            prompt = (
                "Ты — журналист и редактор. "
                "Перепиши новость, сохраняя смысл, делая её интересной и читаемой. "
                f"Вот ссылка на фото, относящееся к новости: {photo_url}. "
                "Опиши, как могла бы выглядеть похожая фотография, но не ищи новую картинку и не добавляй других ссылок. "
                "Ответ верни строго в формате:\n"
                "Текст: [переписанный текст]\n"
                "Описание фото: [описание похожей фотографии]"
            )
        else:
            prompt = (
                "Ты — журналист и редактор. "
                "Перепиши новость, сохраняя смысл, делая её интересной и читаемой. "
                "В конце не добавляй никаких ссылок и не упоминай источник. "
                "Ответ верни строго в формате:\n"
                "Текст: [переписанный текст]"
            )

        messages = [
            {"role": "system", "content": prompt},
            {"role": "user", "content": f"Новость: {text}"}
        ]
        response = client.chat.completions.create(model="gpt-4o", messages=messages)
        full_response = response.choices[0].message.content

        if photo_url:
            parts = full_response.split("Описание фото:")
            text_part = parts[0].replace("Текст:", "").strip()
        else:
            text_part = full_response.replace("Текст:", "").strip()

        return {"text": text_part}
    except Exception as e:
        return {"text": f"Ошибка: {str(e)}"}

