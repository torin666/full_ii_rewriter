import openai
import logging
import os
from config.settings import OPENAI_API_KEY

logger = logging.getLogger(__name__)

class ImageGenerator:
    def __init__(self):
        self.client = openai.OpenAI(api_key=OPENAI_API_KEY)
        
    async def generate_image(self, prompt, model="dall-e-3"):
        """
        Генерирует изображение используя DALL-E
        
        Args:
            prompt (str): Описание изображения для генерации
            model (str): Модель для генерации ("dall-e-3" или "dall-e-2")
            
        Returns:
            str: URL сгенерированного изображения или None в случае ошибки
        """
        try:
            # Добавляем инструкцию избегать текста
            enhanced_prompt = f"{prompt}. NO TEXT, NO WRITING, NO LETTERS, NO SIGNS, NO BILLBOARDS, completely text-free image."
            
            # Пробуем сначала DALL-E 3
            if model == "dall-e-3":
                try:
                    response = self.client.images.generate(
                        model="dall-e-3",
                        prompt=enhanced_prompt,
                        size="1024x1024",
                        quality="standard",
                        n=1
                    )
                    return response.data[0].url
                except Exception as e:
                    logger.warning(f"Ошибка при использовании DALL-E 3: {e}. Пробуем DALL-E 2")
                    model = "dall-e-2"
            
            # Если DALL-E 3 недоступен или произошла ошибка, используем DALL-E 2
            if model == "dall-e-2":
                response = self.client.images.generate(
                    model="dall-e-2",
                    prompt=enhanced_prompt,
                    size="1024x1024",
                    n=1
                )
                return response.data[0].url
                
        except Exception as e:
            logger.error(f"Ошибка при генерации изображения: {e}")
            return None 