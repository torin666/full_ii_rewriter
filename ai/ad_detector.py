"""
Модуль для детекции рекламного контента
"""

import logging
from openai import AsyncOpenAI
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

async def is_advertisement(text: str) -> dict:
    """
    Проверяет, является ли текст рекламой
    
    Args:
        text: текст для проверки
        
    Returns:
        dict: {
            'is_ad': bool,  # True если реклама
            'confidence': float,  # уверенность от 0 до 1
            'reason': str  # причина определения как реклама
        }
    """
    try:
        if not text or len(text.strip()) < 20:
            return {'is_ad': False, 'confidence': 0.0, 'reason': 'Слишком короткий текст'}
        
        client = AsyncOpenAI(
            api_key=OPENAI_API_KEY,
            base_url="https://api.openai.com/v1",
        )
        
        messages = [
            {
                "role": "system",
                "content": """Ты эксперт по определению рекламного контента. 
                
ЗАДАЧА: Определи, является ли данный текст рекламой или коммерческим предложением.

РЕКЛАМА - это:
- Предложения товаров/услуг с ценами
- Призывы к покупке ("купить", "заказать", "скидка", "акция")
- Контактная информация для продаж (телефоны, сайты магазинов)
- Промо-коды, скидки, распродажи
- Реклама мероприятий с платным входом
- Объявления о работе с зарплатой
- Продажа недвижимости, авто и т.д.

НЕ РЕКЛАМА:
- Новости
- Информационные сообщения
- Анонсы бесплатных мероприятий
- Социальные посты
- Развлекательный контент

Ответь ТОЛЬКО в формате JSON:
{
    "is_ad": true/false,
    "confidence": 0.0-1.0,
    "reason": "краткое объяснение"
}"""
            },
            {
                "role": "user",
                "content": f"Проанализируй текст:\n\n{text}"
            }
        ]
        
        response = await client.chat.completions.create(
            model="gpt-4-1106-preview",
            messages=messages,
            temperature=0.1,  # Низкая температура для точности
            max_tokens=200
        )
        
        result_text = response.choices[0].message.content.strip()
        logger.info(f"🤖 GPT ответ на детекцию рекламы: {result_text}")
        
        # Парсим JSON ответ
        import json
        try:
            result = json.loads(result_text)
            
            # Валидация результата
            if not isinstance(result.get('is_ad'), bool):
                raise ValueError("is_ad должно быть boolean")
            if not isinstance(result.get('confidence'), (int, float)):
                raise ValueError("confidence должно быть числом")
            if not isinstance(result.get('reason'), str):
                raise ValueError("reason должно быть строкой")
                
            # Нормализуем confidence
            confidence = float(result['confidence'])
            if confidence < 0:
                confidence = 0.0
            elif confidence > 1:
                confidence = 1.0
                
            return {
                'is_ad': result['is_ad'],
                'confidence': confidence,
                'reason': result['reason']
            }
            
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.error(f"❌ Ошибка парсинга ответа GPT: {e}")
            logger.error(f"Ответ GPT: {result_text}")
            
            # Fallback: простая проверка по ключевым словам
            return await simple_ad_detection(text)
            
    except Exception as e:
        logger.error(f"❌ Ошибка при детекции рекламы через GPT: {e}")
        # Fallback на простую детекцию
        return await simple_ad_detection(text)

async def simple_ad_detection(text: str) -> dict:
    """
    Простая детекция рекламы по ключевым словам (fallback)
    """
    text_lower = text.lower()
    
    # Рекламные ключевые слова
    ad_keywords = [
        'купить', 'заказать', 'скидка', 'акция', 'распродажа', 'промокод',
        'цена', 'рублей', 'стоимость', 'бесплатная доставка', 'звоните',
        'заказывайте', 'успейте', 'только сегодня', 'ограниченное предложение',
        'магазин', 'интернет-магазин', 'каталог', 'товар', 'услуга',
        'работа', 'вакансия', 'зарплата', 'требуется', 'ищем сотрудника',
        'продам', 'продается', 'сдам', 'сдается', 'аренда', 'недвижимость'
    ]
    
    # Подсчитываем количество рекламных слов
    ad_count = sum(1 for keyword in ad_keywords if keyword in text_lower)
    
    # Определяем вероятность рекламы
    if ad_count >= 3:
        return {
            'is_ad': True,
            'confidence': min(0.8, 0.3 + ad_count * 0.1),
            'reason': f'Найдено {ad_count} рекламных ключевых слов'
        }
    elif ad_count >= 1:
        return {
            'is_ad': True,
            'confidence': 0.4 + ad_count * 0.1,
            'reason': f'Найдено {ad_count} рекламных ключевых слов'
        }
    else:
        return {
            'is_ad': False,
            'confidence': 0.9,
            'reason': 'Рекламные ключевые слова не найдены'
        }

async def filter_advertisements(posts: list, confidence_threshold: float = 0.6) -> list:
    """
    Фильтрует список постов, исключая рекламу
    
    Args:
        posts: список постов для проверки
        confidence_threshold: порог уверенности для исключения рекламы
        
    Returns:
        list: посты без рекламы
    """
    if not posts:
        return []
    
    logger.info(f"🚫 Фильтруем рекламу из {len(posts)} постов (порог уверенности: {confidence_threshold})")
    
    non_ad_posts = []
    
    for post in posts:
        text = post.get('text', '')
        if not text:
            continue
            
        ad_result = await is_advertisement(text)
        
        if ad_result['is_ad'] and ad_result['confidence'] >= confidence_threshold:
            logger.info(f"   🚫 РЕКЛАМА (уверенность: {ad_result['confidence']:.2f}): {text[:50]}...")
            logger.info(f"      Причина: {ad_result['reason']}")
        else:
            non_ad_posts.append(post)
            if ad_result['is_ad']:
                logger.info(f"   ⚠️ Возможная реклама (уверенность: {ad_result['confidence']:.2f}), но пропускаем: {text[:50]}...")
            else:
                logger.info(f"   ✅ НЕ реклама: {text[:50]}...")
    
    logger.info(f"📊 Результат фильтрации рекламы: {len(non_ad_posts)} постов из {len(posts)} (исключено: {len(posts) - len(non_ad_posts)})")
    return non_ad_posts 