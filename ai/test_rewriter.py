import asyncio
import pytest
import sys
import os
import logging

# Добавляем корневую директорию проекта в PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ai.gpt.rewriter import rewriter
from database.DatabaseManager import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Тестовые данные
TEST_USER_ID = 123456789
TEST_GROUP_LINK = "test_group"
TEST_POST_LINK = "https://test.com/post/1"

# Тексты для тестирования
TEST_TEXTS = {
    "normal": "Сегодня в парке прошел фестиваль цветов. Множество людей пришли полюбоваться красивыми композициями.",
    "blocked": "РЕКЛАМА! Купите наши товары со скидкой 90%! Только сегодня!",
    "mixed": "В парке прошел фестиваль. РЕКЛАМА: Приходите в наш магазин!",
}

@pytest.mark.asyncio
async def test_blocked_topics():
    """Тест обработки заблокированных тем"""
    db = DatabaseManager()
    user_id = 123456  # Тестовый ID пользователя
    group_link = "https://t.me/test_group"
    
    # Устанавливаем заблокированные темы
    blocked_topics = "реклама, продажи, криптовалюта"
    db.set_blocked_topics(user_id, group_link, blocked_topics)
    
    # Тест 1: Пост с рекламой
    ad_post = """
    🔥 Только сегодня! Успейте купить по выгодной цене!
    Наши товары самые лучшие на рынке.
    Закажите прямо сейчас со скидкой 50%!
    Для заказа пишите в личные сообщения.
    Не упустите свой шанс!
    """
    
    logger.info("Тест 1: Отправляем пост с рекламой")
    result1 = await rewriter(ad_post, "", user_id, None, group_link)
    logger.info(f"Результат: {result1}")
    assert result1.get('blocked', False), "Пост с рекламой должен быть заблокирован"
    
    # Тест 2: Обычный пост без рекламы
    normal_post = """
    Сегодня прекрасная погода! 
    Солнце светит ярко, птицы поют.
    Отличный день для прогулки в парке.
    """
    
    logger.info("\nТест 2: Отправляем обычный пост")
    result2 = await rewriter(normal_post, "", user_id, None, group_link)
    logger.info(f"Результат: {result2}")
    assert not result2.get('blocked', False), "Обычный пост не должен быть заблокирован"
    
    # Тест 3: Проверка на дубликаты
    # Сначала добавляем пост в базу как опубликованный
    db.add_published_post(group_link, "", normal_post)
    
    # Пытаемся отправить похожий пост
    similar_post = """
    Погода сегодня замечательная! 
    Солнышко светит ярко, птички поют.
    Самое время для прогулки в парке.
    """
    
    logger.info("\nТест 3: Проверка на дубликаты")
    published_posts = db.get_published_posts_today(group_link)
    filtered_posts = db.filter_posts_by_similarity([{'text': similar_post}], published_posts)
    logger.info(f"Найдено {len(filtered_posts)} уникальных постов")
    assert len(filtered_posts) == 0, "Похожий пост должен быть отфильтрован как дубликат"
    
    logger.info("\n✅ Все тесты пройдены успешно!")

@pytest.mark.asyncio
async def test_gpt_role():
    """Тестирование использования правильной роли GPT"""
    db = DatabaseManager()
    
    # Устанавливаем тестовую роль
    test_role = "Ты — журналист, пишущий о культурных событиях."
    db.set_autopost_role(TEST_USER_ID, TEST_GROUP_LINK, test_role)
    
    # Проверяем, что роль правильно применяется
    result = await rewriter(
        TEST_TEXTS["normal"],
        TEST_POST_LINK,
        TEST_USER_ID,
        group_link=TEST_GROUP_LINK
    )
    assert result["text"] is not None, "Текст должен быть переписан"
    assert "**" in result["text"], "Текст должен содержать заголовок в формате markdown"

@pytest.mark.asyncio
async def test_text_length():
    """Тестирование ограничения длины текста"""
    result = await rewriter(
        "А" * 2000,  # Создаем длинный текст
        TEST_POST_LINK,
        TEST_USER_ID,
        group_link=TEST_GROUP_LINK
    )
    assert len(result["text"]) <= 1000, "Текст превышает ограничение в 1000 символов"

if __name__ == "__main__":
    # Запускаем тесты
    asyncio.run(pytest.main([__file__, "-v"])) 