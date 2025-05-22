from urllib.parse import urlparse
from typing import List

def validate_url(url: str, allowed_domains: List[str]) -> bool:
    """
    Проверяет, является ли URL валидным и принадлежит ли он разрешенным доменам.
    
    Args:
        url: URL для проверки
        allowed_domains: Список разрешенных доменов
        
    Returns:
        bool: True если URL валиден и принадлежит разрешенным доменам
    """
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return False
        
        # Проверяем, что домен входит в список разрешенных
        return any(domain in parsed.netloc for domain in allowed_domains)
    except:
        return False

def validate_theme(theme: str, allowed_themes: List[str]) -> bool:
    """
    Проверяет, является ли тема валидной.
    
    Args:
        theme: Тема для проверки
        allowed_themes: Список разрешенных тем
        
    Returns:
        bool: True если тема валидна
    """
    return bool(theme.strip()) and len(theme) <= 100 