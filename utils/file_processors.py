import pandas as pd
from typing import List, Tuple
from utils.validators import validate_url, validate_theme
from config.settings import ALLOWED_DOMAINS, THEMES

def process_txt_file(file_path: str) -> List[Tuple[str, str]]:
    """
    Обрабатывает TXT файл с источниками.
    Формат: каждая строка - ссылка | тематика
    
    Args:
        file_path: Путь к файлу
        
    Returns:
        List[Tuple[str, str]]: Список кортежей (url, theme)
    """
    sources = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # Разделяем строку на URL и тематику
                parts = line.split('|')
                if len(parts) != 2:
                    continue
                
                url, theme = parts[0].strip(), parts[1].strip()
                
                # Проверяем валидность
                if validate_url(url, ALLOWED_DOMAINS) and validate_theme(theme, THEMES):
                    sources.append((url, theme))
    except Exception as e:
        print(f"Ошибка при обработке TXT файла: {str(e)}")
    
    return sources

def process_excel_file(file_path: str) -> List[Tuple[str, str]]:
    """
    Обрабатывает Excel файл с источниками.
    Формат: две колонки - ссылка и тематика
    
    Args:
        file_path: Путь к файлу
        
    Returns:
        List[Tuple[str, str]]: Список кортежей (url, theme)
    """
    sources = []
    
    try:
        # Читаем Excel файл
        df = pd.read_excel(file_path)
        
        # Проверяем наличие нужных колонок
        if len(df.columns) < 2:
            return sources
        
        # Берем первые две колонки
        url_col, theme_col = df.columns[0], df.columns[1]
        
        # Обрабатываем каждую строку
        for _, row in df.iterrows():
            url = str(row[url_col]).strip()
            theme = str(row[theme_col]).strip()
            
            # Проверяем валидность
            if validate_url(url, ALLOWED_DOMAINS) and validate_theme(theme, THEMES):
                sources.append((url, theme))
    except Exception as e:
        print(f"Ошибка при обработке Excel файла: {str(e)}")
    
    return sources 