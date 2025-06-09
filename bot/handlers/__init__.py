from aiogram import Dispatcher
from .source_handlers import router as source_router

def register_handlers(dp: Dispatcher):
    """
    Регистрирует все обработчики бота
    
    Args:
        dp (Dispatcher): диспетчер бота
    """
    # Регистрируем роутер с обработчиками источников
    dp.include_router(source_router)
    
    # Здесь можно добавить регистрацию других роутеров
    # например:
    # dp.include_router(admin_router)
    # dp.include_router(user_router)
    # и т.д. 