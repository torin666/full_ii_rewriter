from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Создает основную клавиатуру"""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Добавить источник")],
            [KeyboardButton(text="Мои источники")],
            [KeyboardButton(text="Установить роль GPT"), KeyboardButton(text="Текущая роль GPT")]
        ],
        resize_keyboard=True
    )
    return keyboard

def get_themes_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру с тематиками"""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Технологии", callback_data="theme_tech"),
                InlineKeyboardButton(text="Бизнес", callback_data="theme_business")
            ],
            [
                InlineKeyboardButton(text="Образование", callback_data="theme_education"),
                InlineKeyboardButton(text="Развлечения", callback_data="theme_entertainment")
            ],
            [
                InlineKeyboardButton(text="Другое", callback_data="theme_custom")
            ]
        ]
    )
    return keyboard

def get_confirmation_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру подтверждения"""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data="confirm_yes"),
                InlineKeyboardButton(text="Нет", callback_data="confirm_no")
            ]
        ]
    )
    return keyboard

def get_source_actions_keyboard(source_id: int) -> InlineKeyboardMarkup:
    """Создает клавиатуру действий с источником"""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Удалить", callback_data=f"delete_source_{source_id}"),
                InlineKeyboardButton(text="Изменить тему", callback_data=f"edit_theme_{source_id}")
            ]
        ]
    )
    return keyboard 