import json
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from typing import List, Dict

# --- Главное меню (Reply) ---
def get_main_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🤖 Автопостинг")],
            [KeyboardButton(text="📝 Источники"), KeyboardButton(text="📢 Паблики")]
        ],
        resize_keyboard=True
    )

# --- Меню Источников (Reply) ---
def get_sources_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Добавить источник"), KeyboardButton(text="Мои источники")],
            [KeyboardButton(text="⬅️ Назад в главное меню")]
        ],
        resize_keyboard=True
    )

# --- Меню Пабликов (Reply) ---
def get_publics_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Добавить паблик"), KeyboardButton(text="Мои паблики")],
            [KeyboardButton(text="⬅️ Назад в главное меню")]
        ],
        resize_keyboard=True
    )

# --- Меню Автопостинга (Reply) ---
def get_autopost_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Начать автопостинг в паблике")],
            [KeyboardButton(text="Управление автопостингом в пабликах")],
            [KeyboardButton(text="⬅️ Назад в главное меню")]
        ],
        resize_keyboard=True
    )

# --- Меню Роли GPT (Reply) ---
def get_gpt_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Изменить роль GPT"), KeyboardButton(text="Текущая роль GPT")],
            [KeyboardButton(text="⬅️ Назад в главное меню")]
        ],
        resize_keyboard=True
    )

# --- Общие инлайн-клавиатуры ---
def get_cancel_keyboard(callback_data: str = "cancel"):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data=callback_data)]])

def get_back_to_main_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ В главное меню", callback_data="back_to_main_menu")]])

def get_skip_keyboard(callback_data: str):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➡️ Пропустить", callback_data=callback_data)]])

# --- Клавиатуры для процесса НАСТРОЙКИ АВТОПОСТИНГА (SETUP) ---

def get_user_groups_keyboard(groups: list, prefix: str):
    buttons = []
    for group in groups:
        link = group.get('group_link', 'Неизвестно')
        # Убираем https:// и www для краткости
        display_name = link.replace("https://", "").replace("www.", "")
        buttons.append([InlineKeyboardButton(text=display_name, callback_data=f"{prefix}{link}")])
    buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_autopost_mode_keyboard(prefix: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 Автоматический", callback_data=f"{prefix}autopost_mode_automatic")],
        [InlineKeyboardButton(text="👤 Контролируемый", callback_data=f"{prefix}autopost_mode_controlled")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])

def get_source_selection_mode_keyboard(prefix: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 Автоматический подбор", callback_data=f"{prefix}source_mode_auto")],
        [InlineKeyboardButton(text="✋ Выбрать источники вручную", callback_data=f"{prefix}source_mode_manual")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{prefix}back_to_mode")]
    ])

def get_user_sources_keyboard(sources: list, selected_ids: list, page: int = 0, prefix: str = "", page_size: int = 5):
    buttons = []
    start = page * page_size
    end = start + page_size
    
    for src in sources[start:end]:
        selected_icon = "✅" if src['id'] in selected_ids else "☑️"
        # Убираем https:// и www для краткости
        display_name = src['link'].replace("https://", "").replace("www.", "")
        buttons.append([InlineKeyboardButton(
            text=f"{selected_icon} {display_name}", 
            callback_data=f"{prefix}select_source_{src['id']}"
        )])

    # Пагинация
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Пред.", callback_data=f"{prefix}page_{page-1}"))
    if end < len(sources):
        nav_buttons.append(InlineKeyboardButton(text="След. ➡️", callback_data=f"{prefix}page_{page+1}"))
    
    if nav_buttons:
        buttons.append(nav_buttons)

    # Управляющие кнопки
    control_buttons = []
    if prefix == "setup_":
        control_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data="setup_back_to_mode"))
        control_buttons.append(InlineKeyboardButton(text="✅ Готово", callback_data="setup_done_selecting_sources"))
    elif prefix == "manage_":
        control_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_group_settings"))
        control_buttons.append(InlineKeyboardButton(text="💾 Сохранить", callback_data="manage_done_selecting_sources"))

    buttons.append(control_buttons)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_autopost_role_selection_keyboard(prefix: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Использовать основную роль", callback_data=f"{prefix}use_default_role")],
        [InlineKeyboardButton(text="✏️ Задать новую для этого паблика", callback_data=f"{prefix}set_new_role")]
    ])

# --- Клавиатуры для процесса УПРАВЛЕНИЯ АВТОПОСТИНГОМ (MANAGE) ---

def get_autopost_management_keyboard(settings: list):
    buttons = []
    for setting in settings:
        link = setting.get('group_link', 'Неизвестно')
        display_name = link.replace("https://", "").replace("www.", "")
        status_icon = "🟢" if setting.get('is_active') else "🔴"
        buttons.append([InlineKeyboardButton(text=f"{status_icon} {display_name}", callback_data=f"manage_autopost_{link}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_autopost_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_autopost_settings_keyboard(group_link: str, is_active: bool, mode: str):
    toggle_text = "🔴 Остановить" if is_active else "▶️ Запустить"
    toggle_action = "pause" if is_active else "resume"
    
    change_mode_text = "Сменить на 🤖 Автоматический" if mode == 'controlled' else "Сменить на 👤 Контролируемый"
    change_mode_action = "automatic" if mode == 'controlled' else "controlled"

    buttons = [
        [InlineKeyboardButton(text=toggle_text, callback_data=f"toggle_autopost_{toggle_action}_{group_link}")],
        [InlineKeyboardButton(text=change_mode_text, callback_data=f"change_mode_{change_mode_action}_{group_link}")],
        [InlineKeyboardButton(text="🗂 Выбрать источники", callback_data=f"manage_sources_{group_link}")],
        [InlineKeyboardButton(text="👤 Изменить роль GPT", callback_data=f"manage_role_{group_link}")],
        [InlineKeyboardButton(text="🚫 Запретные темы", callback_data=f"manage_topics_{group_link}")],
        [InlineKeyboardButton(text="🗑 Удалить настройку", callback_data=f"delete_autopost_{group_link}")],
        [InlineKeyboardButton(text="⬅️ Назад к списку", callback_data="back_to_autopost_management")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# --- Старые или неиспользуемые клавиатуры, которые можно удалить или пересмотреть ---

def get_themes_keyboard(selected_themes: list = None):
    """Создает клавиатуру для выбора тем."""
    if selected_themes is None:
        selected_themes = []
    
    from config.settings import THEMES
    buttons = []
    row = []
    for theme in THEMES:
        text = f"✅ {theme}" if theme in selected_themes else theme
        row.append(InlineKeyboardButton(text=text, callback_data=f"theme_{theme}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([InlineKeyboardButton(text="Другая тема", callback_data="custom_theme")])
    buttons.append([InlineKeyboardButton(text="Готово", callback_data="done_themes")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_confirmation_keyboard(post_text, photo_url):
    # Не относится к автопостингу, оставляем
    buttons = [
        [InlineKeyboardButton(text="Опубликовать", callback_data="publish_post")],
        [InlineKeyboardButton(text="Отмена", callback_data="cancel_post")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_source_actions_keyboard(source_id):
    # Не относится к автопостингу, оставляем
    buttons = [
        [InlineKeyboardButton(text="Удалить источник", callback_data=f"delete_source_{source_id}")],
        [InlineKeyboardButton(text="Изменить темы", callback_data=f"edit_themes_{source_id}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_publish_keyboard(post_id, group_link, has_media):
    buttons = [
        [InlineKeyboardButton(text="✅ Опубликовать", callback_data=f"publish_post_{post_id}_{group_link}")],
        [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_post_text_{post_id}_{group_link}")],
    ]
    if has_media:
        buttons.append([InlineKeyboardButton(text="🖼️ Заменить медиа", callback_data=f"edit_post_media_{post_id}_{group_link}")])
    buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data=f"cancel_post_{post_id}_{group_link}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_check_keyboard(post_link):
    # Не относится к автопостингу, оставляем
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Проверить", url=post_link)]])

def get_post_edit_keyboard():
    # Не относится к автопостингу, оставляем
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Одобрить", callback_data="approve_post")],
        [InlineKeyboardButton(text="Редактировать", callback_data="edit_post")],
        [InlineKeyboardButton(text="Отменить", callback_data="cancel_post_approval")]
    ])

def get_inline_main_keyboard():
    # Используется для возврата, оставляем
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]])

def get_post_approval_keyboard(queue_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_post_{queue_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"cancel_post_{queue_id}")
        ],
        [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_queued_post_{queue_id}")]
    ])

def get_autopost_approval_keyboard(group_link: str):
    """Клавиатура для управления постом в контролируемом режиме"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Опубликовать", callback_data=f"approve_autopost_{group_link}")],
        [InlineKeyboardButton(text="✏️ Изменить текст", callback_data=f"rewrite_autopost_{group_link}")],
        [InlineKeyboardButton(text="🔄 Заменить пост", callback_data=f"replace_autopost_{group_link}")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel_autopost_{group_link}")]
    ])

def get_recheck_admin_keyboard(channel_link: str):
    """Клавиатура с кнопкой перепроверки прав админа."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Перепроверить права", callback_data=f"recheck_admin_{channel_link}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_add_group")]
    ])