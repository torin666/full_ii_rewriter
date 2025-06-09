from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Создает основную клавиатуру по категориям"""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📝 Источники"), KeyboardButton(text="📢 Паблики")],
            [KeyboardButton(text="🤖 Автопостинг"), KeyboardButton(text="⚙️ Роль GPT")]
        ],
        resize_keyboard=True
    )
    return keyboard

def get_sources_keyboard() -> ReplyKeyboardMarkup:
    """Создает клавиатуру для работы с источниками"""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Добавить источники (одна тема)")],
            [KeyboardButton(text="Добавить источники (разные темы)")],
            [KeyboardButton(text="Мои источники")],
            [KeyboardButton(text="⬅️ Назад в главное меню")]
        ],
        resize_keyboard=True
    )
    return keyboard

def get_publics_keyboard() -> ReplyKeyboardMarkup:
    """Создает клавиатуру для работы с пабликами"""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Добавить паблик")],
            [KeyboardButton(text="Мои паблики")],
            [KeyboardButton(text="⬅️ Назад в главное меню")]
        ],
        resize_keyboard=True
    )
    return keyboard

def get_autopost_keyboard() -> ReplyKeyboardMarkup:
    """Создает клавиатуру для автопостинга"""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Создать пост")],
            [KeyboardButton(text="Начать автопостинг в паблике")],
            [KeyboardButton(text="Управление автопостингом в пабликах")],
            [KeyboardButton(text="⬅️ Назад в главное меню")]
        ],
        resize_keyboard=True
    )
    return keyboard

def get_gpt_keyboard() -> ReplyKeyboardMarkup:
    """Создает клавиатуру для настройки GPT"""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Установить роль GPT")],
            [KeyboardButton(text="Текущая роль GPT")],
            [KeyboardButton(text="⬅️ Назад в главное меню")]
        ],
        resize_keyboard=True
    )
    return keyboard

def get_user_groups_keyboard(groups: list) -> InlineKeyboardMarkup:
    """Создает клавиатуру для выбора паблика пользователя"""
    keyboard = []
    for group in groups:
        keyboard.append([
            InlineKeyboardButton(
                text=group['group_link'],
                callback_data=f"group_{group['group_link']}"
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_themes_keyboard(themes, selected, page, per_page=10):
    """
    Создает клавиатуру для выбора тем.
    Показывает 10 кнопок в 5 рядов по 2 кнопки (2 колонки)
    """
    start = page * per_page
    end = start + per_page
    page_themes = themes[start:end]
    keyboard = []
    
    # Создаем кнопки в 5 рядов по 2 кнопки (2 колонки)
    for i in range(0, len(page_themes), 2):
        row = []
        for j in range(2):
            if i + j < len(page_themes):
                theme = page_themes[i + j]
                checked = "✅" if theme in selected else ""
                # Делаем кнопки короче - убираем пробел после галочки
                text = f"{checked}{theme}" if checked else theme
                row.append(InlineKeyboardButton(
                    text=text, 
                    callback_data=f"theme_{theme}"
                ))
        if row:  # Добавляем ряд только если в нем есть кнопки
            keyboard.append(row)
    
    # Добавляем кнопку "Другое" в отдельном ряду
    other_checked = "✅" if "Другое" in selected else ""
    other_text = f"{other_checked}🔧 Другое" if other_checked else "🔧 Другое"
    keyboard.append([InlineKeyboardButton(
        text=other_text, 
        callback_data="theme_other"
    )])
    
    # Кнопки навигации и подтверждения
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data="theme_prev"))
    if end < len(themes):
        nav.append(InlineKeyboardButton(text="➡️", callback_data="theme_next"))
    if selected:
        nav.append(InlineKeyboardButton(text="✅ Подтвердить", callback_data="theme_confirm"))
    if nav:
        keyboard.append(nav)
        
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

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

def get_publish_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру для публикации поста"""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✏️ Редактировать пост", callback_data="edit_post")
            ],
            [
                InlineKeyboardButton(text="📤 Опубликовать в группе", callback_data="publish_to_group")
            ]
        ]
    )
    return keyboard

def get_admin_check_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру для проверки админских прав"""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔄 Перепроверить", callback_data="recheck_admin")
            ],
            [
                InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_admin_check")
            ]
        ]
    )
    return keyboard

def get_post_edit_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру для редактирования поста"""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✏️ Редактировать еще раз", callback_data="edit_post")
            ],
            [
                InlineKeyboardButton(text="📤 Опубликовать", callback_data="publish_to_group")
            ],
            [
                InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_edit")
            ]
        ]
    )
    return keyboard 

def get_inline_main_keyboard() -> InlineKeyboardMarkup:
    """Создает пустую инлайн клавиатуру (без кнопок)"""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[]
    )
    return keyboard

def get_autopost_mode_keyboard(group_link: str = "") -> InlineKeyboardMarkup:
    """Создает клавиатуру для выбора режима автопостинга"""
    # Извлекаем только username из group_link для callback_data (ограничение 64 символа)
    group_username = group_link
    if group_link.startswith('https://t.me/'):
        group_username = group_link.replace('https://t.me/', '')
    elif group_link.startswith('t.me/'):
        group_username = group_link.replace('t.me/', '')
    elif group_link.startswith('@'):
        group_username = group_link[1:]
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🤖 Автоматический режим", 
                    callback_data=f"autopost_mode_automatic_{group_username}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="👤 Контролируемый режим", 
                    callback_data=f"autopost_mode_controlled_{group_username}"
                )
            ]
        ]
    )
    return keyboard

def get_autopost_management_keyboard(settings: list) -> InlineKeyboardMarkup:
    """Создает клавиатуру для управления автопостингом"""
    keyboard = []
    
    for setting in settings:
        group_name = setting['group_link'].split('/')[-1] if '/' in setting['group_link'] else setting['group_link']
        status = "🟢" if setting['is_active'] else "🔴"
        mode = "🤖" if setting['mode'] == 'automatic' else "👤"
        
        keyboard.append([
            InlineKeyboardButton(
                text=f"{status} {mode} {group_name}", 
                callback_data=f"manage_autopost_{setting['group_link']}"
            )
        ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_autopost_group_actions_keyboard(group_link: str, current_mode: str, is_active: bool) -> InlineKeyboardMarkup:
    """Создает клавиатуру действий для конкретной группы автопостинга"""
    
    # Определяем противоположный режим
    opposite_mode = "controlled" if current_mode == "automatic" else "automatic"
    mode_text = "👤 Контролируемый" if opposite_mode == "controlled" else "🤖 Автоматический"
    
    # Определяем действие для включения/выключения
    toggle_text = "⏸ Приостановить" if is_active else "▶️ Возобновить"
    toggle_action = "pause" if is_active else "resume"
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"🔄 Изменить на {mode_text}",
                    callback_data=f"change_mode_{group_link}_{opposite_mode}"
                )
            ],
            [
                InlineKeyboardButton(
                    text=toggle_text,
                    callback_data=f"toggle_autopost_{group_link}_{toggle_action}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="🗑 Удалить автопостинг",
                    callback_data=f"delete_autopost_{group_link}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад",
                    callback_data="back_to_autopost_management"
                )
            ]
        ]
    )
    return keyboard

def get_autopost_approval_keyboard(group_link: str) -> InlineKeyboardMarkup:
    """Создает клавиатуру для одобрения/редактирования поста"""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Подтвердить публикацию", callback_data=f"approve_autopost_{group_link}")
            ],
            [
                InlineKeyboardButton(text="✏️ Редактировать пост", callback_data=f"edit_autopost_{group_link}")
            ],
            [
                InlineKeyboardButton(text="❌ Отменить публикацию", callback_data=f"cancel_autopost_{group_link}")
            ]
        ]
    )
    return keyboard

def get_source_selection_mode_keyboard():
    """Клавиатура для выбора режима источников"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🤖 Автоматический подбор", callback_data="source_mode_auto"),
            InlineKeyboardButton(text="✋ Выбрать источники", callback_data="source_mode_manual")
        ],
        [
            InlineKeyboardButton(text="ℹ️ Что это?", callback_data="source_mode_info")
        ],
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_autopost_setup")
        ]
    ])
    return keyboard

def get_user_sources_keyboard(sources: list, selected: list = None, page: int = 0):
    """
    Клавиатура для выбора источников пользователя
    
    Args:
        sources: список источников пользователя
        selected: список ID выбранных источников
        page: текущая страница
    """
    if selected is None:
        selected = []
    
    per_page = 10  # 5 рядов по 2 колонки
    start_idx = page * per_page
    end_idx = start_idx + per_page
    page_sources = sources[start_idx:end_idx]
    
    keyboard = []
    
    # Добавляем источники по 2 в ряд
    for i in range(0, len(page_sources), 2):
        row = []
        
        # Первый источник в ряду
        source = page_sources[i]
        is_selected = source['id'] in selected
        emoji = "✅" if is_selected else "☐"
        # Обрезаем название для кнопки
        name = source['link'][:25] + "..." if len(source['link']) > 25 else source['link']
        row.append(InlineKeyboardButton(
            text=f"{emoji} {name}",
            callback_data=f"select_source_{source['id']}"
        ))
        
        # Второй источник в ряду (если есть)
        if i + 1 < len(page_sources):
            source = page_sources[i + 1]
            is_selected = source['id'] in selected
            emoji = "✅" if is_selected else "☐"
            name = source['link'][:25] + "..." if len(source['link']) > 25 else source['link']
            row.append(InlineKeyboardButton(
                text=f"{emoji} {name}",
                callback_data=f"select_source_{source['id']}"
            ))
        
        keyboard.append(row)
    
    # Навигация и управление
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"sources_page_{page-1}"))
    if end_idx < len(sources):
        nav_row.append(InlineKeyboardButton(text="➡️ Вперед", callback_data=f"sources_page_{page+1}"))
    
    if nav_row:
        keyboard.append(nav_row)
    
    # Кнопки управления
    control_row = []
    if selected:
        control_row.append(InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_source_selection"))
        control_row.append(InlineKeyboardButton(text="🗑 Очистить", callback_data="clear_source_selection"))
    
    if control_row:
        keyboard.append(control_row)
    
    # Кнопка назад
    keyboard.append([
        InlineKeyboardButton(text="⬅️ Назад к режимам", callback_data="back_to_source_mode")
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)