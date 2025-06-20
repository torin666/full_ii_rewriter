import json
import logging
from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from typing import Union
from aiogram.exceptions import TelegramBadRequest

from database.DatabaseManager import DatabaseManager
from bot.keyboards.source_keyboards import (
    get_main_keyboard, get_sources_keyboard, get_publics_keyboard,
    get_autopost_keyboard, get_gpt_keyboard,
    get_user_groups_keyboard, get_cancel_keyboard,
    get_autopost_management_keyboard, get_autopost_mode_keyboard,
    get_source_selection_mode_keyboard, get_user_sources_keyboard,
    get_autopost_role_selection_keyboard, get_skip_keyboard,
    get_autopost_settings_keyboard, get_themes_keyboard, get_recheck_admin_keyboard
)
from utils.validators import validate_url
from config.settings import THEMES, ALLOWED_DOMAINS

logger = logging.getLogger(__name__)
router = Router()

# --- Состояния FSM ---
class SourceStates(StatesGroup):
    # Управление источниками
    waiting_for_source_link = State()
    waiting_for_source_themes = State()
    waiting_for_custom_theme = State() # для своей темы
    
    # Управление пабликами
    waiting_for_group_url = State()
    waiting_for_group_themes = State()
    waiting_for_group_custom_theme = State()

    # Управление ролью GPT
    waiting_for_gpt_role = State()

    # Настройка автопостинга (SETUP)
    setup_select_group = State()
    setup_autopost_mode = State()
    setup_source_mode = State()
    setup_select_sources = State()
    setup_autopost_role = State()
    waiting_for_autopost_role_input = State()
    setup_blocked_topics = State()

    # Управление автопостингом (MANAGE)
    manage_source_mode = State()
    manage_select_sources = State()
    waiting_for_role_edit = State()
    waiting_for_blocked_topics_edit = State()
    waiting_for_autopost_edit = State()

    # Редактирование поста из очереди
    waiting_for_queue_post_edit = State()

# ===== 1. БАЗОВЫЕ КОМАНДЫ И НАВИГАЦИЯ (ГЛАВНОЕ МЕНЮ) =====

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    db = DatabaseManager()
    db.get_gpt_role(message.from_user.id) # Создаст дефолтную роль, если нет
    await state.clear()
    await message.answer("Добро пожаловать!", reply_markup=get_main_keyboard())

@router.message(F.text == "📝 Источники")
async def sources_menu(message: Message):
    await message.answer("📝 Управление источниками:", reply_markup=get_sources_keyboard())

@router.message(F.text == "📢 Паблики")
async def publics_menu(message: Message):
    await message.answer("📢 Управление пабликами:", reply_markup=get_publics_keyboard())

@router.message(F.text == "🤖 Автопостинг")
async def autopost_menu(message: Message):
    await message.answer("🤖 Автопостинг:", reply_markup=get_autopost_keyboard())

@router.message(F.text == "⚙️ Роль GPT")
async def gpt_menu(message: Message):
    await message.answer("⚙️ Настройка роли GPT:", reply_markup=get_gpt_keyboard())

@router.message(F.text == "⬅️ Назад в главное меню")
async def back_to_main_menu_message(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Главное меню:", reply_markup=get_main_keyboard())

@router.callback_query(F.data == "cancel")
async def cancel_action(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Действие отменено.")
    await callback.answer()

# ===== 2. УПРАВЛЕНИЕ ИСТОЧНИКАМИ, ПАБЛИКАМИ, РОЛЬЮ GPT =====

# --- Источники ---
@router.message(F.text == "Добавить источник")
async def add_source(message: Message, state: FSMContext):
    await state.set_state(SourceStates.waiting_for_source_link)
    await message.answer("Отправьте одну или несколько ссылок на источники (через пробел или с новой строки):", 
                         reply_markup=get_cancel_keyboard("cancel_add_source"))

@router.message(SourceStates.waiting_for_source_link)
async def process_source_link(message: Message, state: FSMContext):
    urls = message.text.split()
    if not urls:
        await message.answer("Вы не отправили ни одной ссылки. Попробуйте еще раз.")
        return

    validated_urls = []
    invalid_urls = []
    for url in urls:
        if validate_url(url, ALLOWED_DOMAINS):
            validated_urls.append(url)
        else:
            invalid_urls.append(url)

    if invalid_urls:
        await message.answer(f"Эти ссылки невалидны и будут проигнорированы:\n" + "\n".join(invalid_urls))

    if not validated_urls:
        await message.answer("Не найдено ни одной валидной ссылки. Попробуйте снова.")
        return

    await state.update_data(source_links=validated_urls, selected_themes=[])
    await state.set_state(SourceStates.waiting_for_source_themes)
    await message.answer("Теперь выберите темы для этих источников:", 
                         reply_markup=get_themes_keyboard())

@router.callback_query(F.data == "cancel_add_source")
async def cancel_add_source(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Добавление источника отменено.")
    await callback.message.answer("Главное меню", reply_markup=get_main_keyboard())
    await callback.answer()

@router.callback_query(F.data.startswith("theme_"), SourceStates.waiting_for_source_themes)
async def process_theme_selection(callback: CallbackQuery, state: FSMContext):
    theme = callback.data.split("_")[1]
    data = await state.get_data()
    selected_themes = data.get("selected_themes", [])
    
    if theme in selected_themes:
        selected_themes.remove(theme)
    else:
        selected_themes.append(theme)
    
    await state.update_data(selected_themes=selected_themes)
    await callback.message.edit_reply_markup(reply_markup=get_themes_keyboard(selected_themes))
    await callback.answer()

@router.callback_query(F.data == "custom_theme", SourceStates.waiting_for_source_themes)
async def custom_theme_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SourceStates.waiting_for_custom_theme)
    await callback.message.edit_text("Введите название вашей темы:")
    await callback.answer()

@router.message(SourceStates.waiting_for_custom_theme)
async def process_custom_theme(message: Message, state: FSMContext):
    custom_theme = message.text.strip()
    if not custom_theme:
        await message.answer("Название темы не может быть пустым.")
        return
        
    data = await state.get_data()
    selected_themes = data.get("selected_themes", [])
    if custom_theme not in selected_themes:
        selected_themes.append(custom_theme)
        # Также добавим в глобальный список тем на время сессии
        if custom_theme not in THEMES:
            THEMES.append(custom_theme)
            
    await state.update_data(selected_themes=selected_themes)
    await state.set_state(SourceStates.waiting_for_source_themes)
    await message.answer("Ваша тема добавлена. Выберите еще или нажмите 'Готово'.", 
                         reply_markup=get_themes_keyboard(selected_themes))

@router.callback_query(F.data == "done_themes", SourceStates.waiting_for_source_themes)
async def process_done_themes(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    links = data.get("source_links", [])
    themes = data.get("selected_themes", [])
    
    if not themes:
        await callback.answer("Вы не выбрали ни одной темы.", show_alert=True)
        return

    db = DatabaseManager()
    user_id = callback.from_user.id
    count = 0
    for link in links:
        db.add_source(user_id, link, themes)
        count += 1
    
    await state.clear()
    await callback.message.edit_text(f"✅ Успешно добавлено {count} источников с темами: {', '.join(themes)}.")
    await callback.message.answer("📝 Управление источниками:", reply_markup=get_sources_keyboard())
    await callback.answer()

@router.message(F.text == "Мои источники")
async def my_sources(message: Message):
    db = DatabaseManager()
    sources = db.get_user_sources(message.from_user.id)
    if not sources:
        await message.answer("У вас нет добавленных источников.")
        return
    text = "Ваши источники:\n\n" + "\n".join([f"- `{s['link']}` (Темы: {', '.join(s['themes']) if s['themes'] else 'не заданы'})" for s in sources])
    await message.answer(text, parse_mode="Markdown")

# --- Паблики ---

async def check_admin_rights(bot, channel: str) -> bool:
    """Проверяет, является ли бот админом с правом на постинг."""
    # Нормализуем ID канала
    if 't.me/' in channel:
        channel_id = '@' + channel.split('/')[-1]
    elif not channel.startswith('@'):
        channel_id = '@' + channel
    else:
        channel_id = channel

    try:
        member = await bot.get_chat_member(chat_id=channel_id, user_id=bot.id)
        if isinstance(member, (types.ChatMemberOwner, types.ChatMemberAdministrator)):
            if member.can_post_messages:
                return True
        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке прав в канале {channel_id}: {e}")
        return False

@router.message(F.text == "Добавить паблик")
async def add_group(message: Message, state: FSMContext):
    await state.set_state(SourceStates.waiting_for_group_url)
    await message.answer(
        "Отправьте ссылку на ваш Telegram-канал (в формате `https://t.me/channel_name` или `@channel_name`).\n\n"
        "Бот должен быть добавлен в администраторы канала с правом на публикацию постов.",
        reply_markup=get_cancel_keyboard("cancel_add_group"),
        parse_mode="Markdown"
    )

@router.message(SourceStates.waiting_for_group_url)
async def process_group_link(message: Message, state: FSMContext):
    channel_link = message.text.strip()
    if not (channel_link.startswith('@') or 't.me/' in channel_link):
        await message.answer("Неверный формат ссылки. Отправьте ссылку в формате `https://t.me/channel_name` или `@channel_name`.")
        return

    is_admin = await check_admin_rights(message.bot, channel_link)

    if is_admin:
        await state.update_data(group_link=channel_link, selected_themes=[])
        await state.set_state(SourceStates.waiting_for_group_themes)
        await message.answer(f"✅ Права в канале `{channel_link}` подтверждены. Теперь выберите темы для паблика:",
                             reply_markup=get_themes_keyboard(), parse_mode="Markdown")
    else:
        await state.update_data(group_link_to_check=channel_link)
        await message.answer(
            f"❌ Бот не является администратором в канале `{channel_link}` или у него нет прав на публикацию постов. "
            f"Пожалуйста, выдайте права и нажмите кнопку ниже.",
            reply_markup=get_recheck_admin_keyboard(channel_link),
            parse_mode="Markdown"
        )

@router.callback_query(F.data.startswith("recheck_admin_"))
async def recheck_admin_rights(callback: CallbackQuery, state: FSMContext):
    channel_link = callback.data.replace("recheck_admin_", "")
    is_admin = await check_admin_rights(callback.bot, channel_link)
    
    if is_admin:
        await state.update_data(group_link=channel_link, selected_themes=[])
        await state.set_state(SourceStates.waiting_for_group_themes)
        await callback.message.edit_text(f"✅ Права в канале `{channel_link}` подтверждены. Теперь выберите темы для паблика:",
                                         reply_markup=get_themes_keyboard(), parse_mode="Markdown")
    else:
        await callback.answer("Права все еще не предоставлены. Попробуйте еще раз.", show_alert=True)

@router.callback_query(F.data == "cancel_add_group")
async def cancel_add_group(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Добавление паблика отменено.")
    await callback.answer()

@router.callback_query(F.data.startswith("theme_"), SourceStates.waiting_for_group_themes)
async def process_group_theme_selection(callback: CallbackQuery, state: FSMContext):
    theme = callback.data.split("_")[1]
    data = await state.get_data()
    selected_themes = data.get("selected_themes", [])
    
    if theme in selected_themes:
        selected_themes.remove(theme)
    else:
        selected_themes.append(theme)
    
    await state.update_data(selected_themes=selected_themes)
    await callback.message.edit_reply_markup(reply_markup=get_themes_keyboard(selected_themes))
    await callback.answer()

@router.callback_query(F.data == "custom_theme", SourceStates.waiting_for_group_themes)
async def group_custom_theme_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SourceStates.waiting_for_group_custom_theme)
    await callback.message.edit_text("Введите название вашей темы:")
    await callback.answer()

@router.message(SourceStates.waiting_for_group_custom_theme)
async def process_group_custom_theme(message: Message, state: FSMContext):
    custom_theme = message.text.strip()
    if not custom_theme:
        await message.answer("Название темы не может быть пустым.")
        return
        
    data = await state.get_data()
    selected_themes = data.get("selected_themes", [])
    if custom_theme not in selected_themes:
        selected_themes.append(custom_theme)
        if custom_theme not in THEMES:
            THEMES.append(custom_theme)
            
    await state.update_data(selected_themes=selected_themes)
    await state.set_state(SourceStates.waiting_for_group_themes)
    await message.answer("Ваша тема добавлена. Выберите еще или нажмите 'Готово'.", 
                         reply_markup=get_themes_keyboard(selected_themes))

@router.callback_query(F.data == "done_themes", SourceStates.waiting_for_group_themes)
async def process_group_done_themes(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    link = data.get("group_link")
    themes = data.get("selected_themes", [])
    
    if not themes:
        await callback.answer("Вы не выбрали ни одной темы.", show_alert=True)
        return

    db = DatabaseManager()
    user_id = callback.from_user.id
    
    db.add_user_group(user_id, link, themes)
    
    await state.clear()
    await callback.message.edit_text(f"✅ Паблик `{link}` успешно добавлен с темами: {', '.join(themes)}.", parse_mode="Markdown")
    await callback.message.answer("📢 Управление пабликами:", reply_markup=get_publics_keyboard())
    await callback.answer()

@router.message(F.text == "Мои паблики")
async def my_groups(message: Message):
    db = DatabaseManager()
    groups = db.get_user_groups(message.from_user.id)
    if not groups:
        await message.answer("У вас нет добавленных пабликов.")
        return
    text = "Ваши паблики:\n\n" + "\n".join([f"- `{g['group_link']}`" for g in groups])
    await message.answer(text, parse_mode="Markdown")

# --- Роль GPT ---
@router.message(F.text == "Изменить роль GPT")
async def change_gpt_role(message: Message, state: FSMContext):
    await state.set_state(SourceStates.waiting_for_gpt_role)
    db = DatabaseManager()
    role = db.get_gpt_role(message.from_user.id)
    await message.answer(f"Текущая роль:\n`{role}`\n\nОтправьте новый текст роли:", 
                         reply_markup=get_cancel_keyboard(), parse_mode="Markdown")

@router.message(F.text == "Текущая роль GPT")
async def get_current_gpt_role(message: Message):
    db = DatabaseManager()
    role = db.get_gpt_role(message.from_user.id)
    await message.answer(f"Текущая основная роль GPT:\n\n`{role}`", parse_mode="Markdown")

@router.message(SourceStates.waiting_for_gpt_role)
async def process_gpt_role(message: Message, state: FSMContext):
    db = DatabaseManager()
    db.set_gpt_role(message.from_user.id, message.text)
    await state.clear()
    await message.answer("✅ Основная роль GPT обновлена!", reply_markup=get_main_keyboard())


# ===== 3. ЛОГИКА АВТОПОСТИНГА (НОВАЯ СТРУКТУРА) =====

# --- 3.1. Точки входа ---

@router.message(F.text == "Начать автопостинг в паблике")
async def start_autopost_setup_message(message: Message, state: FSMContext):
    db = DatabaseManager()
    user_groups = db.get_user_groups(message.from_user.id)
    if not user_groups:
        await message.answer("Сначала добавьте паблик в разделе '📢 Паблики'.", reply_markup=get_autopost_keyboard())
        return
    await state.clear()
    await state.set_state(SourceStates.setup_select_group)
    await message.answer(
        "Выберите паблик для настройки автопостинга:",
        reply_markup=get_user_groups_keyboard(user_groups, "setup_group_")
    )

@router.message(F.text == "Управление автопостингом в пабликах")
async def manage_autopost_start(message: Message, state: FSMContext):
    await state.clear()
    db = DatabaseManager()
    settings = db.get_autopost_settings(message.from_user.id)
    if not settings:
        await message.answer("У вас нет настроенных пабликов для автопостинга.", reply_markup=get_autopost_keyboard())
        return
    await message.answer(
        "⚙️ Управление автопостингом\n\nВыберите паблик для просмотра и изменения настроек:",
        reply_markup=get_autopost_management_keyboard(settings)
    )

@router.callback_query(F.data == "back_to_autopost_menu")
async def back_to_autopost_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    # Удаляем инлайн клавиатуру и отправляем обычную
    await callback.message.delete()
    await callback.message.answer("🤖 Автопостинг:", reply_markup=get_autopost_keyboard())
    await callback.answer()


# --- 3.2. Процесс НАСТРОЙКИ (SETUP) ---

@router.callback_query(F.data.startswith("setup_group_"))
async def setup_select_group(callback: CallbackQuery, state: FSMContext):
    group_link = callback.data.replace("setup_group_", "")
    await state.update_data(group_link=group_link)
    await state.set_state(SourceStates.setup_autopost_mode)
    await callback.message.edit_text(
        f"Выбран паблик: `{group_link}`\n\nТеперь выберите режим автопостинга:",
        reply_markup=get_autopost_mode_keyboard("setup_"), parse_mode="Markdown"
    )
    await callback.answer()

@router.callback_query(F.data.startswith("setup_autopost_mode_"))
async def setup_select_mode(callback: CallbackQuery, state: FSMContext):
    mode = callback.data.replace("setup_autopost_mode_", "")
    await state.update_data(autopost_mode=mode)
    await state.set_state(SourceStates.setup_source_mode)
    data = await state.get_data()
    mode_text = "👤 Контролируемый" if mode == "controlled" else "🤖 Автоматический"
    text = (f"Выбран паблик: `{data.get('group_link')}`\nРежим: {mode_text}\n\n"
            "Теперь выберите, как подбирать посты для рерайта:\n\n"
            "🤖 **Автоматически** - система сама найдет посты из ваших источников с похожими темами.\n"
            "✋ **Вручную** - вы сами укажете, из каких источников брать посты.")
    await callback.message.edit_text(text, reply_markup=get_source_selection_mode_keyboard("setup_"), parse_mode="Markdown")
    await callback.answer()

@router.callback_query(F.data == "setup_source_mode_auto", SourceStates.setup_source_mode)
async def setup_source_mode_auto(callback: CallbackQuery, state: FSMContext):
    await state.update_data(source_selection_mode='auto', selected_sources='[]')
    await setup_move_to_role_step(callback, state)

@router.callback_query(F.data == "setup_source_mode_manual", SourceStates.setup_source_mode)
async def setup_source_mode_manual(callback: CallbackQuery, state: FSMContext):
    await state.update_data(source_selection_mode='manual', current_page=0, selected_sources_ids=[])
    db = DatabaseManager()
    user_sources = db.get_user_sources(callback.from_user.id)
    if not user_sources:
        await callback.answer("У вас нет добавленных источников.", show_alert=True)
        return
    await state.set_state(SourceStates.setup_select_sources)
    await callback.message.edit_text(
        "Выберите источники (можно несколько). Когда закончите, нажмите 'Готово'.",
        reply_markup=get_user_sources_keyboard(user_sources, [], 0, "setup_")
    )
    await callback.answer()

@router.callback_query(F.data == 'setup_done_selecting_sources', SourceStates.setup_select_sources)
async def setup_done_selecting_sources(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected_ids = data.get('selected_sources_ids', [])
    if not selected_ids:
        await callback.answer("Вы не выбрали ни одного источника.", show_alert=True)
        return
    await state.update_data(selected_sources=json.dumps(selected_ids))
    await setup_move_to_role_step(callback, state)

async def setup_move_to_role_step(callback: CallbackQuery, state: FSMContext):
    db, user_id = DatabaseManager(), callback.from_user.id
    default_role = db.get_gpt_role(user_id)
    await state.update_data(autopost_role=default_role)
    await state.set_state(SourceStates.setup_autopost_role)
    text = f"Следующий шаг - роль GPT. Ваша основная роль:\n`{default_role}`\n\nИспользовать её или задать новую для этого паблика?"
    await callback.message.edit_text(text, reply_markup=get_autopost_role_selection_keyboard("setup_"), parse_mode="Markdown")
    await callback.answer()

@router.callback_query(F.data == 'setup_use_default_role', SourceStates.setup_autopost_role)
async def setup_use_default_role(callback: CallbackQuery, state: FSMContext):
    await setup_move_to_blocked_topics_step(callback, state)

@router.callback_query(F.data == 'setup_set_new_role', SourceStates.setup_autopost_role)
async def setup_set_new_role(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SourceStates.waiting_for_autopost_role_input)
    await callback.message.edit_text("Введите новый текст роли для GPT:", reply_markup=get_cancel_keyboard("setup_cancel_role"))
    await callback.answer()

@router.message(SourceStates.waiting_for_autopost_role_input)
async def setup_new_role_input(message: Message, state: FSMContext):
    if not message.text or not message.text.strip():
        await message.answer("Роль не может быть пустой.", reply_markup=get_cancel_keyboard("setup_cancel_role"))
        return
    await state.update_data(autopost_role=message.text.strip())
    await setup_move_to_blocked_topics_step(message, state)

async def setup_move_to_blocked_topics_step(event: Union[CallbackQuery, Message], state: FSMContext):
    await state.set_state(SourceStates.setup_blocked_topics)
    text = "Последний шаг: укажите через запятую темы для блокировки (например, `реклама, политика`).\nЕсли блокировать не нужно, нажмите 'Пропустить'."
    message = event.message if isinstance(event, CallbackQuery) else event
    
    # При вводе с клавиатуры, старое сообщение с кнопками нужно удалить и отправить новое
    if isinstance(event, Message):
        try:
            # Находим предыдущее сообщение бота, чтобы удалить его клавиатуру
            await event.bot.edit_message_reply_markup(chat_id=event.chat.id, message_id=event.message_id - 1, reply_markup=None)
        except Exception as e:
            logger.warning(f"Не удалось удалить старую клавиатуру: {e}")
    
    await message.answer(text, reply_markup=get_skip_keyboard("setup_skip_blocked_topics"))
    if isinstance(event, CallbackQuery): 
        await event.message.delete()
        await event.answer()

@router.callback_query(F.data == 'setup_skip_blocked_topics', SourceStates.setup_blocked_topics)
async def setup_skip_blocked_topics(callback: CallbackQuery, state: FSMContext):
    await state.update_data(blocked_topics=None)
    await finish_autopost_setup(callback, state)

@router.message(SourceStates.setup_blocked_topics)
async def setup_blocked_topics_input(message: Message, state: FSMContext):
    await state.update_data(blocked_topics=message.text)
    await finish_autopost_setup(message, state)

async def finish_autopost_setup(event: Union[CallbackQuery, Message], state: FSMContext):
    user_id = event.from_user.id
    data = await state.get_data()
    message = event.message if isinstance(event, CallbackQuery) else event
    try:
        db = DatabaseManager()
        group_link = data['group_link']
        
        # Удаляем старую настройку, если она была, и создаем новую
        db.delete_autopost_setting(user_id, group_link)
        db.add_autopost_setting(user_id, group_link, data['autopost_mode'])
        
        # Сохраняем остальные параметры
        db.save_selected_sources(user_id, group_link, data.get('selected_sources'))
        db.set_autopost_role(user_id, group_link, data.get('autopost_role'))
        db.set_blocked_topics(user_id, group_link, data.get('blocked_topics'))
        db.set_posts_count(user_id, group_link, 5) # значение по умолчанию
        
        await message.answer(f"✅ Настройка автопостинга для `{group_link}` завершена!", reply_markup=get_main_keyboard(), parse_mode="Markdown")
        if isinstance(event, CallbackQuery): 
            await event.message.delete()
    except Exception as e:
        logger.error(f"Ошибка при завершении настройки: {e}, data: {data}")
        await message.answer("Произошла ошибка при сохранении.", reply_markup=get_main_keyboard())
    finally:
        await state.clear()

# --- 3.3. Процесс УПРАВЛЕНИЯ (MANAGE) ---

@router.callback_query(F.data.startswith("manage_autopost_"))
async def manage_settings_menu(callback: CallbackQuery, state: FSMContext):
    """Отображает меню настроек для выбранной группы."""
    group_link = callback.data.split("manage_autopost_")[1]
    await _show_autopost_settings_menu(callback, callback.from_user.id, group_link, state)

@router.callback_query(F.data == "back_to_autopost_management")
async def back_to_autopost_management_list(callback: CallbackQuery, state: FSMContext):
    """Обработчик для кнопки 'Назад к списку' в меню настроек."""
    await state.clear()
    # Просто вызываем функцию, которая показывает список настроек
    await manage_autopost_start(callback.message, state)
    # Отвечаем на callback, чтобы убрать "часики"
    await callback.answer()
    # Удаляем предыдущее сообщение с настройками, чтобы не было дублей
    try:
        await callback.message.delete()
    except TelegramBadRequest:
        logger.warning("Не удалось удалить сообщение (возможно, оно уже удалено).")

async def _show_autopost_settings_menu(event: Union[CallbackQuery, Message], user_id: int, group_link: str, state: FSMContext):
    await state.set_state(None)
    await state.update_data(group_link=group_link)
    db = DatabaseManager()
    settings = db.get_autopost_settings_for_group(user_id, group_link)

    message = event if isinstance(event, Message) else event.message
    
    if not settings:
        await message.edit_text("Настройки для этого паблика не найдены. Возможно, они были удалены.")
        await manage_autopost_start(message, state) # Показать список снова
        return

    status = "🟢 Активен" if settings.get('is_active') else "🔴 Отключен"
    mode = "🤖 Автоматический" if settings.get('mode') == 'automatic' else "👤 Контролируемый"
    source_mode_text = "Авто" if settings.get('source_selection_mode', 'auto') == 'auto' else "Ручной"
    role = db.get_autopost_role(user_id, group_link)
    role_text = "Да (отличается от основной)" if role != db.get_gpt_role(user_id) else "Нет (используется основная)"
    topics = db.get_blocked_topics(user_id, group_link)
    topics_text = "Да" if topics else "Нет"
    
    text = (f"⚙️ **Настройки для:** `{group_link}`\n\n"
            f"**Статус:** {status}\n"
            f"**Режим:** {mode}\n"
            f"**Источники:** {source_mode_text}\n"
            f"**Отдельная роль:** {role_text}\n"
            f"**Запретные темы:** {topics_text}\n\n"
            "Выберите действие:")

    keyboard = get_autopost_settings_keyboard(group_link, settings.get('is_active'), settings.get('mode'))
    await message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
    if isinstance(event, CallbackQuery): await event.answer()

@router.callback_query(F.data.startswith("toggle_autopost_"))
async def manage_toggle_autopost(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    action, group_link = callback.data.replace("toggle_autopost_", "").split("_", 1)
    new_status = action == "resume"
    db = DatabaseManager()
    db.toggle_autopost_status(user_id, group_link, new_status)
    await callback.answer(f"Автопостинг {'запущен' if new_status else 'остановлен'}")
    await _show_autopost_settings_menu(callback, user_id, group_link, state)

@router.callback_query(F.data.startswith("change_mode_"))
async def manage_change_mode(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    new_mode, group_link = callback.data.replace("change_mode_", "").split("_", 1)
    db = DatabaseManager()
    db.update_autopost_mode(user_id, group_link, new_mode)
    await callback.answer(f"Режим изменен на {'автоматический' if new_mode == 'automatic' else 'контролируемый'}")
    await _show_autopost_settings_menu(callback, user_id, group_link, state)

@router.callback_query(F.data.startswith("delete_autopost_"))
async def manage_delete_autopost(callback: CallbackQuery, state: FSMContext):
    group_link = callback.data.replace("delete_autopost_", "")
    db = DatabaseManager()
    db.delete_autopost_setting(callback.from_user.id, group_link)
    await callback.answer(f"Настройки для {group_link} удалены.", show_alert=True)
    await callback.message.delete()
    # Показываем обновленный список
    await manage_autopost_start(callback.message, state)

@router.callback_query(F.data.startswith("manage_sources_"))
async def manage_sources_start(callback: CallbackQuery, state: FSMContext):
    """(MANAGE) Шаг 1: Показывает выбор режима источников (авто/ручной)."""
    group_link = callback.data.replace("manage_sources_", "")
    await state.update_data(group_link=group_link)
    await state.set_state(SourceStates.manage_source_mode)
    
    text = (f"Редактирование источников для `{group_link}`\n\n"
            "Выберите, как подбирать посты для рерайта:\n\n"
            "🤖 **Автоматически** - система сама найдет посты из ваших источников с похожими темами.\n"
            "✋ **Вручную** - вы сами укажете, из каких источников брать посты.")
    
    await callback.message.edit_text(
        text, 
        reply_markup=get_source_selection_mode_keyboard("manage_"),
        parse_mode="Markdown"
    )
    await callback.answer()

@router.callback_query(F.data == "manage_source_mode_auto", SourceStates.manage_source_mode)
async def manage_source_mode_auto(callback: CallbackQuery, state: FSMContext):
    """(MANAGE) Шаг 2.1: Выбран авто-подбор, сохраняем и возвращаемся."""
    data = await state.get_data()
    group_link = data.get("group_link")
    user_id = callback.from_user.id
    db = DatabaseManager()
    
    db.set_source_selection_mode(user_id, group_link, 'auto')
    
    await callback.answer("✅ Режим источников изменен на автоматический.", show_alert=True)
    await _show_autopost_settings_menu(callback, user_id, group_link, state)

@router.callback_query(F.data == "manage_source_mode_manual", SourceStates.manage_source_mode)
async def manage_source_mode_manual(callback: CallbackQuery, state: FSMContext):
    """(MANAGE) Шаг 2.2: Выбран ручной подбор, переходим к выбору источников."""
    data = await state.get_data()
    group_link = data.get("group_link")
    user_id = callback.from_user.id
    db = DatabaseManager()
    
    settings = db.get_autopost_settings_for_group(user_id, group_link)
    user_sources = db.get_user_sources(user_id)
    if not user_sources:
        await callback.answer("У вас нет добавленных источников.", show_alert=True)
        return

    try:
        selected_ids = json.loads(settings.get('selected_sources', '[]')) if settings.get('selected_sources') else []
    except (json.JSONDecodeError, TypeError):
        selected_ids = []

    await state.set_state(SourceStates.manage_select_sources)
    await state.update_data(selected_sources_ids=selected_ids, current_page=0)
    await callback.message.edit_text(
        f"Редактирование источников для `{group_link}`\n\n"
        "Выберите источники (можно несколько). Когда закончите, нажмите 'Сохранить'.",
        reply_markup=get_user_sources_keyboard(user_sources, selected_ids, 0, "manage_"),
        parse_mode="Markdown"
    )
    await callback.answer()

@router.callback_query(F.data.startswith("manage_role_"))
async def manage_role_start(callback: CallbackQuery, state: FSMContext):
    group_link = callback.data.replace("manage_role_", "")
    db = DatabaseManager()
    role = db.get_autopost_role(callback.from_user.id, group_link)

    await state.set_state(SourceStates.waiting_for_role_edit)
    await state.update_data(group_link=group_link)
    await callback.message.edit_text(
        f"Текущая роль для `{group_link}`:\n\n`{role}`\n\nОтправьте новый текст роли или напишите 'сброс', чтобы использовать основную роль.",
        reply_markup=get_cancel_keyboard("back_to_group_settings"),
        parse_mode="Markdown"
    )
    await callback.answer()

@router.callback_query(F.data.startswith("manage_topics_"))
async def manage_topics_start(callback: CallbackQuery, state: FSMContext):
    group_link = callback.data.replace("manage_topics_", "")
    db = DatabaseManager()
    topics = db.get_blocked_topics(callback.from_user.id, group_link)

    await state.set_state(SourceStates.waiting_for_blocked_topics_edit)
    await state.update_data(group_link=group_link)
    await callback.message.edit_text(
        f"Текущие запретные темы для `{group_link}`:\n\n`{topics if topics else 'Не заданы'}`\n\n"
        "Отправьте новый список тем через запятую или напишите 'нет', чтобы очистить.",
        reply_markup=get_cancel_keyboard("back_to_group_settings"),
        parse_mode="Markdown"
    )
    await callback.answer()

@router.message(SourceStates.waiting_for_role_edit)
async def manage_role_input(message: Message, state: FSMContext):
    data = await state.get_data()
    group_link = data.get("group_link")
    user_id = message.from_user.id
    db = DatabaseManager()
    
    role_text = message.text
    if role_text.lower() in ['сброс', 'reset', 'default']:
        role_text = None # Используется для сброса к основной роли

    db.set_autopost_role(user_id, group_link, role_text)
    await message.answer("✅ Роль для этого паблика обновлена.")
    await _show_autopost_settings_menu(message, user_id, group_link, state)

@router.message(SourceStates.waiting_for_blocked_topics_edit)
async def manage_topics_input(message: Message, state: FSMContext):
    data = await state.get_data()
    group_link = data.get("group_link")
    user_id = message.from_user.id
    db = DatabaseManager()

    topics_text = message.text
    if topics_text.lower() in ['нет', 'no', 'clear', 'очистить']:
        topics_text = None

    db.set_blocked_topics(user_id, group_link, topics_text)
    await message.answer("✅ Запретные темы обновлены.")
    await _show_autopost_settings_menu(message, user_id, group_link, state)

@router.callback_query(F.data == "manage_back_to_mode", SourceStates.manage_source_mode)
async def manage_back_from_source_selection_mode(callback: CallbackQuery, state: FSMContext):
    """(MANAGE) Навигация: от выбора режима источников назад к настройкам группы."""
    data = await state.get_data()
    group_link = data.get("group_link")
    await _show_autopost_settings_menu(callback, callback.from_user.id, group_link, state)
    
@router.callback_query(F.data == 'setup_cancel_role', SourceStates.waiting_for_autopost_role_input)
async def setup_cancel_role_input(callback: CallbackQuery, state: FSMContext):
    await setup_move_to_role_step(callback, state)

@router.callback_query(F.data == "back_to_group_settings")
async def back_to_group_settings(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    group_link = data.get("group_link")
    if not group_link:
        await callback.answer("Ошибка: не удалось найти паблик. Попробуйте снова.", show_alert=True)
        await state.clear()
        await callback.message.delete()
        await manage_autopost_start(callback.message, state)
        return
    await _show_autopost_settings_menu(callback, callback.from_user.id, group_link, state)
    await callback.answer()

# --- 3.4. Общие компоненты (пагинация, выбор и т.д.) ---

@router.callback_query(F.data.startswith(('setup_page_', 'manage_page_')), 
                       SourceStates.setup_select_sources, SourceStates.manage_select_sources)
async def process_sources_page(callback: CallbackQuery, state: FSMContext):
    prefix = "manage_" if callback.data.startswith("manage_") else "setup_"
    page = int(callback.data.split('_')[2])
    
    await state.update_data(current_page=page)
    data = await state.get_data()
    db, selected_ids = DatabaseManager(), data.get('selected_sources_ids', [])
    user_sources = db.get_user_sources(callback.from_user.id)
    
    await callback.message.edit_reply_markup(reply_markup=get_user_sources_keyboard(user_sources, selected_ids, page, prefix))
    await callback.answer()

@router.callback_query(F.data.startswith(('setup_select_source_', 'manage_select_source_')), 
                       SourceStates.setup_select_sources, SourceStates.manage_select_sources)
async def process_select_source(callback: CallbackQuery, state: FSMContext):
    prefix = "manage_" if callback.data.startswith("manage_") else "setup_"
    source_id = int(callback.data.split('_')[3])
    
    data = await state.get_data()
    selected_ids, page = data.get('selected_sources_ids', []), data.get('current_page', 0)
    
    if source_id in selected_ids: 
        selected_ids.remove(source_id)
    else: 
        selected_ids.append(source_id)
        
    await state.update_data(selected_sources_ids=selected_ids)
    db = DatabaseManager()
    user_sources = db.get_user_sources(callback.from_user.id)
    
    await callback.message.edit_reply_markup(reply_markup=get_user_sources_keyboard(user_sources, selected_ids, page, prefix))
    await callback.answer()

@router.callback_query(F.data == 'manage_done_selecting_sources', SourceStates.manage_select_sources)
async def manage_done_selecting_sources(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    group_link = data.get("group_link")
    user_id = callback.from_user.id
    selected_ids = data.get('selected_sources_ids', [])
    db = DatabaseManager()
    
    db.save_selected_sources(user_id, group_link, json.dumps(selected_ids))
    await callback.answer("✅ Источники обновлены.", show_alert=True)
    await _show_autopost_settings_menu(callback, user_id, group_link, state)

# --- 3.4. Управление постом в КОНТРОЛИРУЕМОМ режиме ---

@router.callback_query(F.data.startswith("approve_post_"))
async def approve_post_in_queue(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Одобрить'"""
    try:
        queue_id = int(callback.data.split("_")[-1])
        db = DatabaseManager()
        
        # Одобряем пост и выставляем текущее время для немедленной публикации
        success = db.approve_post_in_queue(queue_id)
        
        if success:
            await callback.message.edit_text(
                f"✅ Пост (ID: {queue_id}) одобрен и будет опубликован в ближайшее время.",
                reply_markup=None
            )
            await callback.answer("Одобрено!")
        else:
            await callback.message.edit_text(
                f"⚠️ Пост (ID: {queue_id}) уже был обработан или одобрен ранее.",
                reply_markup=None
            )
            await callback.answer("Пост уже обработан.", show_alert=True)

    except (ValueError, IndexError):
        await callback.answer("Ошибка: неверный ID поста.", show_alert=True)
    except Exception as e:
        logger.error(f"Ошибка при одобрении поста {callback.data}: {e}")
        await callback.answer("Произошла ошибка при одобрении.", show_alert=True)


@router.callback_query(F.data.startswith("cancel_post_"))
async def cancel_post_in_queue(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Отклонить'"""
    try:
        queue_id = int(callback.data.split("_")[-1])
        db = DatabaseManager()
        db.update_queue_status(queue_id, "canceled")

        await callback.message.edit_text(
            f"❌ Публикация поста (ID: {queue_id}) отменена.",
            reply_markup=None
        )
        await callback.answer("Публикация отменена.")
    except (ValueError, IndexError):
        await callback.answer("Ошибка: неверный ID поста.", show_alert=True)
    except Exception as e:
        logger.error(f"Ошибка при отмене поста {callback.data}: {e}")
        await callback.answer("Произошла ошибка при отмене.", show_alert=True)


@router.callback_query(F.data.startswith("edit_queued_post_"))
async def edit_post_in_queue(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки 'Редактировать'"""
    try:
        queue_id = int(callback.data.split("_")[-1])
        db = DatabaseManager()
        post_data = db.get_post_from_queue(queue_id)

        if not post_data:
            await callback.answer("Не удалось найти этот пост в очереди.", show_alert=True)
            return

        await state.set_state(SourceStates.waiting_for_queue_post_edit)
        await state.update_data(queue_id=queue_id)

        # Отправляем текст в виде `code` для легкого копирования
        await callback.message.answer(
            "Ниже текущий текст поста. Скопируйте его, отредактируйте и отправьте мне новый вариант.\n\n"
            "Для отмены просто нажмите на кнопку.",
            reply_markup=get_cancel_keyboard(f"cancel_edit_{queue_id}")
        )
        await callback.message.answer(f"```{post_data['post_text']}```", parse_mode="MarkdownV2")
        await callback.answer()

    except (ValueError, IndexError):
        await callback.answer("Ошибка: неверный ID поста.", show_alert=True)
    except Exception as e:
        logger.error(f"Ошибка при начале редактирования поста {callback.data}: {e}")
        await callback.answer("Произошла ошибка.", show_alert=True)


@router.message(SourceStates.waiting_for_queue_post_edit)
async def process_edited_post_text(message: Message, state: FSMContext):
    """Получение нового текста для поста из очереди"""
    data = await state.get_data()
    queue_id = data.get("queue_id")
    
    if not queue_id:
        await state.clear()
        await message.answer("Произошла ошибка, не найден ID поста. Попробуйте снова.")
        return

    new_text = message.text
    db = DatabaseManager()
    
    if db.update_queued_post_text(queue_id, new_text):
        await message.answer(
            f"✅ Текст для поста (ID: {queue_id}) успешно обновлен!\n\n"
            "Теперь вы можете нажать 'Одобрить' в исходном сообщении, когда будете готовы."
        )
    else:
        await message.answer("❌ Не удалось обновить текст поста. Попробуйте снова.")

    await state.clear()


@router.callback_query(F.data.startswith("cancel_edit_"))
async def cancel_edit_post_in_queue(callback: CallbackQuery, state: FSMContext):
    """Отмена процесса редактирования"""
    await state.clear()
    await callback.message.edit_text("Редактирование отменено.")
    await callback.answer()