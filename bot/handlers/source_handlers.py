from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from datetime import datetime
import logging

from database.DatabaseManager import DatabaseManager
from bot.keyboards.source_keyboards import (
    get_main_keyboard,
    get_sources_keyboard,
    get_publics_keyboard,
    get_autopost_keyboard,
    get_gpt_keyboard,
    get_themes_keyboard,
    get_confirmation_keyboard,
    get_source_actions_keyboard,
    get_user_groups_keyboard,
    get_publish_keyboard,
    get_admin_check_keyboard,
    get_post_edit_keyboard,
    get_inline_main_keyboard,
    get_autopost_mode_keyboard,
    get_autopost_management_keyboard,
    get_autopost_group_actions_keyboard,
    get_autopost_approval_keyboard,
    get_source_selection_mode_keyboard,
    get_user_sources_keyboard
)
from utils.validators import validate_url
from config.settings import ALLOWED_DOMAINS, THEMES
from ai.gpt.rewriter import rewriter
from utils.telegram_client import TelegramClientManager

logger = logging.getLogger(__name__)
router = Router()

class SourceStates(StatesGroup):
    waiting_for_url = State()
    waiting_for_themes = State()
    waiting_for_custom_theme = State()
    waiting_for_gpt_role = State()
    waiting_for_group_url = State()
    waiting_for_group_themes = State()
    waiting_for_group_selection = State()
    waiting_for_multiple_urls = State()
    waiting_for_url_themes = State()
    waiting_for_publish_target = State()
    waiting_for_admin_check = State()
    waiting_for_post_edit = State()
    # Состояния для автопостинга
    waiting_for_autopost_group_selection = State()
    waiting_for_autopost_edit = State()
    # Новые состояния для выбора источников
    waiting_for_source_selection_mode = State()
    waiting_for_source_selection = State()

@router.message(Command("start"))
async def cmd_start(message: Message):
    # Инициализируем базу данных
    db = DatabaseManager()
    
    try:
        # Добавляем пользователя с дефолтной ролью
        default_role = "Ты — журналист и редактор."
        db.set_gpt_role(message.from_user.id, default_role)
        
        # Отправляем приветственное сообщение
        await message.answer(
            "Добро пожаловать! Я помогу вам управлять источниками и пабликами для анализа.\n\n"
            "Доступные команды:\n"
            "/add_source - добавить источник для анализа\n"
            "/my_sources - посмотреть мои источники\n"
            "/add_group - добавить свой паблик\n"
            "/my_groups - посмотреть свои паблики\n"
            "/create_post - создать пост\n"
            "/set_role - установить роль для GPT\n"
            "/get_role - посмотреть текущую роль GPT\n\n"
            f"Для вас установлена базовая роль GPT: {default_role}",
            reply_markup=get_main_keyboard()
        )
    except Exception as e:
        # Если произошла ошибка, все равно показываем основное меню
        await message.answer(
            "Произошла ошибка при инициализации. Пожалуйста, попробуйте позже или обратитесь к администратору.",
            reply_markup=get_main_keyboard()
        )
        # Логируем ошибку для отладки
        logging.error(f"Ошибка при инициализации пользователя {message.from_user.id}: {str(e)}")

# ===== ОБРАБОТЧИКИ КАТЕГОРИЙ МЕНЮ =====

@router.message(F.text == "📝 Источники")
async def sources_menu(message: Message):
    """Переход в меню источников"""
    await message.answer(
        "📝 Управление источниками:",
        reply_markup=get_sources_keyboard()
    )

@router.message(F.text == "📢 Паблики")
async def publics_menu(message: Message):
    """Переход в меню пабликов"""
    await message.answer(
        "📢 Управление пабликами:",
        reply_markup=get_publics_keyboard()
    )

@router.message(F.text == "🤖 Автопостинг")
async def autopost_menu(message: Message):
    """Переход в меню автопостинга"""
    await message.answer(
        "🤖 Автопостинг:",
        reply_markup=get_autopost_keyboard()
    )

@router.message(F.text == "⚙️ Роль GPT")
async def gpt_menu(message: Message):
    """Переход в меню настройки GPT"""
    await message.answer(
        "⚙️ Настройка роли GPT:",
        reply_markup=get_gpt_keyboard()
    )

@router.message(F.text == "⬅️ Назад в главное меню")
async def back_to_main(message: Message, state: FSMContext):
    """Возврат в главное меню"""
    await state.clear()  # Очищаем любое состояние
    await message.answer(
        "Главное меню:",
        reply_markup=get_main_keyboard()
    )

# ===== ОБРАБОТЧИКИ ПАБЛИКОВ =====

@router.message(F.text == "Добавить паблик")
@router.message(Command("add_group"))
async def add_group_start(message: Message, state: FSMContext):
    await state.set_state(SourceStates.waiting_for_group_url)
    await message.answer(
        "Пожалуйста, отправьте ссылку на ваш Telegram канал/группу.\n"
        "Например: t.me/channel или @channel\n\n"
        "⚠️ Важно: \n"
        "- Принимаются только Telegram ссылки\n"
        "- Бот должен быть добавлен в канал/группу как администратор\n"
        "- После ввода ссылки будет проверка админских прав"
    )

@router.message(SourceStates.waiting_for_group_url)
async def process_group_url(message: Message, state: FSMContext):
    url = message.text.strip()
    
    # Проверяем, что это Telegram ссылка
    if not (url.startswith('t.me/') or url.startswith('@') or url.startswith('https://t.me/')):
        await message.answer(
            "❌ Принимаются только Telegram ссылки!\n\n"
            "Правильные форматы:\n"
            "• t.me/channel\n"
            "• @channel\n"
            "• https://t.me/channel\n\n"
            "Попробуйте еще раз:"
        )
        return
    
    # Извлекаем username из ссылки
    if url.startswith('https://t.me/'):
        username = url.replace('https://t.me/', '')
    elif url.startswith('t.me/'):
        username = url.replace('t.me/', '')
    elif url.startswith('@'):
        username = url[1:]
    else:
        username = url
    
    # Проверяем админские права
    await message.answer("🔍 Проверяю права бота в канале/группе...")
    
    rights_check = await TelegramClientManager.check_bot_admin_rights(username)
    
    if rights_check["error"]:
        await state.update_data(group_url=url, username=username)
        await state.set_state(SourceStates.waiting_for_admin_check)
        await message.answer(
            f"❌ Проблема с правами бота:\n\n"
            f"{rights_check['error']}\n\n"
            f"Убедитесь, что:\n"
            f"1. Бот добавлен в @{username}\n"
            f"2. Бот имеет права администратора\n"
            f"3. Включены права на отправку сообщений\n\n"
            f"После исправления нажмите кнопку перепроверки:",
            reply_markup=get_admin_check_keyboard()
        )
        return
    
    if not rights_check["is_admin"] or not rights_check["can_post"]:
        await state.update_data(group_url=url, username=username)
        await state.set_state(SourceStates.waiting_for_admin_check)
        await message.answer(
            f"❌ Недостаточно прав для публикации в @{username}\n\n"
            f"Права администратора: {'✅' if rights_check['is_admin'] else '❌'}\n"
            f"Права на публикацию: {'✅' if rights_check['can_post'] else '❌'}\n\n"
            f"Исправьте права и нажмите перепроверку:",
            reply_markup=get_admin_check_keyboard()
        )
        return
    
    # Все проверки пройдены
    await state.update_data(group_url=url, username=username)
    await state.set_state(SourceStates.waiting_for_group_themes)
    await message.answer(
        f"✅ Права проверены успешно!\n\n"
        f"Теперь выберите темы для @{username}:",
        reply_markup=get_themes_keyboard(THEMES, [], 0)
    )

@router.callback_query(SourceStates.waiting_for_group_themes)
async def process_group_themes(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected = data.get("selected_themes", [])
    page = data.get("page", 0)
    
    if callback.data.startswith("theme_"):
        theme = callback.data.replace("theme_", "")
        if theme == "prev":
            page = max(0, page - 1)
        elif theme == "next":
            page = min((len(THEMES) - 1) // 10, page + 1)  # Изменено с 4 на 10
        elif theme == "other":
            # Переходим к вводу пользовательской темы для группы
            await state.set_state(SourceStates.waiting_for_custom_theme)
            await callback.message.edit_text(
                "🔧 Введите вашу собственную тематику для паблика:\n\n"
                "Пример: 'Криптовалюты', 'Здоровье и медицина', 'Путешествия' и т.д."
            )
            await callback.answer()
            return
        elif theme == "confirm":
            if not selected:
                await callback.answer("Выберите хотя бы одну тему!")
                return
                
            db = DatabaseManager()
            group_url = data.get('group_url')
            username = data.get('username')
            
            # Добавляем паблик с выбранными темами
            try:
                db.add_user_group(callback.from_user.id, group_url, selected)
                await callback.message.edit_text(
                    f"✅ Паблик @{username} успешно добавлен!\n\n"
                    f"Темы: {', '.join(selected)}"
                )
                await state.clear()
                return
            except Exception as e:
                await callback.message.edit_text(f"❌ Ошибка при добавлении паблика: {str(e)}")
                await state.clear()
            return
        else:
            # Обработка выбора/отмены темы
            if theme in selected:
                selected.remove(theme)
            else:
                selected.append(theme)
        
        await state.update_data(selected_themes=selected, page=page)
        await callback.message.edit_reply_markup(
            reply_markup=get_themes_keyboard(THEMES, selected, page)
        )
    await callback.answer()

@router.callback_query(SourceStates.waiting_for_admin_check)
async def handle_admin_check(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    username = data.get("username")
    
    if callback.data == "recheck_admin":
        await callback.message.edit_text("🔍 Повторно проверяю права бота...")
        
        rights_check = await TelegramClientManager.check_bot_admin_rights(username)
        
        if rights_check["error"]:
            await callback.message.edit_text(
                f"❌ Проблема с правами бота:\n\n"
                f"{rights_check['error']}\n\n"
                f"Убедитесь, что:\n"
                f"1. Бот добавлен в @{username}\n"
                f"2. Бот имеет права администратора\n"
                f"3. Включены права на отправку сообщений\n\n"
                f"После исправления нажмите кнопку перепроверки:",
                reply_markup=get_admin_check_keyboard()
            )
            return
        
        if not rights_check["is_admin"] or not rights_check["can_post"]:
            await callback.message.edit_text(
                f"❌ Недостаточно прав для публикации в @{username}\n\n"
                f"Права администратора: {'✅' if rights_check['is_admin'] else '❌'}\n"
                f"Права на публикацию: {'✅' if rights_check['can_post'] else '❌'}\n\n"
                f"Исправьте права и нажмите перепроверку:",
                reply_markup=get_admin_check_keyboard()
            )
            return
        
        # Права проверены успешно
        await state.set_state(SourceStates.waiting_for_group_themes)
        await callback.message.edit_text(
            f"✅ Права проверены успешно!\n\n"
            f"Теперь выберите темы для @{username}:",
            reply_markup=get_themes_keyboard(THEMES, [], 0)
        )
        
    elif callback.data == "cancel_admin_check":
        await callback.message.edit_text(
            "❌ Добавление паблика отменено.",
            reply_markup=None
        )
        await state.clear()
    
    await callback.answer()

@router.message(F.text == "Мои паблики")
@router.message(Command("my_groups"))
async def show_user_groups(message: Message):
    db = DatabaseManager()
    groups = db.get_user_groups(message.from_user.id)
    
    if not groups:
        await message.answer(
            "У вас пока нет добавленных пабликов.\n"
            "Используйте кнопку 'Добавить паблик' чтобы добавить паблик.",
            reply_markup=get_publics_keyboard()
        )
        return
    
    text = "Ваши паблики:\n\n"
    for group in groups:
        text += f"URL: {group['group_link']}\nТемы: {', '.join(group['themes'])}\n\n"
    
    await message.answer(text, reply_markup=get_publics_keyboard())

@router.message(F.text == "Создать пост")
@router.message(Command("create_post"))
async def create_post_start(message: Message, state: FSMContext):
    db = DatabaseManager()
    groups = db.get_user_groups(message.from_user.id)
    
    if not groups:
        await message.answer(
            "У вас нет добавленных пабликов.\n"
            "Сначала добавьте паблик в разделе 'Паблики'",
            reply_markup=get_autopost_keyboard()
        )
        return
    
    await state.set_state(SourceStates.waiting_for_group_selection)
    await message.answer(
        "Выберите паблик для создания поста:",
        reply_markup=get_user_groups_keyboard(groups)
    )

@router.callback_query(F.data == "edit_post")
async def edit_post_start(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    post_text = data.get('post_text', '')
    
    if not post_text:
        await callback.answer("❌ Текст поста не найден", show_alert=True)
        return
    
    await state.set_state(SourceStates.waiting_for_post_edit)
    
    # Формируем сообщение для редактирования
    edit_message = f"📝 Текущий текст поста для копирования:\n\n```\n{post_text}\n```\n\nСкопируйте текст, отредактируйте его и отправьте исправленную версию:"
    
    # Проверяем тип сообщения: фото/видео или текст
    if callback.message.photo or callback.message.video:
        # Для фото/видео редактируем caption - ограничиваем длину до 1024 символов
        if len(edit_message) > 1024:
            edit_message = edit_message[:1021] + "..."
        
        await callback.message.edit_caption(
            caption=edit_message,
            parse_mode="Markdown"
        )
    else:
        # Для текстовых сообщений редактируем текст
        await callback.message.edit_text(
            edit_message,
            parse_mode="Markdown"
        )

@router.callback_query(F.data == "publish_to_group")
async def publish_to_group_start(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    group_link = data.get('group_link')
    post_text = data.get('post_text', '')
    post_image = data.get('post_image')
    
    # Если группа уже выбрана, публикуем сразу
    if group_link:
        # Извлекаем username из ссылки
        if group_link.startswith('https://t.me/'):
            username = group_link.replace('https://t.me/', '')
        elif group_link.startswith('t.me/'):
            username = group_link.replace('t.me/', '')
        elif group_link.startswith('@'):
            username = group_link[1:]
        else:
            username = group_link
        
        try:
            # Публикуем пост
            success = await TelegramClientManager.send_to_group(
                username,
                post_text,
                post_image
            )
            
            if success:
                # Показываем успешное уведомление
                if callback.message.photo or callback.message.video:
                    success_text = f"✅ Пост успешно опубликован в @{username}!"
                    if len(success_text) > 1024:
                        success_text = success_text[:1021] + "..."
                    await callback.message.edit_caption(
                        caption=success_text,
                        reply_markup=get_inline_main_keyboard()
                    )
                else:
                    await callback.message.edit_text(
                        f"✅ Пост успешно опубликован в @{username}!",
                        reply_markup=get_inline_main_keyboard()
                    )
            else:
                # Показываем ошибку
                if callback.message.photo or callback.message.video:
                    await callback.message.edit_caption(
                        caption=f"❌ Не удалось опубликовать пост в @{username}.\n"
                               "Проверьте права бота в группе/канале.",
                        reply_markup=get_inline_main_keyboard()
                    )
                else:
                    await callback.message.edit_text(
                        f"❌ Не удалось опубликовать пост в @{username}.\n"
                        "Проверьте права бота в группе/канале.",
                        reply_markup=get_inline_main_keyboard()
                    )
        except Exception as e:
            logger.error(f"Ошибка при публикации: {str(e)}")
            if callback.message.photo or callback.message.video:
                await callback.message.edit_caption(
                    caption=f"❌ Ошибка при публикации: {str(e)}",
                    reply_markup=get_inline_main_keyboard()
                )
            else:
                await callback.message.edit_text(
                    f"❌ Ошибка при публикации: {str(e)}",
                    reply_markup=get_inline_main_keyboard()
                )
        
        # Очищаем состояние после публикации
        await state.clear()
        return
    
    # Если группа не выбрана, запрашиваем у пользователя
    await state.set_state(SourceStates.waiting_for_publish_target)
    
    # Проверяем тип сообщения: фото/видео или текст
    if callback.message.photo or callback.message.video:
        # Для фото/видео редактируем caption
        await callback.message.edit_caption(
            caption="Укажите username группы/канала для публикации (без @):\n\n"
                   "Например: mygroup или mychannel\n\n"
                   "⚠️ Убедитесь, что бот добавлен в группу/канал как администратор!"
        )
    else:
        # Для текстовых сообщений редактируем текст
        await callback.message.edit_text(
            "Укажите username группы/канала для публикации (без @):\n\n"
            "Например: mygroup или mychannel\n\n"
            "⚠️ Убедитесь, что бот добавлен в группу/канал как администратор!"
        )

@router.callback_query(F.data == "main_menu")
async def handle_main_menu(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки возврата в главное меню"""
    await callback.message.answer(
        "Вы вернулись в главное меню:",
        reply_markup=get_main_keyboard()
    )
    await callback.answer()
    await state.clear()

@router.callback_query(F.data == "cancel_edit")
async def cancel_edit(callback: CallbackQuery, state: FSMContext):
    # Проверяем тип сообщения: фото/видео или текст
    if callback.message.photo or callback.message.video:
        # Для фото/видео редактируем caption
        await callback.message.edit_caption(
            caption="❌ Редактирование отменено.",
            reply_markup=get_inline_main_keyboard()
        )
    else:
        # Для текстовых сообщений редактируем текст
        await callback.message.edit_text(
            "❌ Редактирование отменено.",
            reply_markup=get_inline_main_keyboard()
        )
    await state.clear()

@router.callback_query(SourceStates.waiting_for_group_selection)
async def process_group_selection(callback: CallbackQuery, state: FSMContext):
    group_link = callback.data.replace("group_", "")
    db = DatabaseManager()
    
    # Получаем посты с похожими темами
    posts = db.get_similar_theme_posts(callback.from_user.id, group_link)
    
    if not posts:
        await callback.message.edit_text(
            "К сожалению, не найдено подходящих постов для генерации.\n"
            "Попробуйте позже или измените темы паблика."
        )
        await state.clear()
        return
    
    # Берем пост с наибольшим количеством лайков и комментариев
    best_post = posts[0]
    
    # Получаем роль пользователя и генерируем новый текст
    try:
        result = await rewriter(
            best_post['text'],
            best_post['post_link'],
            callback.from_user.id,
            best_post.get('photo_url')
        )
        
        if not result or not result.get("text"):
            await callback.message.edit_text(
                "Произошла ошибка при генерации текста. Попробуйте позже."
            )
            return
        
        # Помечаем пост как использованный после успешного переписывания
        db.mark_post_as_used(best_post['text'])
        
        # Используем только переписанный текст без добавления источника
        text = result['text']
        
        # Если есть медиафайл
        if result.get("image_url"):
            try:
                from aiogram.types import URLInputFile
                
                if result.get("is_video"):
                    # Отправляем видео с текстом И кнопками
                    video_file = URLInputFile(result["image_url"])
                    await callback.message.answer_video(
                        video_file,
                        caption=text,
                        reply_markup=get_publish_keyboard()
                    )
                else:
                    # Отправляем фото с текстом И кнопками - принудительно как фото
                    photo_file = URLInputFile(result["image_url"])
                    await callback.message.answer_photo(
                        photo_file,
                        caption=text,
                        reply_markup=get_publish_keyboard()
                    )
                
                # Если текст не поместился в caption, aiogram обработает это автоматически
                
                # Удаляем старое сообщение
                await callback.message.delete()
                
            except Exception as e:
                logger.error(f"Ошибка при отправке медиафайла: {str(e)}")
                # Если не удалось отправить медиафайл, отправляем только текст с кнопками
                await callback.message.edit_text(
                    text,
                    reply_markup=get_publish_keyboard()
                )
        else:
            # Если нет медиафайла, отправляем только текст с кнопкой публикации
            await callback.message.edit_text(
                text,
                reply_markup=get_publish_keyboard()
            )
        
        # Сохраняем данные поста для публикации
        await state.update_data(
            post_text=text,
            post_image=result.get("image_url"),
            group_link=group_link,
            is_video=result.get("is_video", False)
        )
        
        # НЕ очищаем состояние полностью, чтобы сохранить данные поста для кнопок
        # Только сбрасываем текущее состояние ожидания
        await state.set_state(None)
        
    except Exception as e:
        await callback.message.edit_text(
            f"Произошла ошибка при генерации поста: {str(e)}"
        )
        await state.clear()

@router.message(SourceStates.waiting_for_publish_target)
async def process_publish_target(message: Message, state: FSMContext):
    target = message.text.strip()
    data = await state.get_data()
    
    try:
        # Публикуем пост
        success = await TelegramClientManager.send_to_group(
            target,
            data.get('post_text', ''),
            data.get('post_image'),
            data.get('is_video', False)
        )
        
        if success:
            await message.answer(
                f"✅ Пост успешно опубликован в @{target}!",
                reply_markup=get_autopost_keyboard()
            )
        else:
            await message.answer(
                f"❌ Не удалось опубликовать пост в @{target}.\n"
                "Проверьте:\n"
                "- Правильность username\n"
                "- Права бота в группе/канале\n"
                "- Доступность группы/канала",
                reply_markup=get_autopost_keyboard()
            )
    except Exception as e:
        await message.answer(
            f"❌ Ошибка при публикации: {str(e)}",
            reply_markup=get_autopost_keyboard()
        )
    
    # Очищаем состояние только после публикации
    await state.clear()

@router.message(SourceStates.waiting_for_post_edit)
async def process_post_edit(message: Message, state: FSMContext):
    edited_text = message.text.strip()
    data = await state.get_data()
    
    # Обновляем текст поста
    await state.update_data(post_text=edited_text)
    
    # Если есть изображение, отправляем с изображением и кнопками
    if data.get('post_image'):
        try:
            from aiogram.types import URLInputFile
            
            # Принудительно отправляем как фото
            photo_file = URLInputFile(data['post_image'])
            await message.answer_photo(
                photo_file,
                caption=edited_text,
                reply_markup=get_publish_keyboard()
            )
            # Если текст не поместился в caption, aiogram обработает это автоматически
        except Exception as e:
            logger.error(f"Ошибка при отправке отредактированного поста с изображением: {str(e)}")
            # Если не удалось отправить изображение, отправляем только текст
            await message.answer(
                f"✅ Пост обновлен!\n\n{edited_text}",
                reply_markup=get_publish_keyboard()
            )
    else:
        # Если нет изображения, отправляем только текст с кнопками
        await message.answer(
            f"✅ Пост обновлен!\n\n{edited_text}",
            reply_markup=get_publish_keyboard()
        )
    
    # НЕ очищаем состояние полностью, чтобы сохранить данные для других кнопок
    # Только сбрасываем текущее состояние ожидания
    await state.set_state(None)

@router.message(F.text == "Добавить источники (одна тема)")
async def add_source_single_theme(message: Message, state: FSMContext):
    await state.set_state(SourceStates.waiting_for_url)
    await message.answer(
        "Пожалуйста, отправьте ссылки на источники (VK или Telegram).\n"
        "Можно вводить ссылки через пробел или с новой строки, например:\n\n"
        "vk.com/group1 vk.com/group2\n"
        "или\n"
        "vk.com/group1\n"
        "vk.com/group2\n"
        "t.me/channel1\n\n"
        "Все источники будут добавлены с одинаковыми темами.\n"
        "Если вы не нашли для себя подходящую тему или вам нужна уникальная тема для ваших источников, то выберете другое."
    )

@router.message(F.text == "Добавить источники (разные темы)")
async def add_source_multiple_themes(message: Message, state: FSMContext):
    await state.set_state(SourceStates.waiting_for_multiple_urls)
    await message.answer(
        "Пожалуйста, отправьте ссылки на источники (VK или Telegram).\n"
        "Каждую ссылку с новой строки или через пробел.\n"
        "После этого вы сможете выбрать тему для каждого источника отдельно.\n"
        "Если вы не нашли для себя подходящую тему или вам нужна уникальная тема для ваших источников, то выберете другое."
        
    )

@router.message(SourceStates.waiting_for_multiple_urls)
async def process_multiple_urls(message: Message, state: FSMContext):
    # Разбиваем текст сначала по переносам строк, потом по пробелам
    urls = []
    lines = message.text.strip().split('\n')
    for line in lines:
        urls.extend(line.strip().split())
    
    valid_urls = []
    invalid_urls = []
    
    for url in urls:
        url = url.strip()
        if validate_url(url, ALLOWED_DOMAINS):
            valid_urls.append(url)
        else:
            invalid_urls.append(url)
    
    if not valid_urls:
        await message.answer(
            "Все указанные ссылки некорректны. Пожалуйста, проверьте формат и попробуйте снова.\n"
            "Ссылки должны быть на VK или Telegram."
        )
        return
    
    if invalid_urls:
        await message.answer(
            f"Следующие ссылки некорректны и будут пропущены:\n{chr(10).join(invalid_urls)}"
        )
    
    # Удаляем дубликаты ссылок
    valid_urls = list(dict.fromkeys(valid_urls))
    
    await state.update_data(source_urls=valid_urls, current_url_index=0)
    await state.set_state(SourceStates.waiting_for_url_themes)
    await message.answer(
        f"Выберите темы для источника {valid_urls[0]}:",
        reply_markup=get_themes_keyboard(THEMES, [], 0)
    )

@router.callback_query(SourceStates.waiting_for_url_themes)
async def process_url_themes(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected = data.get("selected_themes", [])
    page = data.get("page", 0)
    urls = data.get("source_urls", [])
    current_index = data.get("current_url_index", 0)
    
    if callback.data.startswith("theme_"):
        theme = callback.data.replace("theme_", "")
        if theme == "prev":
            page = max(0, page - 1)
        elif theme == "next":
            page = min((len(THEMES) - 1) // 10, page + 1)  # Изменено с 4 на 10
        elif theme == "other":
            # Переходим к вводу пользовательской темы для URL
            await state.set_state(SourceStates.waiting_for_custom_theme)
            await callback.message.edit_text(
                "🔧 Введите вашу собственную тематику для этого источника:\n\n"
                "Пример: 'Криптовалюты', 'Здоровье и медицина', 'Путешествия' и т.д."
            )
            await callback.answer()
            return
        elif theme == "confirm":
            if not selected:
                await callback.answer("Выберите хотя бы одну тему!")
                return
            
            db = DatabaseManager()
            current_url = urls[current_index]
            
            try:
                # Добавляем текущий источник с выбранными темами
                db.add_source(callback.from_user.id, current_url, selected)
                
                # Если это был последний URL
                if current_index == len(urls) - 1:
                    await callback.message.edit_text(
                        "✅ Все источники успешно добавлены с выбранными темами!"
                    )
                    await state.clear()
                    return
                
                # Переходим к следующему URL
                current_index += 1
                await state.update_data(
                    current_url_index=current_index,
                    selected_themes=[],
                    page=0
                )
                
                # Запрашиваем темы для следующего URL
                await callback.message.edit_text(
                    f"Выберите темы для источника {urls[current_index]}:",
                    reply_markup=get_themes_keyboard(THEMES, [], 0)
                )
                
            except Exception as e:
                await callback.message.edit_text(
                    f"❌ Ошибка при добавлении источника {current_url}: {str(e)}"
                )
                await state.clear()
            return
            
        else:
            # Обработка выбора/отмены обычной темы
            if theme in selected:
                selected.remove(theme)
            else:
                selected.append(theme)
        
        await state.update_data(selected_themes=selected, page=page)
        await callback.message.edit_reply_markup(
            reply_markup=get_themes_keyboard(THEMES, selected, page)
        )
    await callback.answer()

@router.message(SourceStates.waiting_for_custom_theme)
async def process_custom_theme(message: Message, state: FSMContext):
    """Обработка пользовательской темы"""
    custom_theme = message.text.strip()
    
    if len(custom_theme) < 2:
        await message.answer("Тема слишком короткая. Введите более подробное описание:")
        return
    
    if len(custom_theme) > 50:
        await message.answer("Тема слишком длинная (максимум 50 символов). Сократите описание:")
        return
    
    # Получаем данные и добавляем пользовательскую тему к выбранным
    data = await state.get_data()
    selected = data.get("selected_themes", [])
    
    # Добавляем пользовательскую тему
    selected.append(custom_theme)
    
    await state.update_data(selected_themes=selected)
    
    # Определяем к какому состоянию возвращаться
    if data.get("group_url"):
        # Это для групп
        await state.set_state(SourceStates.waiting_for_group_themes)
        username = data.get('username', 'группа')
        await message.answer(
            f"✅ Добавлена пользовательская тема: '{custom_theme}'\n\n"
            f"Выбранные темы для @{username}: {', '.join(selected)}\n\n"
            "Вы можете выбрать еще темы или нажать 'Подтвердить':",
            reply_markup=get_themes_keyboard(THEMES, selected, data.get("page", 0))
        )
    elif data.get("source_urls"):
        # Это для URL с разными темами
        await state.set_state(SourceStates.waiting_for_url_themes)
        urls = data.get("source_urls", [])
        current_index = data.get("current_url_index", 0)
        current_url = urls[current_index] if current_index < len(urls) else "источника"
        await message.answer(
            f"✅ Добавлена пользовательская тема: '{custom_theme}'\n\n"
            f"Выбранные темы для {current_url}: {', '.join(selected)}\n\n"
            "Вы можете выбрать еще темы или нажать 'Подтвердить':",
            reply_markup=get_themes_keyboard(THEMES, selected, data.get("page", 0))
        )
    else:
        # Это для обычного добавления источников
        await state.set_state(SourceStates.waiting_for_themes)
        await message.answer(
            f"✅ Добавлена пользовательская тема: '{custom_theme}'\n\n"
            f"Выбранные темы: {', '.join(selected)}\n\n"
            "Вы можете выбрать еще темы или нажать 'Подтвердить':",
            reply_markup=get_themes_keyboard(THEMES, selected, data.get("page", 0))
        )

@router.message(F.text == "Мои источники")
async def show_sources(message: Message):
    db = DatabaseManager()
    sources = db.get_user_sources(message.from_user.id)
    
    if not sources:
        await message.answer("У вас пока нет добавленных источников.", reply_markup=get_sources_keyboard())
        return
    
    text = "Ваши источники:\n\n"
    for source in sources:
        # Обрабатываем themes - может быть строкой или списком
        themes = source['themes']
        if isinstance(themes, list):
            themes_str = ', '.join(themes)
        elif isinstance(themes, str):
            themes_str = themes
        else:
            themes_str = str(themes)
            
        text += f"ID: {source['id']}\nURL: {source['link']}\nТематика: {themes_str}\n\n"
    
    await message.answer(text, reply_markup=get_sources_keyboard())

@router.message(F.text == "Установить роль GPT")
async def set_gpt_role_button(message: Message, state: FSMContext):
    await state.set_state(SourceStates.waiting_for_gpt_role)
    await message.answer(
        "Пожалуйста, опишите роль для GPT. Например:\n"
        "'Ты - эксперт по маркетингу, который анализирует посты и комментарии с точки зрения маркетинговых стратегий'\n"
        "или\n"
        "'Ты - психолог, который анализирует эмоциональный окрас постов и комментариев'"
    )

@router.message(F.text == "Текущая роль GPT")
async def get_gpt_role_button(message: Message):
    db = DatabaseManager()
    role = db.get_gpt_role(message.from_user.id)
    
    if role:
        await message.answer(
            f"Текущая роль GPT:\n\n{role}",
            reply_markup=get_gpt_keyboard()
        )
    else:
        await message.answer(
            "Роль GPT не установлена. Нажмите кнопку 'Установить роль GPT' для установки роли.",
            reply_markup=get_gpt_keyboard()
        )

@router.message(SourceStates.waiting_for_gpt_role)
async def process_gpt_role(message: Message, state: FSMContext):
    role = message.text.strip()
    
    # Сохраняем роль в базу данных
    db = DatabaseManager()
    try:
        db.set_gpt_role(message.from_user.id, role)
        await message.answer(
            f"Роль GPT успешно установлена!\n\nТекущая роль:\n{role}",
            reply_markup=get_gpt_keyboard()
        )
    except Exception as e:
        await message.answer(
            f"Произошла ошибка при установке роли: {str(e)}",
            reply_markup=get_gpt_keyboard()
        )
    
    await state.clear()

@router.message(SourceStates.waiting_for_url)
async def process_url(message: Message, state: FSMContext):
    # Разбиваем текст сначала по переносам строк, потом по пробелам
    urls = []
    lines = message.text.strip().split('\n')
    for line in lines:
        urls.extend(line.strip().split())
    
    valid_urls = []
    invalid_urls = []
    
    for url in urls:
        url = url.strip()
        if validate_url(url, ALLOWED_DOMAINS):
            valid_urls.append(url)
        else:
            invalid_urls.append(url)
    
    if not valid_urls:
        await message.answer(
            "Все указанные ссылки некорректны. Пожалуйста, проверьте формат и попробуйте снова.\n"
            "Ссылки должны быть на VK или Telegram."
        )
        return
    
    if invalid_urls:
        await message.answer(
            f"Следующие ссылки некорректны и будут пропущены:\n{chr(10).join(invalid_urls)}"
        )
    
    # Удаляем дубликаты ссылок
    valid_urls = list(dict.fromkeys(valid_urls))
    
    await state.update_data(source_urls=valid_urls, selected_themes=[], page=0)
    await state.set_state(SourceStates.waiting_for_themes)
    await message.answer(
        f"Найдено {len(valid_urls)} уникальных корректных ссылок.\n"
        "Выберите общую тематику для всех источников:",
        reply_markup=get_themes_keyboard(THEMES, [], 0)
    )

# ===== ОБРАБОТЧИКИ АВТОПОСТИНГА =====

@router.message(F.text == "Начать автопостинг в паблике")
async def start_autopost(message: Message, state: FSMContext):
    """Начало настройки автопостинга"""
    db = DatabaseManager()
    groups = db.get_user_groups(message.from_user.id)
    
    if not groups:
        await message.answer(
            "У вас нет добавленных пабликов.\n"
            "Сначала добавьте ссылку в разделе 'Паблики'",
            reply_markup=get_autopost_keyboard()
        )
        return
    
    await state.set_state(SourceStates.waiting_for_autopost_group_selection)
    await message.answer(
        "Выберите паблик для настройки автопостинга:",
        reply_markup=get_user_groups_keyboard(groups)
    )

@router.callback_query(SourceStates.waiting_for_autopost_group_selection)
async def process_autopost_group_selection(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора группы для автопостинга"""
    # Проверяем что это именно выбор группы, а не режима
    if not callback.data.startswith("group_"):
        await callback.answer("Некорректные данные", show_alert=True)
        return
        
    group_link = callback.data.replace("group_", "")
    
    # Сохраняем выбранную группу и переходим к выбору режима источников
    await state.update_data(selected_group_link=group_link)
    await state.set_state(SourceStates.waiting_for_source_selection_mode)
    
    await callback.message.edit_text(
        f"Выбран паблик: {group_link}\n\n"
        "Теперь выберите режим источников для автопостинга:\n\n"
        "🤖 **Автоматический подбор** - система будет автоматически находить посты из ваших источников с похожими темами на выбранный паблик\n\n"
        "✋ **Выбрать источники** - вы сами выберете конкретные источники, из которых будут браться посты для этого паблика",
        reply_markup=get_source_selection_mode_keyboard(),
        parse_mode="Markdown"
    )

@router.callback_query(SourceStates.waiting_for_source_selection_mode)
async def process_source_selection_mode(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора режима источников"""
    data = await state.get_data()
    group_link = data.get('selected_group_link')
    
    if callback.data == "source_mode_info":
        # Показываем информацию о режимах источников
        await callback.answer(
            "🤖 АВТОМАТИЧЕСКИЙ:\n"
            "Система сама найдет похожие посты\n\n"
            "✋ ВЫБРАТЬ ИСТОЧНИКИ:\n"
            "Вы выберете конкретные источники",
            show_alert=True
        )
        return
    
    if callback.data == "source_mode_auto":
        # Автоматический режим - сразу переходим к выбору режима автопостинга
        await state.update_data(source_selection_mode='auto', selected_sources=None)
        # Сбрасываем состояние, чтобы кнопки режима автопостинга работали
        await state.set_state(None)
        await callback.message.edit_text(
            f"Выбран паблик: {group_link}\n"
            f"Режим источников: 🤖 Автоматический подбор\n\n"
            "Теперь выберите режим автопостинга:\n\n"
            "🤖 **Автоматический** - посты публикуются сразу без вашего подтверждения\n"
            "👤 **Контролируемый** - каждый пост отправляется вам на одобрение перед публикацией",
            reply_markup=get_autopost_mode_keyboard(group_link),
            parse_mode="Markdown"
        )
    elif callback.data == "source_mode_manual":
        # Ручной режим - переходим к выбору источников
        await state.update_data(source_selection_mode='manual')
        await state.set_state(SourceStates.waiting_for_source_selection)
        
        # Получаем источники пользователя
        db = DatabaseManager()
        sources = db.get_user_sources(callback.from_user.id)
        
        if not sources:
            await callback.message.edit_text(
                "❌ У вас нет добавленных источников.\n"
                "Сначала добавьте источники в разделе 'Источники'.",
                reply_markup=get_source_selection_mode_keyboard()
            )
            return
        
        await callback.message.edit_text(
            f"Выбран паблик: {group_link}\n"
            f"Режим источников: ✋ Выбрать источники\n\n"
            f"Выберите источники из {len(sources)} доступных:\n"
            "✅ - выбран, ☐ - не выбран",
            reply_markup=get_user_sources_keyboard(sources, [], 0)
        )
    elif callback.data == "back_to_autopost_setup":
        # Возврат к выбору группы
        db = DatabaseManager()
        groups = db.get_user_groups(callback.from_user.id)
        
        await state.set_state(SourceStates.waiting_for_autopost_group_selection)
        await callback.message.edit_text(
            "Выберите паблик для настройки автопостинга:",
            reply_markup=get_user_groups_keyboard(groups)
    )
    
    await callback.answer()

@router.callback_query(F.data.startswith("autopost_mode_"))
async def process_autopost_mode(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора режима автопостинга"""
    # Извлекаем режим и username из callback_data
    # Формат: autopost_mode_{mode}_{group_username}
    parts = callback.data.replace("autopost_mode_", "").split("_", 1)
    if len(parts) < 2:
        await callback.answer("Ошибка в данных", show_alert=True)
        return
        
    mode = parts[0]
    group_username = parts[1]
    
    if not group_username:
        await callback.answer("Ошибка: группа не выбрана", show_alert=True)
        return
    
    # Получаем полную ссылку группы из состояния
    data = await state.get_data()
    group_link = data.get('selected_group_link')
    
    if not group_link:
        await callback.answer("Ошибка: ссылка на группу не найдена", show_alert=True)
        return
    
    # Получаем данные выбора источников из состояния
    source_selection_mode = data.get('source_selection_mode', 'auto')
    selected_sources = data.get('selected_sources', None)
    
    try:
        db = DatabaseManager()
        # Сохраняем настройки автопостинга с информацией об источниках
        db.add_autopost_setting(
            callback.from_user.id, 
            group_link, 
            mode, 
            source_selection_mode, 
            selected_sources
        )
        
        mode_text = "🤖 Автоматический" if mode == "automatic" else "👤 Контролируемый"
        source_mode_text = "🤖 Автоматический подбор" if source_selection_mode == "auto" else "✋ Выбранные источники"
        
        success_message = (
                f"✅ Автопостинг настроен!\n\n"
                f"Паблик: {group_link}\n"
            f"Режим автопостинга: {mode_text}\n"
            f"Режим источников: {source_mode_text}\n"
        )
        
        if source_selection_mode == "manual" and selected_sources:
            success_message += f"Выбрано источников: {len(selected_sources)}\n"
        
        success_message += f"\nПосты будут генерироваться каждые 5 минут (±1 минута) на основе настроенных источников."
        
        try:
            await callback.message.edit_text(
                success_message,
                reply_markup=get_inline_main_keyboard()
            )
            await callback.answer(f"Настроен {mode_text.lower()} режим")
        except Exception as edit_error:
            # Если не удается отредактировать сообщение, отправляем новое
            await callback.message.answer(
                success_message,
                reply_markup=get_inline_main_keyboard()
            )
            await callback.answer(f"Настроен {mode_text.lower()} режим")
        
    except Exception as e:
        try:
            await callback.message.edit_text(
                f"❌ Ошибка при настройке автопостинга: {str(e)}"
            )
        except:
            await callback.message.answer(
                f"❌ Ошибка при настройке автопостинга: {str(e)}"
            )
    
    await state.clear()

@router.message(F.text == "Управление автопостингом в пабликах")
async def manage_autopost(message: Message):
    """Управление настройками автопостинга"""
    db = DatabaseManager()
    settings = db.get_autopost_settings(message.from_user.id)
    
    if not settings:
        await message.answer(
            "У вас нет настроенных автопостингов.\n"
            "Используйте 'Начать автопостинг в паблике' для настройки.",
            reply_markup=get_autopost_keyboard()
        )
        return
    
    text = "Управление автопостингом:\n\n"
    text += "🟢 - активен, 🔴 - приостановлен\n"
    text += "🤖 - автоматический, 👤 - контролируемый\n\n"
    text += "Выберите паблик для управления:"
    
    await message.answer(
        text,
        reply_markup=get_autopost_management_keyboard(settings)
    )

@router.callback_query(F.data.startswith("manage_autopost_"))
async def show_autopost_actions(callback: CallbackQuery):
    """Показывает действия для конкретной группы"""
    group_link = callback.data.replace("manage_autopost_", "")
    
    db = DatabaseManager()
    settings = db.get_autopost_settings(callback.from_user.id)
    
    # Находим настройки для выбранной группы
    group_setting = None
    for setting in settings:
        if setting['group_link'] == group_link:
            group_setting = setting
            break
    
    if not group_setting:
        await callback.answer("Настройки не найдены", show_alert=True)
        return
    
    mode_text = "🤖 Автоматический" if group_setting['mode'] == 'automatic' else "👤 Контролируемый"
    status_text = "🟢 Активен" if group_setting['is_active'] else "🔴 Приостановлен"
    
    await callback.message.edit_text(
        f"Настройки автопостинга для {group_link}:\n\n"
        f"Статус: {status_text}\n"
        f"Режим: {mode_text}\n\n"
        f"Выберите действие:",
        reply_markup=get_autopost_group_actions_keyboard(
            group_link, 
            group_setting['mode'], 
            group_setting['is_active']
        )
    )

@router.callback_query(F.data.startswith("change_mode_"))
async def change_autopost_mode(callback: CallbackQuery):
    """Изменяет режим автопостинга"""
    parts = callback.data.replace("change_mode_", "").split("_")
    if len(parts) < 2:
        await callback.answer("Ошибка в данных", show_alert=True)
        return
    
    new_mode = parts[-1]
    group_link = "_".join(parts[:-1])
    
    try:
        db = DatabaseManager()
        db.update_autopost_mode(callback.from_user.id, group_link, new_mode)
        
        mode_text = "🤖 Автоматический" if new_mode == 'automatic' else "👤 Контролируемый"
        await callback.answer(f"Режим изменен на {mode_text}")
        
        # Обновляем сообщение
        await show_autopost_actions(callback)
        
    except Exception as e:
        await callback.answer(f"Ошибка: {str(e)}", show_alert=True)

@router.callback_query(F.data.startswith("toggle_autopost_"))
async def toggle_autopost(callback: CallbackQuery):
    """Включает/выключает автопостинг"""
    parts = callback.data.replace("toggle_autopost_", "").split("_")
    if len(parts) < 2:
        await callback.answer("Ошибка в данных", show_alert=True)
        return
    
    action = parts[-1]
    group_link = "_".join(parts[:-1])
    is_active = action == "resume"
    
    try:
        db = DatabaseManager()
        db.toggle_autopost_status(callback.from_user.id, group_link, is_active)
        
        status_text = "возобновлен" if is_active else "приостановлен"
        await callback.answer(f"Автопостинг {status_text}")
        
        # Обновляем сообщение
        await show_autopost_actions(callback)
        
    except Exception as e:
        await callback.answer(f"Ошибка: {str(e)}", show_alert=True)

@router.callback_query(F.data.startswith("delete_autopost_"))
async def delete_autopost(callback: CallbackQuery):
    """Удаляет настройки автопостинга"""
    group_link = callback.data.replace("delete_autopost_", "")
    
    try:
        db = DatabaseManager()
        db.delete_autopost_setting(callback.from_user.id, group_link)
        
        await callback.message.edit_text(
            f"✅ Автопостинг для {group_link} удален.",
            reply_markup=get_inline_main_keyboard()
        )
        
    except Exception as e:
        await callback.answer(f"Ошибка: {str(e)}", show_alert=True)

@router.callback_query(F.data == "back_to_autopost_management")
async def back_to_autopost_management(callback: CallbackQuery):
    """Возврат к управлению автопостингом"""
    db = DatabaseManager()
    settings = db.get_autopost_settings(callback.from_user.id)
    
    text = "Управление автопостингом:\n\n"
    text += "🟢 - активен, 🔴 - приостановлен\n"
    text += "🤖 - автоматический, 👤 - контролируемый\n\n"
    text += "Выберите паблик для управления:"
    
    await callback.message.edit_text(
        text,
        reply_markup=get_autopost_management_keyboard(settings)
    )

# ===== ОБРАБОТЧИКИ ОДОБРЕНИЯ АВТОПОСТОВ =====

@router.callback_query(F.data.startswith("approve_autopost_"))
async def approve_autopost(callback: CallbackQuery, state: FSMContext):
    """Одобрение автопоста для публикации"""
    encoded_group_link = callback.data.replace("approve_autopost_", "")
    
    try:
        # Сразу отвечаем на callback чтобы предотвратить повторные нажатия
        await callback.answer()
        
        # Декодируем group_link из base64
        import base64
        group_link = base64.b64decode(encoded_group_link.encode()).decode()
        
        logger.info(f"👤 Пользователь {callback.from_user.id} одобряет пост для {group_link}")
        
        db = DatabaseManager()
        # Находим пост в очереди и помечаем как одобренный
        success = db.approve_autopost_in_queue(callback.from_user.id, group_link)
        
        if success:
            # Проверяем тип сообщения
            if callback.message.photo or callback.message.video:
                await callback.message.edit_caption(
                    caption=f"✅ Автопост одобрен для публикации в {group_link}"
                )
            else:
                await callback.message.edit_text(
                    f"✅ Автопост одобрен для публикации в {group_link}"
                )
        else:
            logger.warning(f"❌ Пост не найден в очереди для одобрения: user={callback.from_user.id}, group={group_link}")
            await callback.answer("Пост не найден в очереди или уже одобрен", show_alert=True)
            
    except Exception as e:
        await callback.answer(f"Ошибка: {str(e)}", show_alert=True)

@router.callback_query(F.data.startswith("edit_autopost_"))
async def edit_autopost_start(callback: CallbackQuery, state: FSMContext):
    """Начало редактирования автопоста"""
    encoded_group_link = callback.data.replace("edit_autopost_", "")
    
    try:
        # Декодируем group_link из base64
        import base64
        group_link = base64.b64decode(encoded_group_link.encode()).decode()
    except Exception as e:
        await callback.answer(f"Ошибка декодирования: {str(e)}", show_alert=True)
        return
    
    # Получаем текущий текст поста из caption или text
    current_text = ""
    if callback.message.caption:
        current_text = callback.message.caption
        # Убираем префикс "Готов пост для"
        if "Готов пост для" in current_text:
            current_text = current_text.split("\n\n", 1)[1] if "\n\n" in current_text else current_text
    elif callback.message.text:
        current_text = callback.message.text
        if "Готов пост для" in current_text:
            current_text = current_text.split("\n\n", 1)[1] if "\n\n" in current_text else current_text
    
    await state.set_state(SourceStates.waiting_for_autopost_edit)
    await state.update_data(
        autopost_group_link=group_link,
        autopost_current_text=current_text,
        autopost_has_photo=bool(callback.message.photo),
        autopost_has_video=bool(callback.message.video),
        autopost_photo_id=callback.message.photo[-1].file_id if callback.message.photo else None,
        autopost_video_id=callback.message.video.file_id if callback.message.video else None
    )
    
    # Формируем сообщение для редактирования как в "Создать пост"
    edit_message = f"📝 Текущий текст поста для копирования:\n\n```\n{current_text}\n```\n\nСкопируйте текст, отредактируйте его и отправьте исправленную версию:"
    
    # Ограничиваем длину до 1024 символов для caption
    if len(edit_message) > 1024:
        edit_message = edit_message[:1021] + "..."
    
    await callback.message.edit_caption(
        caption=edit_message,
        parse_mode="Markdown"
    )

@router.message(SourceStates.waiting_for_autopost_edit)
async def process_autopost_edit(message: Message, state: FSMContext):
    """Обработка редактирования автопоста"""
    edited_text = message.text.strip()
    data = await state.get_data()
    group_link = data.get('autopost_group_link')
    
    if not group_link:
        await message.answer("Ошибка: группа не найдена")
        await state.clear()
        return
    
    try:
        db = DatabaseManager()
        # Обновляем текст поста в очереди
        success = db.update_autopost_in_queue(message.from_user.id, group_link, edited_text)
        
        if success:
            # Отправляем обновленный пост с медиафайлами как в "Создать пост"
            message_text = f"✅ Автопост для {group_link} обновлен!\n\n{edited_text}"
            
            # Кодируем group_link для callback_data
            import base64
            encoded_group_link = base64.b64encode(group_link.encode()).decode()[:60]
            
            # Проверяем есть ли медиафайлы
            if data.get('autopost_has_photo') and data.get('autopost_photo_id'):
                await message.answer_photo(
                    data['autopost_photo_id'],
                    caption=message_text,
                    reply_markup=get_autopost_approval_keyboard(encoded_group_link)
                )
            elif data.get('autopost_has_video') and data.get('autopost_video_id'):
                await message.answer_video(
                    data['autopost_video_id'],
                    caption=message_text,
                    reply_markup=get_autopost_approval_keyboard(encoded_group_link)
                )
            else:
                await message.answer(
                    message_text,
                    reply_markup=get_autopost_approval_keyboard(encoded_group_link)
                )
        else:
            await message.answer("❌ Не удалось обновить пост")
            
    except Exception as e:
        await message.answer(f"❌ Ошибка при обновлении: {str(e)}")
    
    await state.clear()

@router.callback_query(F.data.startswith("cancel_autopost_"))
async def cancel_autopost(callback: CallbackQuery):
    """Отмена автопоста"""
    encoded_group_link = callback.data.replace("cancel_autopost_", "")
    
    try:
        # Сразу отвечаем на callback чтобы предотвратить повторные нажатия
        await callback.answer()
        
        # Декодируем group_link из base64
        import base64
        try:
        group_link = base64.b64decode(encoded_group_link.encode()).decode()
            logger.info(f"🔄 Пользователь {callback.from_user.id} отменяет пост для {group_link}")
        except Exception as decode_error:
            logger.error(f"❌ Ошибка декодирования group_link: {str(decode_error)}, encoded={encoded_group_link}")
            await callback.answer("Ошибка декодирования данных группы", show_alert=True)
            return
    except Exception as e:
        logger.error(f"❌ Ошибка при первичной обработке отмены: {str(e)}")
        await callback.answer(f"Ошибка: {str(e)}", show_alert=True)
        return
    
    try:
        db = DatabaseManager()
        
        # Сначала проверим, есть ли вообще pending посты для этого пользователя и группы
        logger.info(f"🔍 Проверяем наличие pending постов для user={callback.from_user.id}, group={group_link}")
        
        # Помечаем пост как отмененный
        success = db.cancel_autopost_in_queue(callback.from_user.id, group_link)
        
        if success:
            logger.info(f"✅ Пост успешно отменен для {group_link}")
            # Проверяем тип сообщения
            if callback.message.photo or callback.message.video:
            await callback.message.edit_caption(
                caption=f"❌ Автопост для {group_link} отменен."
            )
        else:
                await callback.message.edit_text(
                    f"❌ Автопост для {group_link} отменен."
                )
        else:
            logger.warning(f"❌ Пост не найден в очереди для отмены: user={callback.from_user.id}, group={group_link}")
            await callback.answer("Пост не найден в очереди или уже обработан", show_alert=True)
            
    except Exception as e:
        logger.error(f"❌ Ошибка при отмене автопоста: {str(e)}")
        await callback.answer(f"Ошибка при отмене: {str(e)}", show_alert=True)

@router.callback_query(SourceStates.waiting_for_themes)
async def process_themes_callback(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected = data.get("selected_themes", [])
    page = data.get("page", 0)
    
    if callback.data.startswith("theme_"):
        theme = callback.data.replace("theme_", "")
        if theme == "prev":
            page = max(0, page - 1)
        elif theme == "next":
            page = min((len(THEMES) - 1) // 10, page + 1)  # Изменено с 4 на 10
        elif theme == "other":
            # Переходим к вводу пользовательской темы
            await state.set_state(SourceStates.waiting_for_custom_theme)
            await callback.message.edit_text(
                "🔧 Введите вашу собственную тематику:\n\n"
                "Пример: 'Криптовалюты', 'Здоровье и медицина', 'Путешествия' и т.д."
            )
            await callback.answer()
            return
        elif theme == "confirm":
            if not selected:
                await callback.answer("Выберите хотя бы одну тему!")
                return
                
            db = DatabaseManager()
            urls = data.get("source_urls", [])
            
            success_count = 0
            errors = []
            
            # Добавляем каждый источник с выбранными темами
            for url in urls:
                try:
                    db.add_source(callback.from_user.id, url, selected)
                    success_count += 1
                except Exception as e:
                    errors.append(f"{url}: {str(e)}")
            
            # Формируем сообщение об успехе
            if success_count == len(urls):
                success_msg = "✅ Все источники успешно добавлены!\n\n"
            else:
                success_msg = f"✅ Успешно добавлено {success_count} из {len(urls)} источников.\n\n"
            
            success_msg += "Добавленные URL:\n"
            success_msg += "\n".join(urls)
            success_msg += f"\n\nТема: {', '.join(selected)}"
            
            if errors:
                success_msg += "\n\nОшибки при добавлении:\n"
                success_msg += "\n".join(errors)
            
            await callback.message.edit_text(success_msg)
            await state.clear()
            return
        else:
            # Обработка выбора/отмены обычной темы
            if theme in selected:
                selected.remove(theme)
            else:
                selected.append(theme)
        
        await state.update_data(selected_themes=selected, page=page)
        await callback.message.edit_reply_markup(
            reply_markup=get_themes_keyboard(THEMES, selected, page)
        )
    await callback.answer()

@router.callback_query(SourceStates.waiting_for_source_selection)
async def process_source_selection(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора источников в ручном режиме"""
    data = await state.get_data()
    group_link = data.get('selected_group_link')
    selected_sources = data.get('selected_sources', [])
    current_page = data.get('sources_page', 0)
    
    # Получаем источники пользователя
    db = DatabaseManager()
    sources = db.get_user_sources(callback.from_user.id)
    
    if callback.data.startswith("select_source_"):
        # Выбор/отмена источника
        source_id = int(callback.data.replace("select_source_", ""))
        
        if source_id in selected_sources:
            selected_sources.remove(source_id)
        else:
            selected_sources.append(source_id)
        
        await state.update_data(selected_sources=selected_sources)
        
        # Обновляем клавиатуру
        await callback.message.edit_reply_markup(
            reply_markup=get_user_sources_keyboard(sources, selected_sources, current_page)
        )
        await callback.answer()
        
    elif callback.data.startswith("sources_page_"):
        # Навигация по страницам
        new_page = int(callback.data.replace("sources_page_", ""))
        await state.update_data(sources_page=new_page)
        
        # Обновляем клавиатуру
        await callback.message.edit_reply_markup(
            reply_markup=get_user_sources_keyboard(sources, selected_sources, new_page)
        )
        await callback.answer()
        
    elif callback.data == "clear_source_selection":
        # Очистка выбора
        await state.update_data(selected_sources=[])
        
        # Обновляем клавиатуру
        await callback.message.edit_reply_markup(
            reply_markup=get_user_sources_keyboard(sources, [], current_page)
        )
        await callback.answer("Выбор очищен")
        
    elif callback.data == "confirm_source_selection":
        # Подтверждение выбора источников
        if not selected_sources:
            await callback.answer("Выберите хотя бы один источник!")
            return
        
        # Сохраняем выбранные источники и переходим к выбору режима автопостинга
        await state.update_data(selected_sources=selected_sources)
        
        # Получаем названия выбранных источников для отображения
        selected_names = []
        for source in sources:
            if source['id'] in selected_sources:
                link = source['link'][:30] + "..." if len(source['link']) > 30 else source['link']
                selected_names.append(link)
        
        await callback.message.edit_text(
            f"Выбран паблик: {group_link}\n"
            f"Режим источников: ✋ Выбрать источники\n"
            f"Выбрано источников: {len(selected_sources)}\n\n"
            f"Выбранные источники:\n" + "\n".join(f"• {name}" for name in selected_names[:5]) + 
            (f"\n... и еще {len(selected_names) - 5}" if len(selected_names) > 5 else "") + "\n\n"
            "Теперь выберите режим автопостинга:\n\n"
            "🤖 **Автоматический** - посты публикуются сразу без вашего подтверждения\n"
            "👤 **Контролируемый** - каждый пост отправляется вам на одобрение перед публикацией",
            reply_markup=get_autopost_mode_keyboard(group_link),
            parse_mode="Markdown"
        )
        await callback.answer()
        
    elif callback.data == "back_to_source_mode":
        # Возврат к выбору режима источников
        await state.set_state(SourceStates.waiting_for_source_selection_mode)
        await callback.message.edit_text(
            f"Выбран паблик: {group_link}\n\n"
            "Теперь выберите режим источников для автопостинга:\n\n"
            "🤖 **Автоматический подбор** - система будет автоматически находить посты из ваших источников с похожими темами на выбранный паблик\n\n"
            "✋ **Выбрать источники** - вы сами выберете конкретные источники, из которых будут браться посты для этого паблика",
            reply_markup=get_source_selection_mode_keyboard(),
            parse_mode="Markdown"
        )
    await callback.answer() 