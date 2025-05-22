from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database.models import Source
from database.DatabaseManager import DatabaseManager
from bot.keyboards.source_keyboards import (
    get_main_keyboard,
    get_themes_keyboard,
    get_confirmation_keyboard,
    get_source_actions_keyboard
)
from utils.validators import validate_url
from config.settings import ALLOWED_DOMAINS

router = Router()

class SourceStates(StatesGroup):
    waiting_for_url = State()
    waiting_for_theme = State()
    waiting_for_custom_theme = State()
    waiting_for_gpt_role = State()

@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "Добро пожаловать! Я помогу вам управлять источниками для анализа.\n\n"
        "Доступные команды:\n"
        "/set_role - установить роль для GPT\n"
        "/get_role - посмотреть текущую роль GPT\n"
        "/export - экспортировать источники в Excel\n"
        "/import - импортировать источники из Excel",
        reply_markup=get_main_keyboard()
    )

@router.message(F.text == "Добавить источник")
async def add_source(message: Message, state: FSMContext):
    await state.set_state(SourceStates.waiting_for_url)
    await message.answer("Пожалуйста, отправьте ссылку на источник (VK или Telegram)")

@router.message(SourceStates.waiting_for_url)
async def process_url(message: Message, state: FSMContext):
    url = message.text.strip()
    
    if not validate_url(url, ALLOWED_DOMAINS):
        await message.answer("Неверный формат ссылки. Пожалуйста, отправьте ссылку на VK или Telegram.")
        return
    
    await state.update_data(source_url=url)
    await state.set_state(SourceStates.waiting_for_theme)
    await message.answer(
        "Выберите тематику источника:",
        reply_markup=get_themes_keyboard()
    )

@router.callback_query(F.data.startswith("theme_"))
async def process_theme(callback: CallbackQuery, state: FSMContext):
    theme = callback.data.replace("theme_", "")
    
    if theme == "custom":
        await state.set_state(SourceStates.waiting_for_custom_theme)
        await callback.message.answer("Введите свою тематику:")
        return
    
    data = await state.get_data()
    source_url = data.get("source_url")
    
    # Сохраняем источник в базу данных
    db = DatabaseManager()
    source = Source(
        id=None,
        user_id=callback.from_user.id,
        source_url=source_url,
        theme=theme,
        created_at=None
    )
    
    try:
        db.add_source(source)
        await callback.message.answer(
            f"Источник успешно добавлен!\nURL: {source_url}\nТематика: {theme}",
            reply_markup=get_main_keyboard()
        )
    except Exception as e:
        await callback.message.answer(
            f"Произошла ошибка при добавлении источника: {str(e)}",
            reply_markup=get_main_keyboard()
        )
    
    await state.clear()

@router.message(SourceStates.waiting_for_custom_theme)
async def process_custom_theme(message: Message, state: FSMContext):
    theme = message.text.strip()
    data = await state.get_data()
    source_url = data.get("source_url")
    
    # Сохраняем источник в базу данных
    db = DatabaseManager()
    source = Source(
        id=None,
        user_id=message.from_user.id,
        source_url=source_url,
        theme=theme,
        created_at=None
    )
    
    try:
        db.add_source(source)
        await message.answer(
            f"Источник успешно добавлен!\nURL: {source_url}\nТематика: {theme}",
            reply_markup=get_main_keyboard()
        )
    except Exception as e:
        await message.answer(
            f"Произошла ошибка при добавлении источника: {str(e)}",
            reply_markup=get_main_keyboard()
        )
    
    await state.clear()

@router.message(F.text == "Мои источники")
async def show_sources(message: Message):
    db = DatabaseManager()
    sources = db.get_user_sources(message.from_user.id)
    
    if not sources:
        await message.answer("У вас пока нет добавленных источников.")
        return
    
    text = "Ваши источники:\n\n"
    for source in sources:
        text += f"ID: {source['id']}\nURL: {source['source_url']}\nТематика: {source['theme']}\n\n"
    
    await message.answer(text, reply_markup=get_main_keyboard())

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
            reply_markup=get_main_keyboard()
        )
    else:
        await message.answer(
            "Роль GPT не установлена. Нажмите кнопку 'Установить роль GPT' для установки роли.",
            reply_markup=get_main_keyboard()
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
            reply_markup=get_main_keyboard()
        )
    except Exception as e:
        await message.answer(
            f"Произошла ошибка при установке роли: {str(e)}",
            reply_markup=get_main_keyboard()
        )
    
    await state.clear() 