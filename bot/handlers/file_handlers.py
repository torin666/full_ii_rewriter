from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command
import os
import pandas as pd
from datetime import datetime

from database.DatabaseManager import DatabaseManager
from bot.keyboards.source_keyboards import get_main_keyboard

router = Router()

@router.message(Command("export"))
async def cmd_export(message: Message):
    db = DatabaseManager()
    sources = db.get_user_sources(message.from_user.id)
    
    if not sources:
        await message.answer("У вас нет источников для экспорта.")
        return
    
    # Создаем DataFrame
    df = pd.DataFrame(sources)
    
    # Создаем временную директорию, если её нет
    if not os.path.exists('temp'):
        os.makedirs('temp')
    
    # Генерируем имя файла с текущей датой и временем
    filename = f"temp/sources_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    # Сохраняем в Excel
    df.to_excel(filename, index=False)
    
    # Отправляем файл
    with open(filename, 'rb') as file:
        await message.answer_document(
            document=file,
            caption="Ваши источники в формате Excel",
            reply_markup=get_main_keyboard()
        )
    
    # Удаляем временный файл
    os.remove(filename)

@router.message(Command("import"))
async def cmd_import(message: Message):
    await message.answer(
        "Пожалуйста, отправьте Excel файл со списком источников.\n"
        "Файл должен содержать следующие колонки:\n"
        "- source_url: ссылка на источник\n"
        "- theme: тематика источника"
    ) 