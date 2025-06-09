from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from aiogram import Bot
from aiogram.enums import ParseMode
from parse_all_sources import parse_all_sources
from database.DatabaseManager import DatabaseManager
from ai.gpt.text_rewriter import rewriter
import aiohttp
from aiogram.types import FSInputFile
import tempfile
import os
import logging
import pandas as pd
from io import BytesIO
from aiogram.types import BufferedInputFile
from datetime import datetime

# Настраиваем логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()
db_manager = DatabaseManager()

class Scheduled:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.scheduler = AsyncIOScheduler()
        self.admin = [5147054199]  # ID админа
        self.db_manager = DatabaseManager()

        # Задача парсинга постов каждые 30 минут
        self.scheduler.add_job(
            self.parse_sources,
            CronTrigger.from_crontab('*/1 * * * *', timezone=pytz.timezone('Europe/Moscow'))
        )
        logger.info("Планировщик настроен на парсинг каждые 30 минут")

        # Задача обработки постов каждые 10 минут
        self.scheduler.add_job(
            self.run_parser,
            CronTrigger.from_crontab('*/10 * * * *', timezone=pytz.timezone('Europe/Moscow'))
        )
        logger.info("Планировщик настроен на обработку постов каждые 10 минут")

    async def parse_sources(self):
        """Парсинг всех источников"""
        try:
            logger.info("Начинаем парсинг источников")
            # Получаем все источники из links
            sources = self.db_manager.get_links()
            if not sources:
                logger.info("Нет источников для парсинга")
                return

            # Запускаем парсинг
            await parse_all_sources()
            logger.info("Парсинг источников успешно завершен")

        except Exception as e:
            logger.error(f"Ошибка при парсинге источников: {str(e)}")
            # Отправляем сообщение об ошибке админу
            for admin_id in self.admin:
                try:
                    await self.bot.send_message(
                        admin_id,
                        f"❌ Произошла ошибка при парсинге источников: {str(e)}"
                    )
                except Exception as send_error:
                    logger.error(f"Ошибка при отправке сообщения об ошибке админу {admin_id}: {str(send_error)}")

    async def run_parser(self):
        """
        Запускает парсер и отправляет результаты админу
        """
        try:
            logger.info("Начинаем обработку нового поста")
            # Получаем пост из базы данных
            post_data = self.db_manager.get_post()
            if not post_data:
                logger.info("Нет новых постов для обработки")
                return

            # Явно распаковываем текст и фото
            post_text = post_data.get('text')
            if not post_text:
                logger.error("Не удалось получить текст поста")
                for admin_id in self.admin:
                    try:
                        await self.bot.send_message(admin_id, "❌ Ошибка: не удалось получить текст поста из базы данных")
                    except Exception as send_error:
                        logger.error(f"Ошибка при отправке сообщения об ошибке админу {admin_id}: {str(send_error)}")
                return

            # Получаем ссылку на пост
            post_link = post_data.get('source_url')
            if not post_link:
                logger.error("Не удалось получить ссылку на пост")
                for admin_id in self.admin:
                    try:
                        await self.bot.send_message(admin_id, "❌ Ошибка: не удалось получить ссылку на пост")
                    except Exception as send_error:
                        logger.error(f"Ошибка при отправке сообщения об ошибке админу {admin_id}: {str(send_error)}")
                return

            logger.info(f"Начинаем переписывание текста для поста: {post_link}")
            # Переписываем текст с использованием роли первого админа
            result = await rewriter(post_text, post_link, self.admin[0])
            if not result or not result.get("text"):
                logger.error("Не удалось переписать текст")
                # Отправляем сообщение об ошибке админу
                for admin_id in self.admin:
                    try:
                        await self.bot.send_message(admin_id, "❌ Не удалось переписать текст. Пропускаем этот пост.")
                    except Exception as send_error:
                        logger.error(f"Ошибка при отправке сообщения об ошибке админу {admin_id}: {str(send_error)}")
                return

            # Формируем текст с учетом ограничения Telegram
            source_text = f"\n\nИсточник: {post_link}"
            max_text_length = 1024 - len(source_text)  # Оставляем место для ссылки
            main_text = result["text"].strip()
            
            # Если текст слишком длинный, обрезаем его
            if len(main_text) > max_text_length:
                logger.warning(f"Текст превышает максимальную длину ({len(main_text)} > {max_text_length}), обрезаем его")
                main_text = main_text[:max_text_length-3] + "..."
            
            final_text = main_text + source_text
            logger.info("Текст успешно подготовлен для отправки")

            for admin_id in self.admin:
                try:
                    logger.info(f"Отправляем текстовое сообщение админу {admin_id}")
                    await self.bot.send_message(admin_id, final_text)
                    logger.info(f"Сообщение успешно отправлено админу {admin_id}")
                except Exception as e:
                    logger.error(f"Ошибка при отправке сообщения админу {admin_id}: {str(e)}")
                    try:
                        await self.bot.send_message(admin_id, f"❌ Ошибка при отправке сообщения: {str(e)}")
                    except Exception as send_error:
                        logger.error(f"Не удалось отправить сообщение об ошибке админу {admin_id}: {str(send_error)}")

        except Exception as e:
            logger.error(f"Критическая ошибка в run_parser: {str(e)}")
            # Отправляем сообщение об ошибке админу
            for admin_id in self.admin:
                try:
                    await self.bot.send_message(admin_id, f"❌ Произошла критическая ошибка при обработке поста: {str(e)}")
                except Exception as send_error:
                    logger.error(f"Ошибка при отправке сообщения об ошибке админу {admin_id}: {str(send_error)}")

    async def send_report(self):
        """Отправка отчета администраторам"""
        global last_analysis_result, last_analysis_data

        # Проверяем наличие данных для отчета
        if not last_analysis_result or not last_analysis_data:
            logger.error("No analysis data available to send report")
            return

        try:
            posts_df, comments_df = last_analysis_data

            # Преобразуем timestamp в читаемый формат даты
            posts_df['date'] = pd.to_datetime(posts_df['date'], unit='s').dt.strftime('%d.%m.%Y')
            comments_df['date'] = pd.to_datetime(comments_df['date'], unit='s').dt.strftime('%d.%m.%Y')

            # Создаем Excel файл
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                posts_df.head(30).to_excel(writer, sheet_name='Топ посты', index=False)
                comments_df.head(30).to_excel(writer, sheet_name='Топ комментарии', index=False)
            excel_buffer.seek(0)

            # Логируем полный ответ GPT
            logger.info(f"Полный ответ GPT:\n{last_analysis_result}")

            # Отправляем всем администраторам
            admin_ids = [5147054199, 960425618, 504377809, 232081857]
            for admin_id in admin_ids:
                try:
                    # Отправляем анализ
                    await self.bot.send_message(
                        admin_id,
                        f"Источники: VK, Telegram\n\n"
                        f"{last_analysis_result}"
                    )

                    # Отправляем Excel файл с короткой подписью
                    await self.bot.send_document(
                        admin_id,
                        BufferedInputFile(
                            excel_buffer.getvalue(),
                            filename=f"report_{datetime.now().date()}.xlsx"
                        ),
                        caption="Топ-30 постов и комментариев за день"
                    )

                    logger.info(f"Report successfully sent to admin {admin_id}")

                except Exception as e:
                    logger.error(f"Error sending to admin {admin_id}: {e}")

            logger.info("Report sent successfully")

        except Exception as e:
            logger.error(f"Error in report sending: {e}")

    def start(self):
        """Запускает планировщик"""
        self.scheduler.start()
        logger.info("Планировщик запущен")





