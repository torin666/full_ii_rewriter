import asyncio
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Set
import os
import psutil
import gc
from aiogram import Bot
from aiogram.types import URLInputFile, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from database.DatabaseManager import DatabaseManager
from utils.telegram_client import TelegramClientManager
from bot.keyboards.source_keyboards import get_autopost_approval_keyboard, get_post_approval_keyboard
from ai.gpt.rewriter import rewriter
import aiohttp
import tempfile
import pytz

logger = logging.getLogger(__name__)


class AutopostManager:
    
    def __init__(self, bot: Bot, db: DatabaseManager = None, telegram_manager: TelegramClientManager = None):
        """Инициализация менеджера автопостинга"""
        self.bot = bot
        self.db = db or DatabaseManager()
        self.telegram_manager = telegram_manager
        self.is_running = False
        self.processing_posts: Set[str] = set()  # Для предотвращения дублирования
        self.autopost_task = None
        self.pending_posts_task = None

    def is_post_used(self, text: str) -> bool:
        """Проверяет, был ли пост уже использован"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT using_post FROM {self.db.schema}.posts 
                        WHERE text = %s
                    """, (text,))
                    result = cur.fetchone()
                    return result and result[0] == 'True'
        except Exception as e:
            logger.error(f"Ошибка проверки использования поста: {e}")
            return False

    async def start_autopost_loop(self):
        """Запуск основного цикла автопостинга"""
        try:
            self.is_running = True
            logger.info("🚀 Запуск цикла автопостинга")
            
            # Запускаем основной цикл автопостинга
            self.autopost_task = asyncio.create_task(self.process_autopost_cycle())
            
            # Запускаем цикл обработки ожидающих постов
            self.pending_posts_task = asyncio.create_task(self.process_pending_posts_cycle())
            
            # Ждем завершения задач
            await asyncio.gather(self.autopost_task, self.pending_posts_task)
            
        except Exception as e:
            logger.error(f"❌ Ошибка в цикле автопостинга: {e}")
        finally:
            self.is_running = False

    async def stop(self):
        """Остановка автопостинга"""
        self.is_running = False
        if self.autopost_task:
            self.autopost_task.cancel()
        if self.pending_posts_task:
            self.pending_posts_task.cancel()
        logger.info("🛑 Автопостинг остановлен")

    async def process_autopost_cycle(self):
        """Основной цикл обработки автопостинга"""
        while self.is_running:
            try:
                # Получаем активные группы для автопостинга
                groups = self.db.get_active_autopost_groups()
                logger.info(f"📊 Найдено {len(groups)} активных групп для автопостинга")
                
                if not groups:
                    logger.info("💤 Нет активных групп, ожидание...")
                    await asyncio.sleep(60)  # Проверяем каждую минуту
                    continue
                
                # Обрабатываем каждую группу
                for group in groups:
                    try:
                        await self.process_group_autopost(
                            group['user_id'], 
                            group['group_link'], 
                            group['mode']
                        )
                        await asyncio.sleep(2)  # Пауза между группами
                    except Exception as e:
                        logger.error(f"❌ Ошибка обработки группы {group['group_link']}: {e}")
                        continue
                
                # Пауза перед следующим циклом
                await asyncio.sleep(30)  # Проверяем каждые 30 секунд
                
            except Exception as e:
                logger.error(f"❌ Ошибка в цикле автопостинга: {e}")
                await asyncio.sleep(60)

    async def process_group_autopost(self, user_id: int, group_link: str, mode: str):
        """
        Обрабатывает автопостинг для группы, перебирая посты до первого успешного.
        """
        try:
            logger.info(f"🚀 Начинаем автопостинг для группы: {group_link} (режим: {mode})")
            
            # 1. Получаем до 10 постов-кандидатов
            candidate_posts = self.db.get_multiple_theme_posts(user_id, group_link, limit=10)
            if not candidate_posts:
                logger.warning(f"🤷‍♂️ Не найдены посты-кандидаты для {group_link}")
                return

            # 2. Получаем оригинальные тексты уже опубликованных сегодня постов
            published_today = self.db.get_published_posts_today(group_link)
            published_texts = [p.get('text', '') for p in published_today]
            logger.info(f"📊 Найдено {len(candidate_posts)} кандидатов. Опубликовано сегодня: {len(published_today)}. Начинаем проверку на уникальность.")

            # 3. Перебираем кандидатов в поисках уникального
            post_to_process = None
            for post in candidate_posts:
                is_duplicate = False
                candidate_text = post.get('text', '')
                if not candidate_text:
                    continue

                for published_text in published_texts:
                    # Порог схожести можно настроить, 0.8 - довольно строгий
                    if self.db.compare_texts(candidate_text, published_text, threshold=0.85):
                        logger.info(f"   - Кандидат {post['post_link'][:40]}... похож на уже опубликованный пост. Пропускаем.")
                        is_duplicate = True
                        break
                
                if not is_duplicate:
                    logger.info(f"✅ Найден уникальный пост для обработки: {post['post_link']}")
                    post_to_process = post
                    break  # Нашли уникальный пост, выходим из цикла проверки

            # 4. Если уникальный пост не найден после проверки всех кандидатов
            if not post_to_process:
                logger.warning(f"🙅‍♂️ Уникальные посты не найдены для {group_link} после проверки {len(candidate_posts)} кандидатов.")
                # Обновляем время, чтобы не проверять эту же группу слишком часто
                self.db.update_next_post_time(group_link)
                return

            # 5. Обрабатываем найденный уникальный пост
            logger.info(f"✍️ Отправляем на переработку пост: {post_to_process['post_link']}")
            
            rewriter_result = await rewriter(
                text=post_to_process['text'],
                post_link=post_to_process['post_link'],
                user_id=user_id,
                photo_url=post_to_process.get('photo_url'),
                group_link=group_link
            )
            
            # Проверяем, заблокирован ли пост
            if rewriter_result.get('blocked'):
                logger.warning(f"🚫 Пост {post_to_process['post_link']} заблокирован. Причина: {rewriter_result.get('blocked_reason')}")
                self.db.mark_post_as_used(post_to_process['post_link'])
                return  # Завершаем обработку, т.к. пост заблокирован

            new_text = rewriter_result.get('text')
            if not new_text:
                logger.error(f"❌ Не удалось переписать текст для поста {post_to_process['post_link']}. Пропускаем.")
                return

            logger.info(f"✅ Текст для поста {post_to_process['post_link']} успешно переписан.")
            
            # 6. Отправляем на публикацию или на проверку
            scheduled_time = datetime.now(pytz.timezone('Europe/Moscow'))
            
            queue_id = self.db.add_autopost_to_queue(
                user_id=user_id,
                group_link=group_link,
                original_post_url=post_to_process['post_link'],
                text=new_text,
                image_url=rewriter_result.get('image_url'),
                is_video=rewriter_result.get('is_video', False),
                scheduled_time=scheduled_time,
                mode=mode
            )

            if mode == 'automatic':
                self.db.update_queue_status(queue_id, 'approved')
                logger.info(f"✅ Пост ID {queue_id} для {group_link} добавлен и сразу одобрен.")
            else:
                self.db.update_queue_status(queue_id, 'sent_for_approval')
                await self.send_post_for_approval(
                    user_id=user_id, 
                    group_link=group_link, 
                    text=new_text, 
                    image_url=rewriter_result.get('image_url'), 
                    is_video=rewriter_result.get('is_video', False),
                    queue_id=queue_id
                )
                logger.info(f"✅ Пост ID {queue_id} для {group_link} отправлен на одобрение.")

            # Помечаем исходный пост как использованный, чтобы не брать его снова
            self.db.mark_post_as_used(post_to_process['post_link'])
            
            # Обновляем время следующего поста, чтобы предотвратить спам
            logger.info(f"⏰ Обновляем время следующего поста для группы {group_link}.")
            self.db.update_next_post_time(group_link)

        except Exception as e:
            logger.error(f"❌ Критическая ошибка в process_group_autopost для {group_link}: {e}")
            import traceback
            traceback.print_exc()

    async def send_post_for_approval(self, user_id, group_link, text, image_url=None, is_video=False, queue_id=None):
        """Отправляет пост на одобрение пользователю."""
        if queue_id is None:
            logger.error("Ошибка: для отправки на одобрение требуется queue_id!")
            return

        try:
            keyboard = get_post_approval_keyboard(queue_id)
            
            message_text = (
                f"👇 Ваш пост для группы `{group_link}` готов к публикации.\n\n"
                f"---\n\n{text}"
            )

            # Проверяем, есть ли медиафайл и существует ли он
            media_file = None
            if image_url:
                # Проверяем, является ли это локальным путем
                if os.path.exists(image_url):
                    # Локальный файл - проверяем его существование
                    if os.path.isfile(image_url):
                        media_file = FSInputFile(image_url)
                        logger.info(f"📁 Используем локальный файл: {image_url}")
                    else:
                        logger.warning(f"⚠️ Указанный путь не является файлом: {image_url}")
                else:
                    # Это URL - проверяем, что это действительно URL
                    if image_url.startswith(('http://', 'https://')):
                        media_file = URLInputFile(image_url)
                        logger.info(f"🌐 Используем URL файл: {image_url}")
                    else:
                        logger.warning(f"⚠️ Некорректный URL или путь: {image_url}")

            if media_file:
                try:
                    if is_video:
                        await self.bot.send_video(
                            user_id,
                            media_file,
                            caption=message_text,
                            reply_markup=keyboard,
                            parse_mode="Markdown"
                        )
                    else:
                        await self.bot.send_photo(
                            user_id,
                            media_file,
                            caption=message_text,
                            reply_markup=keyboard,
                            parse_mode="Markdown"
                        )
                    logger.info(f"✅ Пост с медиафайлом отправлен на одобрение (ID: {queue_id})")
                except Exception as media_error:
                    logger.error(f"Ошибка при отправке поста с медиафайлом (ID: {queue_id}): {media_error}")
                    # Fallback: отправляем только текст
                    await self.bot.send_message(
                        user_id,
                        message_text,
                        reply_markup=keyboard,
                        parse_mode="Markdown"
                    )
                    logger.info(f"✅ Пост без медиафайла отправлен на одобрение (ID: {queue_id})")
            else:
                # Отправляем только текст
                await self.bot.send_message(
                    user_id,
                    message_text,
                    reply_markup=keyboard,
                    parse_mode="Markdown"
                )
                logger.info(f"✅ Пост без медиафайла отправлен на одобрение (ID: {queue_id})")

            if result:
                # Добавляем запись в таблицу опубликованных постов
                self.db.add_published_post(
                    group_link=group_link,
                    text=post.get('post_text'),
                    post_link=post.get('original_post_url')
                )
                logger.info(f"✅ Пост успешно опубликован в {group_link} и добавлен в 'published_posts'")
                return True
            else:
                return False
        except TelegramBadRequest as e:
            if "can't parse entities" in e.message:
                logger.error(f"❌ Ошибка парсинга Markdown для поста в {group_link}. Текст: '{post.get('post_text')}'")

    async def publish_to_group(self, user_id: int, group_link: str, text: str, image_url: str = None, is_video: bool = False):
        """Публикует пост в группу и уведомляет пользователя."""
        try:
            # Получаем telegram_id из group_link
            try:
                # Если ссылка вида https://t.me/channel_name
                if 't.me/' in group_link:
                    target_id = f"@{group_link.split('t.me/')[1]}"
                # Если ссылка вида @channel_name
                elif group_link.startswith('@'):
                    target_id = group_link
                # Если числовой ID
                else:
                    target_id = int(group_link)
            except Exception as e:
                logger.error(f"Некорректный формат group_link: {group_link} - {e}")
                await self.bot.send_message(user_id, f"❌ Некорректный формат ссылки на группу: {group_link}")
                return False

            logger.info(f"Попытка публикации в: {target_id}")
            
            media_to_send = None
            if image_url:
                if os.path.exists(image_url) and os.path.isfile(image_url):
                    media_to_send = FSInputFile(image_url)
                    logger.info(f"🖼️ Используем локальный файл: {image_url}")
                elif image_url.startswith(('http://', 'https://')):
                    media_to_send = URLInputFile(image_url)
                    logger.info(f"🖼️ Используем URL: {image_url}")
                else:
                    logger.warning(f"⚠️ Файл не найден локально и не является валидным URL: {image_url}. Публикуем без медиа.")

            if media_to_send:
                try:
                    # В текущей версии aiogram нет is_video в send_photo, 
                    # и нет универсального send_media. Поэтому упрощаем до фото.
                    # Если понадобится видео, нужно будет добавить отдельную логику.
                    await self.bot.send_photo(target_id, media_to_send, caption=text, parse_mode="Markdown")
                except Exception as media_error:
                    logger.error(f"❌ Ошибка отправки медиафайла, пробуем отправить только текст. Ошибка: {media_error}")
                    await self.bot.send_message(target_id, text, parse_mode="Markdown")
            else:
                logger.info("📝 Публикуем без медиафайла")
                await self.bot.send_message(target_id, text, parse_mode="Markdown")
            
            logger.info(f"✅ Пост успешно опубликован в группе {group_link}")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка публикации в {group_link}: {e}")
            await self.bot.send_message(user_id, f"❌ Не удалось опубликовать пост в группе {group_link}. Проверьте, что бот добавлен в администраторы с правами на публикацию.")
            return False

    def get_media_file(self, media_path: str):
        """
        Проверяет существование медиафайла и возвращает его.
        Если файл не найден, возвращает None.
        """
        if media_path and os.path.exists(media_path):
            return FSInputFile(media_path)
        return None

    async def process_pending_posts_cycle(self):
        """Бесконечный цикл обработки ожидающих постов"""
        while self.is_running:
            try:
                await self.process_pending_posts()
                await asyncio.sleep(10)  # Проверяем каждые 10 секунд
            except Exception as e:
                logger.error(f"❌ Ошибка в цикле обработки ожидающих постов: {e}")
                await asyncio.sleep(60)

    async def process_pending_posts(self):
        """Обрабатывает ожидающие посты из очереди (статус 'approved')"""
        try:
            # Получаем только одобренные посты
            pending_posts = self.db.get_pending_autopost_queue(status_filter='approved')
            
            if not pending_posts:
                return  # Нет постов для публикации

            logger.info(f"📬 Найдено {len(pending_posts)} одобренных постов для публикации.")
            
            for post in pending_posts:
                post_id = post['id']
                group_link = post['group_link']
                
                # Проверяем, не обрабатывается ли уже этот пост
                if post_id in self.processing_posts:
                    logger.info(f"⏳ Пост ID {post_id} уже в процессе обработки, пропускаем.")
                    continue
                
                # Помечаем пост как "в процессе публикации", чтобы избежать дублей
                self.processing_posts.add(post_id)
                self.db.update_queue_status(post_id, 'publishing')
                logger.info(f"🚀 Начинаем публикацию поста ID {post_id} в группу {group_link}")
                
                try:
                    # Публикуем пост
                    published = await self.publish_post(group_link, post)
                    
                    if published:
                        # Обновляем статус в очереди и добавляем в опубликованные
                        self.db.update_queue_status(post_id, 'published')
                        self.db.add_published_post(group_link, post.get('original_post_url', 'N/A'), post['post_text'])
                        
                        # Обновляем время следующего поста
                        self.db.update_next_post_time(group_link)
                        logger.info(f"✅ Пост ID {post_id} успешно опубликован в {group_link}.")
                    else:
                        # Если публикация не удалась
                        self.db.update_queue_status(post_id, 'failed')
                        logger.error(f"❌ Не удалось опубликовать пост ID {post_id} в {group_link}.")
                        
                except Exception as e:
                    self.db.update_queue_status(post_id, 'failed')
                    logger.error(f"❌ Критическая ошибка при публикации поста ID {post_id}: {e}")
                finally:
                    # Убираем пост из множества обрабатываемых
                    self.processing_posts.remove(post_id)
                    
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в process_pending_posts: {e}")

    async def approve_post(self, user_id: int, group_link: str):
        """Одобряет пост в очереди и инициирует немедленную публикацию"""
        try:
            success = self.db.approve_autopost_in_queue(user_id, group_link)
            if success:
                logger.info(f"✅ Пост одобрен пользователем {user_id} для группы {group_link}")
                # Сразу после одобрения пытаемся опубликовать
                await self.process_pending_posts()
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка одобрения поста: {e}")
            return False

    async def cancel_post(self, user_id: int, group_link: str):
        """Отмена поста пользователем"""
        try:
            success = self.db.cancel_autopost_in_queue(user_id, group_link)
            if success:
                logger.info(f"❌ Пост отменен пользователем {user_id} для группы {group_link}")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка отмены поста: {e}")
            return False

    async def edit_post(self, user_id: int, group_link: str, new_text: str):
        """Редактирование поста пользователем"""
        try:
            success = self.db.update_autopost_in_queue(user_id, group_link, new_text)
            if success:
                logger.info(f"✏️ Пост отредактирован пользователем {user_id} для группы {group_link}")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка редактирования поста: {e}")
            return False

    async def publish_post(self, group_link: str, post: Dict) -> bool:
        """Публикует пост в группу"""
        try:
            # Получаем текст и фото
            text = post.get('post_text', '')  # Используем post_text из очереди
            photo_url = post.get('post_image')  # Используем post_image из очереди
            post_link = post.get('original_post_url')  # Используем original_post_url из очереди
            
            # Получаем telegram_id из group_link
            try:
                # Если ссылка вида https://t.me/channel_name
                if 't.me/' in group_link:
                    target_id = f"@{group_link.split('t.me/')[1]}"
                # Если ссылка вида @channel_name
                elif group_link.startswith('@'):
                    target_id = group_link
                # Если числовой ID
                else:
                    target_id = int(group_link)
            except Exception as e:
                logger.error(f"Некорректный формат group_link: {group_link} - {e}")
                return False

            logger.info(f"Попытка публикации в: {target_id}")
            
            media_to_send = None
            if photo_url:
                if os.path.exists(photo_url) and os.path.isfile(photo_url):
                    media_to_send = FSInputFile(photo_url)
                    logger.info(f"🖼️ Используем локальный файл: {photo_url}")
                elif photo_url.startswith(('http://', 'https://')):
                    media_to_send = URLInputFile(photo_url)
                    logger.info(f"🖼️ Используем URL: {photo_url}")
                else:
                    logger.warning(f"⚠️ Файл не найден локально и не является валидным URL: {photo_url}. Публикуем без медиа.")

            if media_to_send:
                try:
                    # В текущей версии aiogram нет is_video в send_photo, 
                    # и нет универсального send_media. Поэтому упрощаем до фото.
                    # Если понадобится видео, нужно будет добавить отдельную логику.
                    await self.bot.send_photo(target_id, media_to_send, caption=text, parse_mode="Markdown")
                except Exception as media_error:
                    logger.error(f"❌ Ошибка отправки медиафайла, пробуем отправить только текст. Ошибка: {media_error}")
                    await self.bot.send_message(target_id, text, parse_mode="Markdown")
            else:
                logger.info("📝 Публикуем без медиафайла")
                await self.bot.send_message(target_id, text, parse_mode="Markdown")
            
            logger.info(f"✅ Пост успешно опубликован в группе {group_link}")
            # Добавляем запись о публикации
            self.db.add_published_post(group_link, post_link, text)
            # Помечаем пост как использованный
            if post_link and post_link != 'N/A':
                self.db.mark_post_as_used(post_link)
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при публикации поста: {e}")
            return False

    def get_last_post_link(self, group_link: str, text: str) -> str:
        """Получает ссылку на последний пост для указанной группы и текста."""
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                query = f"""
                    SELECT post_link FROM {self.db.schema}.posts
                    WHERE group_link = %s AND text = %s
                    ORDER BY date DESC
                    LIMIT 1
                """
                cur.execute(query, (group_link, text))
                result = cur.fetchone()
                return result[0] if result else None

    def process_autopost(self, user_id: int, group_link: str, settings: dict) -> bool:
        """Обрабатывает автопостинг для конкретной группы"""
        try:
            # Получаем посты с похожими темами
            posts = self.db.get_similar_theme_posts(user_id, group_link)
            
            if not posts:
                logger.info(f"Нет подходящих постов для {group_link}")
                return False
            
            # Проверяем заблокированные темы
            blocked_topics = settings.get('blocked_topics', '').strip()
            if blocked_topics:
                filtered_posts = []
                for post in posts:
                    if not self.db.check_content_blocked(post['text'], blocked_topics):
                        filtered_posts.append(post)
                posts = filtered_posts
                
                if not posts:
                    logger.info(f"Все посты содержат заблокированные темы для {group_link}")
                    return False
            
            # Выбираем случайный пост
            post = random.choice(posts)
            
            # Получаем роль для автопостинга
            role = settings.get('autopost_role')
            if not role:
                role = self.db.get_gpt_role(user_id)
            
            # Переписываем текст
            rewriter = Rewriter()
            new_text = rewriter.rewrite_text(post['text'], role)
            
            if not new_text:
                logger.error(f"Не удалось переписать текст для {group_link}")
                return False
            
            # Публикуем пост
            if settings['mode'] == 'automatic':
                # Автоматический режим - публикуем сразу
                success = self.publish_post(user_id, group_link, new_text, post['media_files'])
                if success:
                    self.db.mark_post_as_used(post['id'], group_link)
                    return True
            else:
                # Контролируемый режим - отправляем на проверку
                self.send_post_for_approval(user_id, group_link, new_text, post['media_files'], post['id'])
                return True
            
        except Exception as e:
            logger.error(f"Ошибка при обработке автопостинга для {group_link}: {e}")
            return False


