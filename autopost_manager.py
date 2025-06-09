import asyncio
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List

from aiogram import Bot
from aiogram.types import URLInputFile, InlineKeyboardMarkup, InlineKeyboardButton

from database.DatabaseManager import DatabaseManager
from utils.telegram_client import TelegramClientManager
from bot.keyboards.source_keyboards import get_autopost_approval_keyboard
from ai.gpt.rewriter import rewriter

logger = logging.getLogger(__name__)

class AutopostManager:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.db = DatabaseManager()
        self.is_running = False
        self.processing_posts = set()  # Множество ID постов которые сейчас обрабатываются
        
    async def start(self):
        """Запуск автопостинга"""
        self.is_running = True
        logger.info("AutopostManager started")
        
        # Запускаем задачу обработки ожидающих постов
        pending_posts_task = asyncio.create_task(self.process_pending_posts_cycle())
        
        try:
            while self.is_running:
                try:
                    await self.process_autopost_cycle()
                    # Ждем 5 минут для тестирования (можно изменить на любое время)
                    wait_time = 300 + random.randint(-60, 60)  # 4-6 минут
                    logger.info(f"⏰ Ожидание {wait_time} секунд до следующего цикла автопостинга")
                    await asyncio.sleep(wait_time)
                except Exception as e:
                    logger.error(f"Error in autopost cycle: {e}")
                    await asyncio.sleep(60)  # Ждем 1 минуту при ошибке
        finally:
            # Останавливаем задачу обработки ожидающих постов
            pending_posts_task.cancel()
            try:
                await pending_posts_task
            except asyncio.CancelledError:
                pass
    
    async def stop(self):
        """Остановка автопостинга"""
        self.is_running = False
        logger.info("AutopostManager stopped")
    
    async def process_autopost_cycle(self):
        """Основной цикл обработки автопостинга"""
        logger.info("🔄 Начало цикла автопостинга")
        
        # Получаем все активные настройки автопостинга
        active_groups = self.db.get_active_autopost_groups()
        logger.info(f"📊 Найдено активных групп для автопостинга: {len(active_groups)}")
        
        if not active_groups:
            logger.info("ℹ️ Нет активных групп для автопостинга")
            return
        
        # Обрабатываем группы по одной с задержками между ними
        for i, group_info in enumerate(active_groups):
            try:
                logger.info(f"🎯 Обрабатываем группу {i+1}/{len(active_groups)}: {group_info['group_link']} (user: {group_info['user_id']}, mode: {group_info['mode']})")
                
                # ДОБАВЛЯЕМ ТАЙМ-АУТ для обработки каждой группы
                try:
                    await asyncio.wait_for(
                        self.process_group_autopost(
                    group_info['user_id'],
                    group_info['group_link'],
                    group_info['mode']
                        ),
                        timeout=120.0  # 2 минуты максимум на одну группу
                    )
                except asyncio.TimeoutError:
                    logger.error(f"⏰ Тайм-аут при обработке группы {group_info['group_link']} (пользователь {group_info['user_id']})")
                    continue
                
                # Добавляем небольшую задержку между обработкой групп
                # чтобы дать возможность боту обрабатывать другие сообщения
                if i < len(active_groups) - 1:  # не ждем после последней группы
                    logger.info(f"⏸️ Пауза 10 секунд перед следующей группой...")
                    await asyncio.sleep(10)
                    
            except Exception as e:
                logger.error(f"❌ Error processing autopost for group {group_info['group_link']}: {e}")
                # Продолжаем обработку остальных групп даже при ошибке
                continue
                
        logger.info("✅ Цикл автопостинга завершен")
    
    async def process_group_autopost(self, user_id: int, group_link: str, mode: str):
        """Обработка автопостинга для конкретной группы"""
        logger.info(f"🔍 Ищем подходящие посты для user {user_id}, group {group_link}")
        
        # Проверяем есть ли уже ожидающие посты для этой группы
        if self.db.has_pending_autopost(user_id, group_link):
            logger.info(f"⏸️ Пропускаем {group_link} - есть ожидающий пост")
            return
        
        # Получаем посты с похожими темами
        posts = self.db.get_similar_theme_posts(user_id, group_link)
        logger.info(f"📝 Найдено подходящих постов: {len(posts)}")
        
        if not posts:
            logger.warning(f"⚠️ No suitable posts found for user {user_id}, group {group_link}")
            return
        
        # Берем пост с наибольшим количеством лайков и комментариев
        best_post = posts[0]
        logger.info(f"⭐ Выбран лучший пост: {best_post['post_link'][:50]}... (likes: {best_post.get('likes', 0)})")
        
        try:
            logger.info(f"🤖 Начинаем генерацию поста через GPT для {group_link}")
            
            # ДОБАВЛЯЕМ ТАЙМ-АУТ для генерации поста (максимум 60 секунд)
            try:
                result = await asyncio.wait_for(
                    rewriter(
                best_post['text'],
                best_post['post_link'],
                user_id,
                best_post.get('photo_url')
                    ),
                    timeout=60.0  # 60 секунд максимум
            )
            except asyncio.TimeoutError:
                logger.error(f"⏰ Тайм-аут при генерации поста для {group_link} (пользователь {user_id})")
                return
            
            if not result or not result.get("text"):
                logger.error(f"❌ Failed to generate post for user {user_id}, group {group_link}")
                return
            
            logger.info(f"✅ Пост успешно сгенерирован, длина: {len(result['text'])} символов")
            
            # Помечаем пост как использованный
            self.db.mark_post_as_used(best_post['text'])
            
            # Используем только переписанный текст без добавления источника
            text = result['text']
            
            # Обрабатываем в зависимости от режима
            if mode == "automatic":
                logger.info(f"🚀 Автоматическая публикация в {group_link}")
                # Добавляем тайм-аут для публикации
                try:
                    await asyncio.wait_for(
                        self.publish_post_automatically(
                    user_id, group_link, text, 
                    result.get("image_url"), 
                    result.get("is_video", False)
                        ),
                        timeout=30.0  # 30 секунд на публикацию
                )
                except asyncio.TimeoutError:
                    logger.error(f"⏰ Тайм-аут при автоматической публикации в {group_link}")
            else:  # controlled
                logger.info(f"👤 Отправляем пост на одобрение для {group_link}")
                # Добавляем тайм-аут для отправки на одобрение
                try:
                    await asyncio.wait_for(
                        self.send_post_for_approval(
                    user_id, group_link, text,
                    result.get("image_url"),
                    result.get("is_video", False)
                        ),
                        timeout=15.0  # 15 секунд на отправку сообщения
                )
                except asyncio.TimeoutError:
                    logger.error(f"⏰ Тайм-аут при отправке поста на одобрение для {group_link}")
                
        except Exception as e:
            logger.error(f"❌ Error generating post for user {user_id}, group {group_link}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def publish_post_automatically(self, user_id: int, group_link: str, text: str, image_url: str = None, is_video: bool = False):
        """Автоматическая публикация поста"""
        try:
            # Извлекаем username из group_link для TelegramClientManager
            if group_link.startswith('https://t.me/'):
                username = group_link.replace('https://t.me/', '')
            elif group_link.startswith('t.me/'):
                username = group_link.replace('t.me/', '')
            elif group_link.startswith('@'):
                username = group_link[1:]
            else:
                username = group_link
            
            # ДОБАВЛЯЕМ ТАЙМ-АУТ для публикации в группу
            success = await asyncio.wait_for(
                TelegramClientManager.send_to_group(
                    username, text, image_url, is_video
                ),
                timeout=30.0  # 30 секунд на публикацию
            )
            
            if success:
                logger.info(f"Auto-posted to {group_link} for user {user_id}")
                # Отправляем уведомление пользователю с тайм-аутом
                try:
                    await asyncio.wait_for(
                        self.bot.send_message(
                    user_id,
                    f"✅ Автопост опубликован в {group_link}\n\n{text[:200]}{'...' if len(text) > 200 else ''}"
                        ),
                        timeout=5.0
                )
                except asyncio.TimeoutError:
                    logger.error(f"⏰ Тайм-аут при отправке уведомления пользователю {user_id} об успешной публикации")
            else:
                logger.error(f"Failed to auto-post to {group_link} for user {user_id}")
                try:
                    await asyncio.wait_for(
                        self.bot.send_message(
                    user_id,
                    f"❌ Не удалось опубликовать автопост в {group_link}"
                        ),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    logger.error(f"⏰ Тайм-аут при отправке уведомления пользователю {user_id} об ошибке публикации")
                
        except asyncio.TimeoutError:
            logger.error(f"⏰ Тайм-аут при автоматической публикации в {group_link} для пользователя {user_id}")
            try:
                await asyncio.wait_for(
                    self.bot.send_message(
                        user_id,
                        f"❌ Тайм-аут при публикации автопоста в {group_link}"
                    ),
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                logger.error(f"⏰ Тайм-аут при отправке уведомления о тайм-ауте пользователю {user_id}")
        except Exception as e:
            logger.error(f"Error auto-posting to {group_link}: {e}")
            try:
                await asyncio.wait_for(
                    self.bot.send_message(
                user_id,
                f"❌ Ошибка при публикации автопоста в {group_link}: {str(e)}"
                    ),
                    timeout=5.0
            )
            except asyncio.TimeoutError:
                logger.error(f"⏰ Тайм-аут при отправке уведомления об ошибке пользователю {user_id}")
    
    async def send_post_for_approval(self, user_id: int, group_link: str, text: str, image_url: str = None, is_video: bool = False):
        """Отправка поста на одобрение пользователю"""
        try:
            # Добавляем пост в очередь на одобрение
            scheduled_time = datetime.now() + timedelta(minutes=10)
            self.db.add_autopost_to_queue(
                user_id, group_link, text, image_url, scheduled_time, is_video
            )
            
            # Отправляем пост пользователю для одобрения
            message_text = f"Готов пост для {group_link}\n\n{text}"
            
            # Кодируем group_link для callback_data (убираем спецсимволы)
            import base64
            encoded_group_link = base64.b64encode(group_link.encode()).decode()[:60]  # Ограничиваем длину
            
            if image_url:
                try:
                    if is_video:
                        video_file = URLInputFile(image_url)
                        # ДОБАВЛЯЕМ ТАЙМ-АУТ для отправки видео
                        await asyncio.wait_for(
                            self.bot.send_video(
                            user_id,
                            video_file,
                            caption=message_text,
                            reply_markup=get_autopost_approval_keyboard(encoded_group_link)
                            ),
                            timeout=10.0
                        )
                    else:
                        photo_file = URLInputFile(image_url)
                        # ДОБАВЛЯЕМ ТАЙМ-АУТ для отправки фото
                        await asyncio.wait_for(
                            self.bot.send_photo(
                            user_id,
                            photo_file,
                            caption=message_text,
                            reply_markup=get_autopost_approval_keyboard(encoded_group_link)
                            ),
                            timeout=10.0
                        )
                    
                    # Если текст не поместился в caption, aiogram обработает это автоматически
                        
                except (Exception, asyncio.TimeoutError) as e:
                    logger.error(f"Error sending media for approval: {e}")
                    # Если не удалось отправить медиа, отправляем только текст
                    await asyncio.wait_for(
                        self.bot.send_message(
                        user_id,
                        message_text,
                        reply_markup=get_autopost_approval_keyboard(encoded_group_link)
                        ),
                        timeout=5.0
                    )
            else:
                # ДОБАВЛЯЕМ ТАЙМ-АУТ для отправки текстового сообщения
                await asyncio.wait_for(
                    self.bot.send_message(
                    user_id,
                    message_text,
                    reply_markup=get_autopost_approval_keyboard(encoded_group_link)
                    ),
                    timeout=5.0
                )
            
            logger.info(f"Sent post for approval to user {user_id} for group {group_link}")
            
        except asyncio.TimeoutError:
            logger.error(f"⏰ Тайм-аут при отправке поста на одобрение пользователю {user_id}")
        except Exception as e:
            logger.error(f"Error sending post for approval: {e}")
    
    async def process_pending_posts(self):
        """Обработка постов, ожидающих публикации"""
        # Получаем посты, которые готовы к публикации
        pending_posts = self.db.get_pending_autopost_queue()
        
        for post in pending_posts:
            try:
                # Проверяем что пост не обрабатывается уже
                if post['id'] in self.processing_posts:
                    logger.info(f"⏸ Пост {post['id']} уже обрабатывается, пропускаем")
                    continue
                
                if post['status'] == 'approved':
                    # Помечаем пост как обрабатываемый
                    self.processing_posts.add(post['id'])
                    logger.info(f"📤 Публикация одобренного поста ID:{post['id']} для user {post['user_id']}, group {post['group_link']}")
                    
                    try:
                        # Сначала пытаемся изменить статус на 'publishing' для защиты от дублирования
                        self.db.update_queue_status(post['id'], 'publishing')
                        
                        # Извлекаем username из group_link для TelegramClientManager
                        group_link = post['group_link']
                        if group_link.startswith('https://t.me/'):
                            username = group_link.replace('https://t.me/', '')
                        elif group_link.startswith('t.me/'):
                            username = group_link.replace('t.me/', '')
                        elif group_link.startswith('@'):
                            username = group_link[1:]
                        else:
                            username = group_link
                        
                        # Публикуем одобренный пост
                        success = await TelegramClientManager.send_to_group(
                            username,
                            post['post_text'],
                            post.get('post_image'),
                            post.get('is_video', False)
                        )
                        
                        if success:
                            self.db.update_queue_status(post['id'], 'published')
                            logger.info(f"✅ Одобренный пост ID:{post['id']} опубликован в {post['group_link']}")
                            await self.bot.send_message(
                                post['user_id'],
                                f"✅ Пост опубликован в {post['group_link']}"
                            )
                        else:
                            self.db.update_queue_status(post['id'], 'failed')
                            logger.error(f"❌ Не удалось опубликовать одобренный пост ID:{post['id']} в {post['group_link']}")
                            await self.bot.send_message(
                                post['user_id'],
                                f"❌ Не удалось опубликовать пост в {post['group_link']}"
                            )
                    finally:
                        # Убираем пост из обрабатываемых
                        self.processing_posts.discard(post['id'])
                        
                elif post['status'] == 'pending' and datetime.now() >= post['scheduled_time']:
                    # Помечаем пост как обрабатываемый
                    self.processing_posts.add(post['id'])
                    logger.info(f"⏰ Автоматическая публикация просроченного поста для user {post['user_id']}, group {post['group_link']}")
                    
                    try:
                        # Извлекаем username из group_link для TelegramClientManager
                        group_link = post['group_link']
                        if group_link.startswith('https://t.me/'):
                            username = group_link.replace('https://t.me/', '')
                        elif group_link.startswith('t.me/'):
                            username = group_link.replace('t.me/', '')
                        elif group_link.startswith('@'):
                            username = group_link[1:]
                        else:
                            username = group_link
                        
                        # Автоматически публикуем просроченный пост
                        success = await TelegramClientManager.send_to_group(
                            username,
                            post['post_text'],
                            post.get('post_image'),
                            post.get('is_video', False)
                        )
                        
                        if success:
                            self.db.update_queue_status(post['id'], 'published')
                            logger.info(f"✅ Просроченный пост автоматически опубликован в {post['group_link']}")
                            await self.bot.send_message(
                                post['user_id'],
                                f"✅ Пост автоматически опубликован в {post['group_link']} (время ожидания истекло)"
                            )
                        else:
                            self.db.update_queue_status(post['id'], 'failed')
                            logger.error(f"❌ Не удалось автоматически опубликовать просроченный пост в {post['group_link']}")
                    finally:
                        # Убираем пост из обрабатываемых
                        self.processing_posts.discard(post['id'])
                        
            except Exception as e:
                logger.error(f"Error processing pending post {post['id']}: {e}")
                self.db.update_queue_status(post['id'], 'failed')
                # Убираем пост из обрабатываемых при ошибке
                self.processing_posts.discard(post['id'])
    
    async def process_pending_posts_cycle(self):
        """Цикл обработки ожидающих постов"""
        while self.is_running:
            try:
                await self.process_pending_posts()
                # Проверяем ожидающие посты каждые 2 минуты
                await asyncio.sleep(120)
            except Exception as e:
                logger.error(f"Error in pending posts cycle: {e}")
                await asyncio.sleep(60)  # Ждем минуту при ошибке 