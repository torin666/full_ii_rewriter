from datetime import datetime
import logging


def get_vk_posts(vk_session, owner_id, start_time, end_time):
    logger = logging.getLogger(__name__)
    posts = []
    try:
        def date_str_to_timestamp(date_str, end_of_day=False):
            dt = datetime.strptime(date_str, "%d.%m.%Y")
            if end_of_day:
                dt = dt.replace(hour=23, minute=59, second=59)
            return int(dt.timestamp())

        start_ts = date_str_to_timestamp(start_time)
        end_ts = date_str_to_timestamp(end_time, end_of_day=True)

        logger.info(f"Начинаю парсинг ВК для owner_id={owner_id}, период: {start_time} - {end_time}")

        if owner_id > 0:
            response = vk_session.method('wall.get', {
                'owner_id': owner_id,
                'count': 100,
                'filter': 'owner'
            })
        else:
            response = vk_session.method('wall.get', {
                'owner_id': owner_id,
                'count': 100
            })

        logger.info(f"VK API вернул {len(response['items'])} постов")
        for post in response['items']:
            logger.info(f"Пост VK: id={post['id']}, дата={datetime.fromtimestamp(post['date'])}")
        
        filtered_count = 0
        for post in response['items']:
            if start_ts <= post['date'] <= end_ts:
                filtered_count += 1
                post_date = datetime.fromtimestamp(post['date']).strftime("%d.%m.%Y")

                # Ищем первую фотографию среди всех вложений
                photo_url = None
                if 'attachments' in post and post['attachments']:
                    for att in post['attachments']:
                        if att['type'] == 'photo':
                            sizes = att['photo']['sizes']
                            # Берем самое большое фото
                            max_area = 0
                            for s in sizes:
                                area = s['width'] * s['height']
                                if area > max_area:
                                    max_area = area
                                    photo_url = s['url']
                            break  # Берем только первую фотографию

                posts.append({
                    'post_link': f"https://vk.com/wall{owner_id}_{post['id']}",
                    'text': post['text'],
                    'date': post_date,
                    'likes': post.get('likes', {}).get('count', 0),
                    'views': post.get('views', {}).get('count', 0),
                    'photo_url': photo_url
                })
                logger.info(f"Добавлен пост: id={post['id']}, дата={post_date}, фото: {'есть' if photo_url else 'нет'}")
        logger.info(f"В фильтр по дате попало {filtered_count} постов")

    except Exception as e:
        logger.error(f"VK Posts Error: {e}")

    return posts