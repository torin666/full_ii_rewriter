import vk_api
from config.vk_config import VK_APP_ID, VK_APP_SECRET, VK_SCOPE
from database.DatabaseManager import DatabaseManager

def get_auth_url():
    """Получить URL для авторизации ВКонтакте"""
    auth_url = f"https://oauth.vk.com/authorize"
    params = {
        "client_id": VK_APP_ID,
        "redirect_uri": VK_REDIRECT_URI,
        "scope": ",".join(VK_SCOPE),
        "response_type": "code",
        "v": "5.131"
    }
    return f"{auth_url}?{'&'.join(f'{k}={v}' for k, v in params.items())}"

async def get_access_token(code: str):
    """Получить access token по коду авторизации"""
    import aiohttp
    
    token_url = "https://oauth.vk.com/access_token"
    params = {
        "client_id": VK_APP_ID,
        "client_secret": VK_APP_SECRET,
        "redirect_uri": VK_REDIRECT_URI,
        "code": code
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(token_url, params=params) as response:
            data = await response.json()
            return data.get("access_token")

def post_to_wall(token: str, owner_id: str, message: str):
    """Опубликовать пост на стене группы"""
    try:
        vk_session = vk_api.VkApi(token=token)
        vk = vk_session.get_api()
        
        # Если owner_id это группа, добавляем минус
        if not owner_id.startswith('-'):
            owner_id = f"-{owner_id}"
            
        response = vk.wall.post(
            owner_id=owner_id,
            message=message,
            from_group=1
        )
        return response['post_id']
    except Exception as e:
        raise Exception(f"Ошибка при публикации в ВК: {str(e)}")

def get_group_id(token: str, group_url: str):
    """Получить ID группы по её короткому имени"""
    try:
        vk_session = vk_api.VkApi(token=token)
        vk = vk_session.get_api()
        
        # Извлекаем короткое имя группы из URL
        group_name = group_url.split('/')[-1]
        
        response = vk.groups.getById(group_id=group_name)
        return response[0]['id']
    except Exception as e:
        raise Exception(f"Ошибка при получении ID группы: {str(e)}") 