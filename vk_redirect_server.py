from flask import Flask, request
import logging
from pyngrok import ngrok
import json
import os

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='vk_redirect.log'
)
logger = logging.getLogger(__name__)

def setup_ngrok():
    """Настройка ngrok и сохранение URL"""
    try:
        # Запускаем туннель
        public_url = ngrok.connect(8000).public_url
        logger.info(f"ngrok tunnel URL: {public_url}")
        
        # Формируем callback URL
        callback_url = f"{public_url}/vk_callback"
        
        # Сохраняем URL в конфигурацию
        config_path = os.path.join(os.path.dirname(__file__), 'config', 'vk_config.py')
        with open(config_path, 'r', encoding='utf-8') as f:
            config_content = f.read()
        
        # Обновляем REDIRECT_URI в конфигурации
        new_config = config_content.replace(
            'VK_REDIRECT_URI = "http://80.74.24.141:8000/vk_callback"',
            f'VK_REDIRECT_URI = "{callback_url}"'
        )
        
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(new_config)
            
        logger.info(f"Конфигурация обновлена с новым callback URL: {callback_url}")
        return callback_url
    except Exception as e:
        logger.error(f"Ошибка при настройке ngrok: {str(e)}")
        raise

@app.route('/vk_callback')
def vk_callback():
    """Обработчик для redirect_uri от ВКонтакте"""
    try:
        # Получаем код авторизации из параметров
        code = request.args.get('code')
        if not code:
            logger.error("Код авторизации отсутствует в запросе")
            return "Ошибка: код авторизации не получен"

        # Формируем сообщение для бота
        bot_message = f"vk_auth_code={code}"
        
        # Возвращаем HTML страницу с инструкциями
        return f"""
        <html>
        <head>
            <title>Авторизация ВКонтакте</title>
            <meta charset="utf-8">
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 40px;
                    line-height: 1.6;
                }}
                .container {{
                    max-width: 600px;
                    margin: 0 auto;
                    padding: 20px;
                    border: 1px solid #ddd;
                    border-radius: 8px;
                }}
                .code {{
                    background: #f5f5f5;
                    padding: 10px;
                    border-radius: 4px;
                    font-family: monospace;
                    margin: 10px 0;
                }}
                .success {{
                    color: #28a745;
                    font-weight: bold;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>✅ Авторизация успешна!</h2>
                <p class="success">Код авторизации успешно получен.</p>
                <p>Пожалуйста, скопируйте следующий текст и отправьте его боту:</p>
                <div class="code">{bot_message}</div>
                <p>После отправки кода боту, вы сможете публиковать посты в ваш паблик ВКонтакте.</p>
            </div>
        </body>
        </html>
        """
    except Exception as e:
        logger.error(f"Ошибка при обработке callback: {str(e)}")
        return "Произошла ошибка при обработке запроса"

if __name__ == '__main__':
    try:
        # Настраиваем ngrok и получаем URL
        callback_url = setup_ngrok()
        logger.info(f"Сервер запущен, callback URL: {callback_url}")
        
        # Запускаем сервер
        app.run(host='0.0.0.0', port=8000)
    except Exception as e:
        logger.error(f"Ошибка при запуске сервера: {str(e)}") 