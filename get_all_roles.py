import sys
from database.DatabaseManager import DatabaseManager

def get_all_roles(user_id):
    try:
        db = DatabaseManager()
        
        # Получаем все группы и их роли
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT group_link, autopost_role 
                    FROM {db.schema}.autopost_settings 
                    WHERE user_id = %s
                    ORDER BY group_link
                    """,
                    (user_id,)
                )
                results = cur.fetchall()
                
                print(f"\nРоли пользователя {user_id}:")
                print("-" * 50)
                
                if not results:
                    print("У пользователя нет настроенных групп")
                else:
                    for group_link, role in results:
                        print(f"\nГруппа: {group_link}")
                        print("Роль:", role if role else "Не установлена")
                        print("-" * 30)
                
    except Exception as e:
        print(f"Ошибка при получении ролей: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Использование: python get_all_roles.py <user_id>")
        sys.exit(1)
    
    try:
        user_id = int(sys.argv[1])
        get_all_roles(user_id)
    except ValueError:
        print("Ошибка: user_id должен быть числом")
        sys.exit(1) 