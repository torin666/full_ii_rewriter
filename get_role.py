import sys
from database.DatabaseManager import DatabaseManager

def get_role_text(user_id):
    try:
        db = DatabaseManager()
        role_text = db.get_gpt_roles(user_id)
        print(f"\nРоль пользователя {user_id}:")
        print("-" * 50)
        print(role_text)
        print("-" * 50)
    except Exception as e:
        print(f"Ошибка при получении роли: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Использование: python get_role.py <user_id>")
        sys.exit(1)
    
    try:
        user_id = int(sys.argv[1])
        get_role_text(user_id)
    except ValueError:
        print("Ошибка: user_id должен быть числом")
        sys.exit(1) 