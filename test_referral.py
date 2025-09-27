#!/usr/bin/env python3
"""
Тестовый файл для проверки реферальных ссылок
"""

import asyncio
import sys
import os

# Добавляем текущую директорию в путь для импорта
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from async_database import AsyncDatabase

async def test_referral_codes():
    """Тестирование реферальных кодов"""
    print("🧪 Тестирование реферальных кодов...")

    # Инициализируем базу данных
    db = AsyncDatabase()

    try:
        await db.initialize()
        print("✅ База данных инициализирована")

        # Создаем тестового пользователя
        test_telegram_id = 123456789
        test_username = "test_user"

        # Создаем пользователя в БД
        await db.create_user(test_telegram_id, test_username)
        print(f"✅ Тестовый пользователь создан: {test_telegram_id}")

        # Создаем реферальный код
        short_code = await db.create_referral_code(test_telegram_id)
        print(f"✅ Реферальный код создан: {short_code}")

        # Проверяем, что код сохранился
        saved_telegram_id = await db.get_telegram_id_by_referral_code(short_code)
        print(f"🔍 Поиск по коду {short_code}: {saved_telegram_id}")

        if saved_telegram_id == test_telegram_id:
            print("✅ Реферальный код работает корректно!")
        else:
            print("❌ Проблема с реферальным кодом!")

        # Проверяем все коды в БД
        all_codes = await asyncio.to_thread(db._execute_query,
            "SELECT telegram_id, short_code FROM referral_codes", fetchall=True)
        print(f"📋 Все коды в БД: {all_codes}")

    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(test_referral_codes())