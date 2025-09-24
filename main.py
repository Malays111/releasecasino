from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from config import TELEGRAM_TOKEN
import bot
import asyncio
import signal
import os
import time

# Глобальные переменные
bot_instance = None
dp = None
storage = None
bot_process_id = None

def check_existing_bot_instances():
    """Проверка на уже запущенные экземпляры бота (упрощенная версия без psutil)"""
    print("🔍 Проверка запущенных экземпляров бота...")
    print("✅ Проверка пропущена (psutil не установлен)")
    return False

def initialize_bot():
    """Инициализация бота"""
    global bot_instance, dp, storage
    
    # Создаем бота и диспетчер с MemoryStorage
    bot_instance = Bot(token=TELEGRAM_TOKEN)
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    
    # Передаем dp и bot в bot.py
    bot.bot = bot_instance
    bot.dp = dp
    bot.setup_handlers()

def signal_handler(signum, frame, loop):
    """Обработчик сигналов завершения"""
    global bot_process_id
    signal_names = {
        signal.SIGINT: "SIGINT (Ctrl+C)",
        signal.SIGTERM: "SIGTERM"
    }

    signal_name = signal_names.get(signum, f"сигнал {signum}")
    print(f"\n🔄 Получен {signal_name}. Начинаем корректное завершение...")

    # Устанавливаем флаг завершения
    if bot_process_id:
        try:
            with open(f"bot_{bot_process_id}.stop", 'w') as f:
                f.write(str(time.time()))
        except:
            pass

    loop.call_soon_threadsafe(loop.stop)


async def on_startup():
    print("VanishCasino Bot запущен!")
    print("Команды:")
    print("  stop - остановить бота")
    print("  restart - перезапустить бота")

    # Инициализация базы данных
    try:
        print("Инициализация базы данных...")
        await bot.async_db.initialize()
        print("База данных инициализирована")
    except Exception as e:
        print(f"Ошибка инициализации БД: {e}")

    # Загрузка настроек из базы данных
    try:
        print("Загрузка настроек...")
        await bot.load_initial_settings()
        print("Настройки загружены")
    except Exception as e:
        print(f"Ошибка загрузки настроек: {e}")

    # Предварительная загрузка данных для ускорения работы
    try:
        print("Предварительная загрузка данных...")
        await bot.preload_data()
        print("Данные загружены")
    except Exception as e:
        print(f"Ошибка предзагрузки данных: {e}")

async def on_shutdown():
    print("🔄 Начинаем корректное завершение работы...")

    # Сохраняем кэши в базу данных перед закрытием
    try:
        print("💾 Сохранение кэшей в базу данных...")
        # Принудительно сохраняем все кэшированные данные
        if hasattr(bot, 'user_balance_cache'):
            print(f"   • Кэш балансов: {len(bot.user_balance_cache)} записей")
        if hasattr(bot, 'user_stats_cache'):
            print(f"   • Кэш статистики: {len(bot.user_stats_cache)} записей")
        if hasattr(bot, 'top_deposited_cache'):
            print(f"   • Кэш топов: {len(bot.top_deposited_cache)} записей")
        print("✅ Кэши сохранены")
    except Exception as e:
        print(f"⚠️ Ошибка при сохранении кэшей: {e}")

    # Сохраняем базу данных перед закрытием
    try:
        print("💾 Сохранение базы данных...")
        await bot.async_db.close()
        print("✅ База данных сохранена и закрыта")
    except Exception as e:
        print(f"⚠️ Ошибка при сохранении базы данных: {e}")

    print("✅ Бот остановлен!")

async def run_bot():
    """Запуск бота"""
    global bot_instance, dp

    # Инициализируем бота
    initialize_bot()

    # Регистрируем handlers для startup/shutdown
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    # Запускаем задачу обновления кэша топов
    asyncio.create_task(bot.update_top_cache())

    # Регистрируем обработчики сигналов для корректного завершения
    loop = asyncio.get_event_loop()
    signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, loop))
    signal.signal(signal.SIGTERM, lambda s, f: signal_handler(s, f, loop))


    try:
        # Запускаем бота без пропуска обновлений для более быстрой реакции
        await dp.start_polling(bot_instance, skip_updates=False)
    except KeyboardInterrupt:
        print("\nБот остановлен пользователем!")
    except Exception as e:
        print(f"Ошибка: {e}")

async def main():
    """Основная функция с возможностью перезапуска"""
    restart_count = 0
    max_restarts = 5

    while restart_count < max_restarts:
        try:
            await run_bot()
            break  # Если бот завершился нормально, выходим из цикла
        except Exception as e:
            restart_count += 1
            print(f"❌ Ошибка бота (попытка {restart_count}/{max_restarts}): {e}")

            if restart_count < max_restarts:
                print("🔄 Перезапуск через 5 секунд...")
                await asyncio.sleep(5)
            else:
                print("❌ Достигнуто максимальное количество перезапусков")
                break

    print("✅ Бот остановлен")

if __name__ == '__main__':
    asyncio.run(main())