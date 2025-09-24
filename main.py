from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from config import TELEGRAM_TOKEN
import bot
import asyncio
import threading
import sys
import signal
import os
import psutil
import time

# Глобальные переменные
bot_instance = None
dp = None
storage = None
bot_process_id = None

def check_existing_bot_instances():
    """Проверка на уже запущенные экземпляры бота"""
    current_pid = os.getpid()
    script_name = os.path.basename(__file__)

    print(f"🔍 Проверка запущенных экземпляров бота (PID: {current_pid})...")

    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['pid'] != current_pid:
                cmdline = proc.info.get('cmdline', [])
                if cmdline and script_name in ' '.join(cmdline):
                    print(f"⚠️ Найден другой экземпляр бота (PID: {proc.info['pid']})")
                    return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    print("✅ Других экземпляров бота не найдено")
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

def console_input(loop):
    """Функция для обработки ввода в консоли"""
    while True:
        try:
            command = input().strip().lower()
            if command == 'stop':
                print("Остановка бота...")
                loop.call_soon_threadsafe(loop.stop)
                break
            elif command == 'restart':
                print("Перезапуск бота...")
                loop.call_soon_threadsafe(loop.stop)
                break
        except:
            pass

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

    # Запускаем поток для чтения команд из консоли
    console_thread = threading.Thread(target=console_input, args=(loop,), daemon=True)
    console_thread.start()

    try:
        # Запускаем бота без пропуска обновлений для более быстрой реакции
        await dp.start_polling(bot_instance, skip_updates=False)
    except KeyboardInterrupt:
        print("\nБот остановлен пользователем!")
    except Exception as e:
        print(f"Ошибка: {e}")

async def main():
    """Основная функция с возможностью перезапуска"""
    while True:
        await run_bot()
        
        # Спрашиваем, хочет ли пользователь перезапустить
        print("\nБот остановлен. Выберите действие:")
        print("1. Перезапустить (введите 'r')")
        print("2. Выйти (введите 'q')")
        print("3. Или просто введите 'r' или 'q':")
        
        try:
            choice = input().strip().lower()
            if choice in ['q', 'quit', 'exit', '2']:
                print("До свидания!")
                break
            elif choice in ['r', 'restart', '1', '']:
                print("Перезапуск...")
                continue
            else:
                print("Перезапуск...")
                continue
        except (EOFError, KeyboardInterrupt):
            print("\nДо свидания!")
            break

if __name__ == '__main__':
    asyncio.run(main())