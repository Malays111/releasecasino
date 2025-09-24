# Токены ботов
TELEGRAM_TOKEN = "8385343502:AAFxLxYG5OcrhdMlWJgIkZGZcf8zCyoTejY"
CRYPTO_BOT_TOKEN = "458530:AA2k6GJxJ6VXSa13SjZIVFziqJL4Sgg0oe3"

# API URLs
CRYPTO_BOT_API = "https://pay.crypt.bot/api"

# Кнопки пополнения
DEPOSIT_AMOUNTS = [5, 10, 15, 20]
CASINO_NAME = "VanishCasino"

# Поддерживаемые криптовалюты (API 1.5.1)
SUPPORTED_ASSETS = ["USDT", "BTC", "ETH", "LTC", "TON", "USDC", "BNB", "TRX"]

# Настройки платежей
DEFAULT_ASSET = "USDT"
DEFAULT_CURRENCY_TYPE = "crypto"
DEFAULT_FIAT = "USD"
INVOICE_EXPIRES_IN = 180  # 3 минуты в секундах

# Минимальные и максимальные суммы
MIN_DEPOSIT = 1.0
MAX_DEPOSIT = 10000.0
MIN_WITHDRAWAL = 1.0  # Минимальная сумма вывода (ограничение CryptoBot API)

# Ссылки на FAQ для игр
DUEL_FAQ_URL = "https://t.me/VanishCasino/52"  # Замените на ссылку для дуэли (кости)
DICE_FAQ_URL = "https://t.me/VanishCasino/53"  # Замените на ссылку для кубиков
BASKETBALL_FAQ_URL = "https://t.me/VanishCasino/12"  # Замените на ссылку для баскетбола
SLOTS_FAQ_URL = "https://t.me/VanishCasino/13"  # Замените на ссылку для слотов
LOTTERY_FAQ_URL = "https://t.me/VanishCasino/15"  # FAQ для лотереи
WHEEL_FAQ_URL = "https://t.me/VanishCasino/16"  # FAQ для колеса фортуны

# Ссылки на группы
GROUPS = [
    {"name": "Vanish Casino", "url": "https://t.me/VanishCasino"},
    {"name": "Выплаты", "url": "https://t.me/+TjSS6Sl3WDEzNzUy"},
    {"name": "Игры", "url": "https://t.me/+wxU6EuBO8ZA4NGFi"}
]


# Фото
BACKGROUND_IMAGE_URL = "https://www.dropbox.com/scl/fi/yq0rqk375v757xka05ov8/1-1.jpg?rlkey=pqy5wkgnq7gtem283nki132j1&st=gvnrbgw0&dl=0"

# Админы
ADMIN_IDS = [8217088275, 1076328217]  # Замените на ID админов

# Реферальная система
REFERRAL_BONUS = 0.3  # 0.3$ за каждого реферала, который пополнил баланс на сумму >= REFERRAL_MIN_DEPOSIT
REFERRAL_MIN_DEPOSIT = 2.0  # Минимальная сумма пополнения для получения реферального бонуса

# Ежедневные задания
DAILY_TASKS = [
    {"description": "Пригласи 50 друзей", "reward": 20.0, "type": "referrals", "target": 50},
    {"description": "Потрать 15$", "reward": 1.0, "type": "spent", "target": 15.0},
    {"description": "Пополни баланс на 10$", "reward": 5.0, "type": "deposited", "target": 10.0},
    {"description": "Сыграй 10 игр", "reward": 2.0, "type": "games", "target": 10},
    {"description": "Пригласи 10 друзей", "reward": 10.0, "type": "referrals", "target": 10},
    {"description": "Потрать 50$", "reward": 3.0, "type": "spent", "target": 50.0},
    {"description": "Пополни баланс на 25$", "reward": 7.0, "type": "deposited", "target": 25.0},
]

# Настройки игр по умолчанию
DEFAULT_GAME_SETTINGS = {
    # Шансы выигрыша (в процентах)
    'duel_win_chance': 7.0,
    'dice_win_chance': 5.0,
    'basketball_win_chance': 10.0,
    'slots_win_chance': 4.0,
    'lottery_win_chance': 1.0,   # Шанс выигрыша в лотерее
    'wheel_win_chance': 3.0,     # Шанс выигрыша в колесе фортуны

    # Множители выигрыша
    'duel_multiplier': 1.8,
    'dice_multiplier': 3.0,
    'basketball_multiplier': 1.5,
    'slots_multiplier': 5.0,
    'lottery_multiplier': 4.0,   # x10 для лотереи
    'wheel_multiplier': 4.0,      # x5 для колеса фортуны
}

# Экспорт новых констант
__all__ = [
    "TELEGRAM_TOKEN", "CRYPTO_BOT_TOKEN", "CRYPTO_BOT_API",
    "DEPOSIT_AMOUNTS", "CASINO_NAME", "SUPPORTED_ASSETS",
    "DEFAULT_ASSET", "DEFAULT_CURRENCY_TYPE", "DEFAULT_FIAT",
    "INVOICE_EXPIRES_IN", "MIN_DEPOSIT", "MAX_DEPOSIT", "MIN_WITHDRAWAL",
    "DUEL_FAQ_URL", "DICE_FAQ_URL", "BASKETBALL_FAQ_URL",
    "SLOTS_FAQ_URL",
    "LOTTERY_FAQ_URL", "WHEEL_FAQ_URL",
    "GROUPS", "BACKGROUND_IMAGE_URL", "ADMIN_IDS",
    "REFERRAL_BONUS", "REFERRAL_MIN_DEPOSIT", "DAILY_TASKS",
    "DEFAULT_GAME_SETTINGS"

]
