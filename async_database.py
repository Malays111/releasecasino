import sqlite3
import asyncio
from typing import Optional, Tuple, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

async def init_db():
    """Инициализация базы данных"""
    db = AsyncDatabase()
    await db.initialize()

class AsyncDatabase:
    def __init__(self, db_name="casino.db"):
        self.db_name = db_name
        self.executor = ThreadPoolExecutor(max_workers=4)
        self._initialized = False

    async def initialize(self):
        """Асинхронная инициализация базы данных"""
        if not self._initialized:
            await self.init_db()
            await self.enable_wal_mode()
            self._initialized = True

    async def init_db(self):
        # Таблица пользователей
        await asyncio.to_thread(self._execute_query, '''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                telegram_id INTEGER UNIQUE,
                username TEXT,
                balance REAL DEFAULT 0,
                referral_count INTEGER DEFAULT 0,
                referral_balance REAL DEFAULT 0,
                total_deposited REAL DEFAULT 0,
                total_spent REAL DEFAULT 0,
                games_played INTEGER DEFAULT 0,
                last_daily_task_completed DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''', commit=True)

        # Добавляем колонки, если их нет
        try:
            await asyncio.to_thread(self._execute_query, "ALTER TABLE users ADD COLUMN referrer_id INTEGER", commit=True)
        except sqlite3.OperationalError:
            pass

        try:
            await asyncio.to_thread(self._execute_query, "ALTER TABLE users ADD COLUMN referral_balance REAL DEFAULT 0", commit=True)
        except sqlite3.OperationalError:
            pass

        try:
            await asyncio.to_thread(self._execute_query, "ALTER TABLE users ADD COLUMN referral_bonus_given INTEGER DEFAULT 0", commit=True)
        except sqlite3.OperationalError:
            pass

        try:
            await asyncio.to_thread(self._execute_query, "ALTER TABLE users ADD COLUMN total_deposited REAL DEFAULT 0", commit=True)
        except sqlite3.OperationalError:
            pass

        try:
            await asyncio.to_thread(self._execute_query, "ALTER TABLE users ADD COLUMN total_spent REAL DEFAULT 0", commit=True)
        except sqlite3.OperationalError:
            pass

        try:
            await asyncio.to_thread(self._execute_query, "ALTER TABLE users ADD COLUMN games_played INTEGER DEFAULT 0", commit=True)
        except sqlite3.OperationalError:
            pass

        try:
            await asyncio.to_thread(self._execute_query, "ALTER TABLE users ADD COLUMN last_daily_task_completed DATE", commit=True)
        except sqlite3.OperationalError:
            pass

        try:
            await asyncio.to_thread(self._execute_query, "ALTER TABLE users ADD COLUMN active_referrals_count INTEGER DEFAULT 0", commit=True)
        except sqlite3.OperationalError:
            pass

        try:
            await asyncio.to_thread(self._execute_query, "ALTER TABLE users ADD COLUMN referral_notification_sent INTEGER DEFAULT 0", commit=True)
        except sqlite3.OperationalError:
            pass

        # Таблица платежей
        await asyncio.to_thread(self._execute_query, '''
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                amount REAL,
                crypto_bot_invoice_id TEXT,
                status TEXT DEFAULT 'pending',
                message_id INTEGER,
                chat_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''', commit=True)

        # Добавляем недостающие колонки в таблицу payments
        try:
            await asyncio.to_thread(self._execute_query, "ALTER TABLE payments ADD COLUMN status TEXT DEFAULT 'pending'", commit=True)
        except sqlite3.OperationalError:
            pass  # Колонка уже существует

        try:
            await asyncio.to_thread(self._execute_query, "ALTER TABLE payments ADD COLUMN message_id INTEGER", commit=True)
        except sqlite3.OperationalError:
            pass  # Колонка уже существует

        try:
            await asyncio.to_thread(self._execute_query, "ALTER TABLE payments ADD COLUMN chat_id INTEGER", commit=True)
        except sqlite3.OperationalError:
            pass  # Колонка уже существует

        # Таблица выводов
        await asyncio.to_thread(self._execute_query, '''
            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                amount REAL,
                wallet_address TEXT,
                crypto_bot_transfer_id TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''', commit=True)

        # Таблица настроек шансов
        await asyncio.to_thread(self._execute_query, '''
            CREATE TABLE IF NOT EXISTS game_settings (
                id INTEGER PRIMARY KEY,
                setting_key TEXT UNIQUE,
                setting_value REAL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''', commit=True)

        # Таблица текстовых настроек
        await asyncio.to_thread(self._execute_query, '''
            CREATE TABLE IF NOT EXISTS text_settings (
                id INTEGER PRIMARY KEY,
                setting_key TEXT UNIQUE,
                setting_value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''', commit=True)

        # Таблица промокодов
        await asyncio.to_thread(self._execute_query, '''
            CREATE TABLE IF NOT EXISTS promo_codes (
                id INTEGER PRIMARY KEY,
                code TEXT UNIQUE,
                reward_amount REAL,
                max_activations INTEGER,
                current_activations INTEGER DEFAULT 0,
                expires_at TIMESTAMP,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''', commit=True)

        # Таблица использованных промокодов
        await asyncio.to_thread(self._execute_query, '''
            CREATE TABLE IF NOT EXISTS used_promo_codes (
                id INTEGER PRIMARY KEY,
                promo_code_id INTEGER,
                user_id INTEGER,
                used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (promo_code_id) REFERENCES promo_codes (id),
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        ''', commit=True)

        # Таблица логов действий пользователей
        await asyncio.to_thread(self._execute_query, '''
            CREATE TABLE IF NOT EXISTS user_logs (
                id INTEGER PRIMARY KEY,
                telegram_id INTEGER,
                action TEXT,
                amount REAL DEFAULT 0,
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''', commit=True)

        # Таблица коротких реферальных кодов
        await asyncio.to_thread(self._execute_query, '''
            CREATE TABLE IF NOT EXISTS referral_codes (
                id INTEGER PRIMARY KEY,
                telegram_id INTEGER UNIQUE,
                short_code TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
            )
        ''', commit=True)

        # Таблица тикетов поддержки
        await asyncio.to_thread(self._execute_query, '''
            CREATE TABLE IF NOT EXISTS support_tickets (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                telegram_id INTEGER,
                username TEXT,
                issue TEXT,
                status TEXT DEFAULT 'open',
                admin_id INTEGER,
                admin_username TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP,
                admin_response TEXT,
                user_cooldown_until TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        ''', commit=True)

        # Таблица сообщений поддержки
        await asyncio.to_thread(self._execute_query, '''
            CREATE TABLE IF NOT EXISTS support_messages (
                id INTEGER PRIMARY KEY,
                ticket_id INTEGER,
                telegram_id INTEGER,
                message TEXT,
                is_admin INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticket_id) REFERENCES support_tickets (id)
            )
        ''', commit=True)

        # Таблица розыгрышей лотереи
        await asyncio.to_thread(self._execute_query, '''
            CREATE TABLE IF NOT EXISTS lottery_draws (
                id INTEGER PRIMARY KEY,
                draw_number INTEGER UNIQUE,
                winning_numbers TEXT,
                draw_date TIMESTAMP,
                status TEXT DEFAULT 'active',
                total_tickets INTEGER DEFAULT 0,
                total_prize_pool REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''', commit=True)

        # Таблица билетов лотереи
        await asyncio.to_thread(self._execute_query, '''
            CREATE TABLE IF NOT EXISTS lottery_tickets (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                telegram_id INTEGER,
                draw_number INTEGER,
                ticket_numbers TEXT,
                purchase_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_winner INTEGER DEFAULT 0,
                winnings REAL DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users (id),
                FOREIGN KEY (draw_number) REFERENCES lottery_draws (draw_number)
            )
        ''', commit=True)

        # Инициализация настроек по умолчанию
        default_settings = [
            ('duel_win_chance', 25.0),
            ('dice_win_chance', 30.0),
            ('basketball_win_chance', 10.0),
            ('slots_win_chance', 15.0),
            ('blackjack_win_chance', 40.0),
            ('duel_multiplier', 1.8),
            ('dice_multiplier', 5.0),
            ('basketball_multiplier', 1.5),
            ('slots_multiplier', 8.0),
            ('blackjack_multiplier', 2.0)
        ]

        for key, value in default_settings:
            await asyncio.to_thread(self._execute_query,
                "INSERT OR IGNORE INTO game_settings (setting_key, setting_value) VALUES (?, ?)",
                (key, value), commit=True)

    def _execute_query(self, query, params=(), fetchone=False, fetchall=False, commit=False):
        """Вспомогательная функция для выполнения SQL запросов в потоке"""
        conn = sqlite3.connect(self.db_name)
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            if commit:
                conn.commit()
                # Принудительное сохранение на диск после каждого коммита
                conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
            if fetchone:
                return cursor.fetchone()
            if fetchall:
                return cursor.fetchall()
            return cursor.lastrowid
        finally:
            conn.close()

    def _execute_many(self, query, params_list, commit=False):
        """Вспомогательная функция для выполнения множественных запросов"""
        conn = sqlite3.connect(self.db_name)
        try:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            if commit:
                conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    async def enable_wal_mode(self):
        """Включить WAL режим для лучшей concurrency"""
        try:
            await asyncio.to_thread(self._execute_query, "PRAGMA journal_mode=WAL;", commit=True)
            await asyncio.to_thread(self._execute_query, "PRAGMA synchronous=NORMAL;", commit=True)
        except Exception as e:
            print(f"Ошибка включения WAL режима: {e}")

    async def get_user(self, telegram_id: int) -> Optional[Tuple]:
        user = await asyncio.to_thread(self._execute_query,
            "SELECT id, telegram_id, username, balance, referral_count, COALESCE(referral_balance, 0) as referral_balance, COALESCE(total_deposited, 0) as total_deposited, COALESCE(total_spent, 0) as total_spent, COALESCE(games_played, 0) as games_played, referrer_id, referral_bonus_given, last_daily_task_completed, COALESCE(active_referrals_count, 0) as active_referrals_count, COALESCE(referral_notification_sent, 0) as referral_notification_sent, created_at FROM users WHERE telegram_id = ?",
            (telegram_id,), fetchone=True)

        if user:
            # Приводим balance и referral_balance к float для безопасности
            user = list(user)
            try:
                user[3] = float(str(user[3]).replace(',', '.')) if user[3] is not None else 0.0  # balance
            except (ValueError, TypeError):
                user[3] = 0.0
            try:
                user[5] = float(str(user[5]).replace(',', '.')) if user[5] is not None else 0.0  # referral_balance
            except (ValueError, TypeError):
                user[5] = 0.0
            user = tuple(user)
        return user

    async def get_user_by_username(self, username: str) -> Optional[Tuple]:
        user = await asyncio.to_thread(self._execute_query,
            "SELECT id, telegram_id, username, balance, referral_count, COALESCE(referral_balance, 0) as referral_balance, COALESCE(total_deposited, 0) as total_deposited, COALESCE(total_spent, 0) as total_spent, COALESCE(games_played, 0) as games_played, referrer_id, referral_bonus_given, last_daily_task_completed, COALESCE(active_referrals_count, 0) as active_referrals_count, COALESCE(referral_notification_sent, 0) as referral_notification_sent, created_at FROM users WHERE username = ?",
            (username,), fetchone=True)

        if user:
            # Приводим balance и referral_balance к float для безопасности
            user = list(user)
            try:
                user[3] = float(str(user[3]).replace(',', '.')) if user[3] is not None else 0.0  # balance
            except (ValueError, TypeError):
                user[3] = 0.0
            try:
                user[5] = float(str(user[5]).replace(',', '.')) if user[5] is not None else 0.0  # referral_balance
            except (ValueError, TypeError):
                user[5] = 0.0
            user = tuple(user)
        return user

    async def create_user(self, telegram_id: int, username: str, referrer_id: Optional[int] = None):
        # Проверяем, существует ли пользователь
        existing_user = await asyncio.to_thread(self._execute_query,
            "SELECT referrer_id FROM users WHERE telegram_id = ?", (telegram_id,), fetchone=True)

        if existing_user:
            # Пользователь уже существует
            current_referrer_id = existing_user[0] if existing_user[0] is not None else None
            if current_referrer_id is None and referrer_id is not None:
                # Устанавливаем referrer_id, если его нет
                await asyncio.to_thread(self._execute_query,
                    "UPDATE users SET referrer_id = ?, referral_bonus_given = 0 WHERE telegram_id = ?",
                    (referrer_id, telegram_id), commit=True)
                # Обновляем счетчик рефералов
                await asyncio.to_thread(self._execute_query,
                    "UPDATE users SET referral_count = COALESCE(referral_count, 0) + 1 WHERE telegram_id = ?",
                    (referrer_id,), commit=True)
                # Принудительное сохранение
                await self._force_save()
                await self._force_checkpoint()
                print(f"✅ Реферер установлен для пользователя {telegram_id}, данные сохранены")
        else:
            # Создаем нового пользователя
            try:
                await asyncio.to_thread(self._execute_query,
                    "INSERT INTO users (telegram_id, username, referrer_id, referral_bonus_given) VALUES (?, ?, ?, 0)",
                    (telegram_id, username, referrer_id), commit=True)

                # Если есть referrer, обновляем его счетчик рефералов
                if referrer_id:
                    await asyncio.to_thread(self._execute_query,
                        "UPDATE users SET referral_count = COALESCE(referral_count, 0) + 1 WHERE telegram_id = ?",
                        (referrer_id,), commit=True)

                # Принудительное сохранение для новых пользователей и рефералов
                await self._force_save()
                await self._force_checkpoint()
                print(f"✅ Новый пользователь {telegram_id} создан, данные сохранены")

            except sqlite3.OperationalError:
                # Если колонки нет, вставляем без них
                await asyncio.to_thread(self._execute_query,
                    "INSERT INTO users (telegram_id, username) VALUES (?, ?)",
                    (telegram_id, username), commit=True)
                # Принудительное сохранение
                await self._force_save()
                await self._force_checkpoint()
                print(f"✅ Новый пользователь {telegram_id} создан (без реферала), данные сохранены")

    async def update_balance(self, telegram_id: int, amount: float):
        try:
            amount = float(amount)
        except (ValueError, TypeError):
            return

        await asyncio.to_thread(self._execute_query,
            "UPDATE users SET balance = (COALESCE(CAST(balance AS REAL), 0) + ?) WHERE telegram_id = ?",
            (amount, telegram_id), commit=True)
        if amount > 0:
            await asyncio.to_thread(self._execute_query,
                "UPDATE users SET total_deposited = COALESCE(total_deposited, 0) + ? WHERE telegram_id = ?",
                (amount, telegram_id), commit=True)
        elif amount < 0:
            await asyncio.to_thread(self._execute_query,
                "UPDATE users SET total_spent = COALESCE(total_spent, 0) + ? WHERE telegram_id = ?",
                (-amount, telegram_id), commit=True)

        # Принудительное сохранение после каждого изменения баланса
        await self._force_save()
        await self._force_checkpoint()
        print(f"✅ Баланс пользователя {telegram_id} обновлен на {amount}, данные сохранены")

    async def update_referral_balance(self, telegram_id: int, amount: float):
        try:
            amount = float(amount)
        except (ValueError, TypeError):
            return

        await asyncio.to_thread(self._execute_query,
            "UPDATE users SET referral_balance = COALESCE(referral_balance, 0) + ? WHERE telegram_id = ?",
            (amount, telegram_id), commit=True)

        # Обновляем active_referrals_count если это пополнение от реферала
        if amount > 0:
            await asyncio.to_thread(self._execute_query,
                "UPDATE users SET active_referrals_count = COALESCE(active_referrals_count, 0) + 1 WHERE telegram_id = ?",
                (telegram_id,), commit=True)

        # Принудительное сохранение для реферальных бонусов
        await self._force_save()
        await self._force_checkpoint()
        print(f"✅ Реферальный баланс пользователя {telegram_id} обновлен на {amount}, данные сохранены")

    async def update_active_referrals_count(self, telegram_id: int, amount: float):
        """Обновление счетчика активных рефералов (тех, кто пополнил баланс на 2$+)"""
        try:
            amount = float(amount)
        except (ValueError, TypeError):
            return

        # Если сумма пополнения >= 2$, увеличиваем счетчик активных рефералов
        if amount >= 2.0:
            await asyncio.to_thread(self._execute_query,
                "UPDATE users SET active_referrals_count = COALESCE(active_referrals_count, 0) + 1 WHERE telegram_id = ?",
                (telegram_id,), commit=True)

    async def get_total_balance(self, telegram_id: int) -> float:
        user = await self.get_user(telegram_id)
        if user:
            return user[3] + user[5]  # balance + referral_balance
        return 0.0

    async def create_payment(self, user_id: int, amount: float, invoice_id: str, message_id: int = None, chat_id: int = None) -> int:
        result = await asyncio.to_thread(self._execute_query,
            "INSERT INTO payments (user_id, amount, crypto_bot_invoice_id, status, message_id, chat_id) VALUES (?, ?, ?, 'pending', ?, ?)",
            (user_id, amount, invoice_id, message_id, chat_id), commit=True)
        # Принудительное сохранение для платежей
        await self._force_save()
        return result

    async def update_payment_status(self, invoice_id: str, status: str):
        await asyncio.to_thread(self._execute_query,
            "UPDATE payments SET status = ? WHERE crypto_bot_invoice_id = ?",
            (status, invoice_id), commit=True)
        # Принудительное сохранение для обновления статуса платежа
        await self._force_save()

    async def get_top_deposited(self, limit: int = 5) -> List[Tuple[str, float]]:
        return await asyncio.to_thread(self._execute_query,
            "SELECT username, total_deposited FROM users ORDER BY total_deposited DESC LIMIT ?",
            (limit,), fetchall=True)

    async def get_top_spent(self, limit: int = 5) -> List[Tuple[str, float]]:
        return await asyncio.to_thread(self._execute_query,
            "SELECT username, total_spent FROM users ORDER BY total_spent DESC LIMIT ?",
            (limit,), fetchall=True)

    async def get_top_referrals(self, limit: int = 5) -> List[Tuple[str, int]]:
        return await asyncio.to_thread(self._execute_query,
            "SELECT username, active_referrals_count FROM users ORDER BY active_referrals_count DESC LIMIT ?",
            (limit,), fetchall=True)

    async def get_top_wins(self, limit: int = 5) -> List[Tuple[str, float]]:
        return await asyncio.to_thread(self._execute_query,
            "SELECT username, (COALESCE(total_deposited, 0) - COALESCE(total_spent, 0)) as net_profit FROM users ORDER BY net_profit DESC LIMIT ?",
            (limit,), fetchall=True)

    async def update_games_played(self, telegram_id: int):
        await asyncio.to_thread(self._execute_query,
            "UPDATE users SET games_played = COALESCE(games_played, 0) + 1 WHERE telegram_id = ?",
            (telegram_id,), commit=True)

    async def save_game_setting(self, key: str, value: float):
        await asyncio.to_thread(self._execute_query,
            "INSERT OR REPLACE INTO game_settings (setting_key, setting_value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            (key, value), commit=True)

    async def load_game_setting(self, key: str, default_value: Optional[float] = None) -> Optional[float]:
        result = await asyncio.to_thread(self._execute_query,
            "SELECT setting_value FROM game_settings WHERE setting_key = ?",
            (key,), fetchone=True)
        return result[0] if result else default_value

    async def load_all_game_settings(self) -> Dict[str, float]:
        rows = await asyncio.to_thread(self._execute_query,
            "SELECT setting_key, setting_value FROM game_settings", fetchall=True)
        return {row[0]: row[1] for row in rows}

    async def save_setting(self, key: str, value: str):
        await asyncio.to_thread(self._execute_query,
            "INSERT OR REPLACE INTO text_settings (setting_key, setting_value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            (key, str(value)), commit=True)
        # Принудительное сохранение для настроек
        await self._force_save()

    async def get_setting(self, key: str, default_value: Optional[str] = None) -> Optional[str]:
        result = await asyncio.to_thread(self._execute_query,
            "SELECT setting_value FROM text_settings WHERE setting_key = ?",
            (key,), fetchone=True)
        return result[0] if result else default_value

    async def create_withdrawal(self, user_id: int, amount: float, wallet_address: str) -> int:
        result = await asyncio.to_thread(self._execute_query,
            "INSERT INTO withdrawals (user_id, amount, wallet_address) VALUES (?, ?, ?)",
            (user_id, amount, wallet_address), commit=True)
        # Принудительное сохранение для выводов
        await self._force_save()
        return result

    async def update_withdrawal_status(self, withdrawal_id: int, status: str, transfer_id: Optional[str] = None):
        if transfer_id:
            await asyncio.to_thread(self._execute_query,
                "UPDATE withdrawals SET status = ?, crypto_bot_transfer_id = ? WHERE id = ?",
                (status, transfer_id, withdrawal_id), commit=True)
        else:
            await asyncio.to_thread(self._execute_query,
                "UPDATE withdrawals SET status = ? WHERE id = ?",
                (status, withdrawal_id), commit=True)
        # Принудительное сохранение для обновления статуса вывода
        await self._force_save()

    async def create_promo_code(self, code: str, reward_amount: float, max_activations: int, expires_at: Optional[str], created_by: int) -> int:
        return await asyncio.to_thread(self._execute_query,
            "INSERT INTO promo_codes (code, reward_amount, max_activations, expires_at, created_by) VALUES (?, ?, ?, ?, ?)",
            (code.upper(), reward_amount, max_activations, expires_at, created_by), commit=True)

    async def get_promo_code(self, code: str) -> Optional[Tuple]:
        return await asyncio.to_thread(self._execute_query,
            "SELECT id, code, reward_amount, max_activations, current_activations, expires_at, created_by, created_at FROM promo_codes WHERE code = ?",
            (code.upper(),), fetchone=True)

    async def activate_promo_code(self, promo_code_id: int, user_id: int) -> Tuple[bool, Any]:
        # Проверяем, не активировал ли уже пользователь этот промокод
        if await asyncio.to_thread(self._execute_query,
            "SELECT id FROM used_promo_codes WHERE promo_code_id = ? AND user_id = ?",
            (promo_code_id, user_id), fetchone=True):
            return False, "Вы уже активировали этот промокод"

        # Получаем информацию о промокоде
        promo = await asyncio.to_thread(self._execute_query,
            "SELECT reward_amount, max_activations, current_activations, expires_at FROM promo_codes WHERE id = ?",
            (promo_code_id,), fetchone=True)

        if not promo:
            return False, "Промокод не найден"

        reward_amount, max_activations, current_activations, expires_at = promo

        # Проверяем срок действия
        from datetime import datetime
        if expires_at and datetime.now() > datetime.fromisoformat(expires_at):
            return False, "Срок действия промокода истек"

        # Проверяем лимит активаций
        if current_activations >= max_activations:
            return False, "Лимит активаций промокода исчерпан"

        # Активируем промокод
        await asyncio.to_thread(self._execute_query,
            "INSERT INTO used_promo_codes (promo_code_id, user_id) VALUES (?, ?)",
            (promo_code_id, user_id), commit=True)
        await asyncio.to_thread(self._execute_query,
            "UPDATE promo_codes SET current_activations = current_activations + 1 WHERE id = ?",
            (promo_code_id,), commit=True)

        return True, reward_amount

    async def get_all_promo_codes(self) -> List[Tuple]:
        return await asyncio.to_thread(self._execute_query,
            "SELECT id, code, reward_amount, max_activations, current_activations, expires_at, created_by, created_at FROM promo_codes ORDER BY created_at DESC",
            fetchall=True)

    async def delete_promo_code(self, promo_id: int):
        await asyncio.to_thread(self._execute_query,
            "DELETE FROM used_promo_codes WHERE promo_code_id = ?", (promo_id,), commit=True)
        await asyncio.to_thread(self._execute_query,
            "DELETE FROM promo_codes WHERE id = ?", (promo_id,), commit=True)

    async def log_action(self, telegram_id: int, action: str, amount: float = 0, reason: str = ""):
        try:
            await asyncio.to_thread(self._execute_query,
                "INSERT INTO user_logs (telegram_id, action, amount, reason) VALUES (?, ?, ?, ?)",
                (telegram_id, action, amount, reason), commit=True)
            # Принудительное сохранение для логов
            await self._force_save()
        except Exception:
            pass  # Игнорируем ошибки логирования

    async def get_user_logs(self, telegram_id: Optional[int] = None, limit: int = 50) -> List[Tuple]:
        if telegram_id:
            return await asyncio.to_thread(self._execute_query,
                "SELECT telegram_id, action, amount, reason, created_at FROM user_logs WHERE telegram_id = ? ORDER BY created_at DESC LIMIT ?",
                (telegram_id, limit), fetchall=True)
        else:
            return await asyncio.to_thread(self._execute_query,
                "SELECT telegram_id, action, amount, reason, created_at FROM user_logs ORDER BY created_at DESC LIMIT ?",
                (limit,), fetchall=True)

    async def mark_referral_bonus_given(self, telegram_id: int):
        """Отметка реферального бонуса как начисленного"""
        await asyncio.to_thread(self._execute_query,
            "UPDATE users SET referral_bonus_given = 1 WHERE telegram_id = ?",
            (telegram_id,), commit=True)

    async def mark_referral_notification_sent(self, telegram_id: int):
        """Отметка реферального уведомления как отправленного"""
        await asyncio.to_thread(self._execute_query,
            "UPDATE users SET referral_notification_sent = 1 WHERE telegram_id = ?",
            (telegram_id,), commit=True)

    async def get_payment_by_invoice(self, invoice_id: str) -> Optional[Tuple]:
        return await asyncio.to_thread(self._execute_query,
            "SELECT user_id, amount, COALESCE(status, 'pending'), message_id, chat_id FROM payments WHERE crypto_bot_invoice_id = ?",
            (invoice_id,), fetchone=True)

    async def get_telegram_id_by_user_id(self, user_id: int) -> Optional[int]:
        result = await asyncio.to_thread(self._execute_query,
            "SELECT telegram_id FROM users WHERE id = ?",
            (user_id,), fetchone=True)
        return result[0] if result else None

    async def get_pending_payments(self, telegram_id: int) -> List[Tuple]:
        # Получаем user_id по telegram_id
        user = await self.get_user(telegram_id)
        if not user:
            return []
        user_id = user[0]
        return await asyncio.to_thread(self._execute_query,
            "SELECT crypto_bot_invoice_id FROM payments WHERE user_id = ? AND COALESCE(status, 'pending') = 'pending'",
            (user_id,), fetchall=True)

    async def get_payment_amount_by_invoice(self, invoice_id: str) -> Optional[float]:
        result = await asyncio.to_thread(self._execute_query,
            "SELECT amount FROM payments WHERE crypto_bot_invoice_id = ?",
            (invoice_id,), fetchone=True)
        return result[0] if result else None

    # Функции для работы с лотереей
    async def create_lottery_draw(self, draw_number: int) -> int:
        """Создание нового розыгрыша лотереи"""
        return await asyncio.to_thread(self._execute_query,
            "INSERT INTO lottery_draws (draw_number, draw_date, status) VALUES (?, datetime('now', '+1 hour'), 'active')",
            (draw_number,), commit=True)

    async def get_current_lottery_draw(self) -> Optional[Tuple]:
        """Получение текущего активного розыгрыша"""
        return await asyncio.to_thread(self._execute_query,
            "SELECT id, draw_number, winning_numbers, draw_date, status, total_tickets, total_prize_pool FROM lottery_draws WHERE status = 'active' ORDER BY draw_number DESC LIMIT 1",
            fetchone=True)

    async def get_lottery_draw_by_number(self, draw_number: int) -> Optional[Tuple]:
        """Получение розыгрыша по номеру"""
        return await asyncio.to_thread(self._execute_query,
            "SELECT id, draw_number, winning_numbers, draw_date, status, total_tickets, total_prize_pool FROM lottery_draws WHERE draw_number = ?",
            (draw_number,), fetchone=True)

    async def update_lottery_draw_winning_numbers(self, draw_number: int, winning_numbers: str):
        """Обновление выигрышных номеров розыгрыша"""
        await asyncio.to_thread(self._execute_query,
            "UPDATE lottery_draws SET winning_numbers = ?, status = 'completed' WHERE draw_number = ?",
            (winning_numbers, draw_number), commit=True)

    async def update_lottery_draw_stats(self, draw_number: int, total_tickets: int, total_prize_pool: float):
        """Обновление статистики розыгрыша"""
        await asyncio.to_thread(self._execute_query,
            "UPDATE lottery_draws SET total_tickets = ?, total_prize_pool = ? WHERE draw_number = ?",
            (total_tickets, total_prize_pool, draw_number), commit=True)

    async def purchase_lottery_tickets(self, user_id: int, telegram_id: int, draw_number: int, ticket_numbers: str) -> int:
        """Покупка билетов лотереи"""
        return await asyncio.to_thread(self._execute_query,
            "INSERT INTO lottery_tickets (user_id, telegram_id, draw_number, ticket_numbers) VALUES (?, ?, ?, ?)",
            (user_id, telegram_id, draw_number, ticket_numbers), commit=True)

    async def get_user_lottery_tickets(self, telegram_id: int, draw_number: Optional[int] = None) -> List[Tuple]:
        """Получение билетов пользователя"""
        if draw_number:
            return await asyncio.to_thread(self._execute_query,
                "SELECT id, user_id, telegram_id, draw_number, ticket_numbers, purchase_date, is_winner, winnings FROM lottery_tickets WHERE telegram_id = ? AND draw_number = ?",
                (telegram_id, draw_number), fetchall=True)
        else:
            return await asyncio.to_thread(self._execute_query,
                "SELECT id, user_id, telegram_id, draw_number, ticket_numbers, purchase_date, is_winner, winnings FROM lottery_tickets WHERE telegram_id = ? ORDER BY purchase_date DESC",
                (telegram_id,), fetchall=True)

    async def get_all_tickets_for_draw(self, draw_number: int) -> List[Tuple]:
        """Получение всех билетов для розыгрыша"""
        return await asyncio.to_thread(self._execute_query,
            "SELECT id, user_id, telegram_id, ticket_numbers FROM lottery_tickets WHERE draw_number = ?",
            (draw_number,), fetchall=True)

    async def mark_ticket_as_winner(self, ticket_id: int, winnings: float):
        """Отметка билета как выигрышного"""
        await asyncio.to_thread(self._execute_query,
            "UPDATE lottery_tickets SET is_winner = 1, winnings = ? WHERE id = ?",
            (winnings, ticket_id), commit=True)

    async def get_lottery_winners(self, draw_number: int) -> List[Tuple]:
        """Получение списка победителей розыгрыша"""
        return await asyncio.to_thread(self._execute_query,
            "SELECT lt.id, lt.user_id, lt.telegram_id, lt.ticket_numbers, lt.winnings, u.username FROM lottery_tickets lt JOIN users u ON lt.telegram_id = u.telegram_id WHERE lt.draw_number = ? AND lt.is_winner = 1",
            (draw_number,), fetchall=True)

    async def get_lottery_history(self, limit: int = 10) -> List[Tuple]:
        """Получение истории розыгрышей"""
        return await asyncio.to_thread(self._execute_query,
            "SELECT id, draw_number, winning_numbers, draw_date, status, total_tickets, total_prize_pool FROM lottery_draws ORDER BY draw_number DESC LIMIT ?",
            (limit,), fetchall=True)

    async def close(self):
        """Закрытие базы данных и ThreadPoolExecutor"""
        try:
            # Принудительно сохраняем все данные в базу
            await self._force_checkpoint()
            print("✅ Данные сохранены в базу данных")

            # Закрываем ThreadPoolExecutor
            if hasattr(self, 'executor') and self.executor:
                self.executor.shutdown(wait=True)
                print("✅ ThreadPoolExecutor закрыт")
        except Exception as e:
            print(f"⚠️ Ошибка при закрытии базы данных: {e}")

    async def _force_checkpoint(self):
        """Принудительное сохранение всех данных в базу"""
        try:
            # Включаем синхронный режим для гарантированного сохранения
            await asyncio.to_thread(self._execute_query, "PRAGMA synchronous=FULL;", commit=True)

            # Принудительно сохраняем данные на диск
            await asyncio.to_thread(self._execute_query, "PRAGMA wal_checkpoint(TRUNCATE);", commit=True)

            # Возвращаем нормальный режим
            await asyncio.to_thread(self._execute_query, "PRAGMA synchronous=NORMAL;", commit=True)

            print("💾 Контрольная точка WAL выполнена")
        except Exception as e:
            print(f"⚠️ Ошибка принудительного сохранения: {e}")

            # Альтернативный метод сохранения при ошибке
            try:
                await asyncio.to_thread(self._execute_query, "COMMIT;", commit=True)
                print("💾 Альтернативное сохранение выполнено")
            except Exception as e2:
                print(f"⚠️ Ошибка альтернативного сохранения: {e2}")

    async def _force_save(self):
        """Принудительное сохранение данных после важных изменений"""
        try:
            # Принудительно сохраняем изменения на диск
            await asyncio.to_thread(self._execute_query, "PRAGMA wal_checkpoint(PASSIVE);", commit=True)
        except Exception as e:
            # Игнорируем ошибки принудительного сохранения в обычных операциях
            pass

    # Функции для работы с тикетами поддержки
    async def create_support_ticket(self, telegram_id: int, username: str, issue: str) -> int:
        """Создание нового тикета поддержки"""
        user = await self.get_user(telegram_id)
        if not user:
            return None

        return await asyncio.to_thread(self._execute_query,
            "INSERT INTO support_tickets (user_id, telegram_id, username, issue, status) VALUES (?, ?, ?, ?, 'open')",
            (user[0], telegram_id, username, issue), commit=True)

    async def get_support_tickets(self, status: str = None, limit: int = 50) -> List[Tuple]:
        """Получение тикетов поддержки"""
        if status:
            return await asyncio.to_thread(self._execute_query,
                "SELECT id, user_id, telegram_id, username, issue, status, admin_id, admin_username, created_at, updated_at, closed_at, admin_response FROM support_tickets WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                (status, limit), fetchall=True)
        else:
            return await asyncio.to_thread(self._execute_query,
                "SELECT id, user_id, telegram_id, username, issue, status, admin_id, admin_username, created_at, updated_at, closed_at, admin_response FROM support_tickets ORDER BY created_at DESC LIMIT ?",
                (limit,), fetchall=True)

    async def get_user_support_tickets(self, telegram_id: int) -> List[Tuple]:
        """Получение тикетов пользователя"""
        return await asyncio.to_thread(self._execute_query,
            "SELECT id, user_id, telegram_id, username, issue, status, admin_id, admin_username, created_at, updated_at, closed_at, admin_response FROM support_tickets WHERE telegram_id = ? ORDER BY created_at DESC",
            (telegram_id,), fetchall=True)

    async def get_support_ticket(self, ticket_id: int) -> Optional[Tuple]:
        """Получение конкретного тикета"""
        return await asyncio.to_thread(self._execute_query,
            "SELECT id, user_id, telegram_id, username, issue, status, admin_id, admin_username, created_at, updated_at, closed_at, admin_response FROM support_tickets WHERE id = ?",
            (ticket_id,), fetchone=True)

    async def update_ticket_status(self, ticket_id: int, status: str, admin_id: int = None, admin_username: str = None, admin_response: str = None):
        """Обновление статуса тикета"""
        if admin_id and admin_username:
            await asyncio.to_thread(self._execute_query,
                "UPDATE support_tickets SET status = ?, admin_id = ?, admin_username = ?, admin_response = ?, updated_at = CURRENT_TIMESTAMP, closed_at = CURRENT_TIMESTAMP WHERE id = ?",
                (status, admin_id, admin_username, admin_response, ticket_id), commit=True)
        else:
            await asyncio.to_thread(self._execute_query,
                "UPDATE support_tickets SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (status, ticket_id), commit=True)

    async def set_user_cooldown(self, telegram_id: int, cooldown_minutes: int):
        """Установка cooldown для пользователя"""
        from datetime import datetime, timedelta
        cooldown_until = datetime.now() + timedelta(minutes=cooldown_minutes)
        await asyncio.to_thread(self._execute_query,
            "UPDATE support_tickets SET user_cooldown_until = ? WHERE telegram_id = ? AND status = 'rejected'",
            (cooldown_until.isoformat(), telegram_id), commit=True)

    async def check_user_cooldown(self, telegram_id: int) -> bool:
        """Проверка cooldown пользователя"""
        from datetime import datetime
        result = await asyncio.to_thread(self._execute_query,
            "SELECT user_cooldown_until FROM support_tickets WHERE telegram_id = ? AND user_cooldown_until > ? ORDER BY user_cooldown_until DESC LIMIT 1",
            (telegram_id, datetime.now().isoformat()), fetchone=True)
        return result is not None

    async def add_support_message(self, ticket_id: int, telegram_id: int, message: str, is_admin: int = 0) -> int:
        """Добавление сообщения в тикет"""
        return await asyncio.to_thread(self._execute_query,
            "INSERT INTO support_messages (ticket_id, telegram_id, message, is_admin) VALUES (?, ?, ?, ?)",
            (ticket_id, telegram_id, message, is_admin), commit=True)

    async def get_support_messages(self, ticket_id: int) -> List[Tuple]:
        """Получение сообщений тикета"""
        return await asyncio.to_thread(self._execute_query,
            "SELECT id, ticket_id, telegram_id, message, is_admin, created_at FROM support_messages WHERE ticket_id = ? ORDER BY created_at ASC",
            (ticket_id,), fetchall=True)

    async def get_open_tickets_count(self) -> int:
        """Получение количества открытых тикетов"""
        result = await asyncio.to_thread(self._execute_query,
            "SELECT COUNT(*) FROM support_tickets WHERE status = 'open'",
            fetchone=True)
        return result[0] if result else 0

    # Функции для работы с короткими реферальными кодами
    async def create_referral_code(self, telegram_id: int) -> str:
        """Создание короткого реферального кода для пользователя"""
        import random
        import string

        # Проверяем, есть ли уже код для этого пользователя
        existing_code = await asyncio.to_thread(self._execute_query,
            "SELECT short_code FROM referral_codes WHERE telegram_id = ?",
            (telegram_id,), fetchone=True)

        if existing_code:
            return existing_code[0]

        # Генерируем короткий код (6 символов)
        while True:
            short_code = ''.join(random.choices(string.ascii_letters + string.digits, k=6))

            # Проверяем, что код уникальный
            existing = await asyncio.to_thread(self._execute_query,
                "SELECT id FROM referral_codes WHERE short_code = ?",
                (short_code,), fetchone=True)

            if not existing:
                break

        # Сохраняем код в базу данных
        await asyncio.to_thread(self._execute_query,
            "INSERT INTO referral_codes (telegram_id, short_code) VALUES (?, ?)",
            (telegram_id, short_code), commit=True)

        # Проверяем, что код действительно сохранился
        check_result = await asyncio.to_thread(self._execute_query,
            "SELECT short_code FROM referral_codes WHERE telegram_id = ? AND short_code = ?",
            (telegram_id, short_code), fetchone=True)

        if check_result:
            print(f"✅ Короткий код {short_code} успешно сохранен для пользователя {telegram_id}")
        else:
            print(f"❌ Ошибка сохранения кода {short_code} для пользователя {telegram_id}")

        return short_code

    async def get_telegram_id_by_referral_code(self, short_code: str) -> Optional[int]:
        """Получение telegram_id по короткому реферальному коду"""
        try:
            result = await asyncio.to_thread(self._execute_query,
                "SELECT telegram_id FROM referral_codes WHERE short_code = ?",
                (short_code,), fetchone=True)
            print(f"🔍 Поиск кода {short_code} в БД, результат: {result}")
            return result[0] if result else None
        except Exception as e:
            print(f"❌ Ошибка поиска кода {short_code}: {e}")
            return None
