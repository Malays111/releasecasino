import sqlite3
import asyncio
from typing import Optional, Tuple, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

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
            "SELECT id, telegram_id, username, balance, referral_count, COALESCE(referral_balance, 0) as referral_balance, COALESCE(total_deposited, 0) as total_deposited, COALESCE(total_spent, 0) as total_spent, COALESCE(games_played, 0) as games_played, referrer_id, referral_bonus_given, last_daily_task_completed, COALESCE(active_referrals_count, 0) as active_referrals_count, created_at FROM users WHERE telegram_id = ?",
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
            "SELECT id, telegram_id, username, balance, referral_count, COALESCE(referral_balance, 0) as referral_balance, COALESCE(total_deposited, 0) as total_deposited, COALESCE(total_spent, 0) as total_spent, COALESCE(games_played, 0) as games_played, referrer_id, referral_bonus_given, last_daily_task_completed, COALESCE(active_referrals_count, 0) as active_referrals_count, created_at FROM users WHERE username = ?",
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
            current_referrer_id = existing_user[0]
            if current_referrer_id is None and referrer_id is not None:
                # Устанавливаем referrer_id, если его нет
                await asyncio.to_thread(self._execute_query,
                    "UPDATE users SET referrer_id = ?, referral_bonus_given = 0 WHERE telegram_id = ?",
                    (referrer_id, telegram_id), commit=True)
                # Обновляем счетчик рефералов
                await asyncio.to_thread(self._execute_query,
                    "UPDATE users SET referral_count = COALESCE(referral_count, 0) + 1 WHERE telegram_id = ?",
                    (referrer_id,), commit=True)
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

            except sqlite3.OperationalError:
                # Если колонки нет, вставляем без них
                await asyncio.to_thread(self._execute_query,
                    "INSERT INTO users (telegram_id, username) VALUES (?, ?)",
                    (telegram_id, username), commit=True)

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

    async def update_referral_balance(self, telegram_id: int, amount: float):
        try:
            amount = float(amount)
        except (ValueError, TypeError):
            return

        await asyncio.to_thread(self._execute_query,
            "UPDATE users SET referral_balance = COALESCE(referral_balance, 0) + ? WHERE telegram_id = ?",
            (amount, telegram_id), commit=True)

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
        return await asyncio.to_thread(self._execute_query,
            "INSERT INTO payments (user_id, amount, crypto_bot_invoice_id, status, message_id, chat_id) VALUES (?, ?, ?, 'pending', ?, ?)",
            (user_id, amount, invoice_id, message_id, chat_id), commit=True)

    async def update_payment_status(self, invoice_id: str, status: str):
        await asyncio.to_thread(self._execute_query,
            "UPDATE payments SET status = ? WHERE crypto_bot_invoice_id = ?",
            (status, invoice_id), commit=True)

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

    async def get_setting(self, key: str, default_value: Optional[str] = None) -> Optional[str]:
        result = await asyncio.to_thread(self._execute_query,
            "SELECT setting_value FROM text_settings WHERE setting_key = ?",
            (key,), fetchone=True)
        return result[0] if result else default_value

    async def create_withdrawal(self, user_id: int, amount: float, wallet_address: str) -> int:
        return await asyncio.to_thread(self._execute_query,
            "INSERT INTO withdrawals (user_id, amount, wallet_address) VALUES (?, ?, ?)",
            (user_id, amount, wallet_address), commit=True)

    async def update_withdrawal_status(self, withdrawal_id: int, status: str, transfer_id: Optional[str] = None):
        if transfer_id:
            await asyncio.to_thread(self._execute_query,
                "UPDATE withdrawals SET status = ?, crypto_bot_transfer_id = ? WHERE id = ?",
                (status, transfer_id, withdrawal_id), commit=True)
        else:
            await asyncio.to_thread(self._execute_query,
                "UPDATE withdrawals SET status = ? WHERE id = ?",
                (status, withdrawal_id), commit=True)

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
