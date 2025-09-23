import requests
from config import CRYPTO_BOT_TOKEN, CRYPTO_BOT_API

class CryptoBotAPI:
    def __init__(self):
        self.token = CRYPTO_BOT_TOKEN
        self.base_url = CRYPTO_BOT_API
        self.headers = {
            'Crypto-Pay-API-Token': self.token,
            'Content-Type': 'application/json'
        }

    def create_invoice(self, amount, description="Пополнение баланса VanishCasino", currency_type="crypto", fiat="USD", accepted_assets=None, expires_in=None):
        """
        Создание инвойса с поддержкой новых возможностей API 1.5.1

        Args:
            amount (float): Сумма платежа
            description (str): Описание платежа
            currency_type (str): Тип валюты ('crypto' или 'fiat')
            fiat (str): Валюта (USD, EUR, etc.)
            accepted_assets (list): Принимаемые криптовалюты
            expires_in (int): Время жизни инвойса в секундах
        """
        url = f"{self.base_url}/createInvoice"
        payload = {
            "asset": "USDT",
            "amount": str(amount),
            "description": description,
            "currency_type": currency_type,
            "fiat": fiat,
            "paid_btn_name": "viewItem",
            "paid_btn_url": "https://t.me/VanishCasinoBot"
        }

        # Добавляем поддерживаемые криптовалюты
        if accepted_assets:
            payload["accepted_assets"] = accepted_assets
        else:
            payload["accepted_assets"] = ["USDT", "BTC", "ETH", "LTC", "TON"]

        # Добавляем время жизни инвойса
        if expires_in:
            payload["expires_in"] = expires_in

        try:
            response = requests.post(url, json=payload, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Ошибка API: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Ошибка создания инвойса: {e}")
        return None
    
    def get_invoices(self, invoice_ids=None):
        url = f"{self.base_url}/getInvoices"
        params = {}
        if invoice_ids:
            params["invoice_ids"] = ','.join(invoice_ids)

        print(f"Запрос инвойсов: {params}")  # Отладка

        try:
            response = requests.get(url, params=params, headers=self.headers)
            print(f"Ответ от Crypto Bot: {response.status_code}")  # Отладка
            if response.status_code == 200:
                result = response.json()
                print(f"Данные инвойсов: {result}")  # Отладка
                return result
        except Exception as e:
            print(f"Ошибка получения инвойсов: {e}")
        return None

    def create_transfer(self, user_id, asset="USDT", amount=None, spend_id=None, comment=None, disable_send_notification=None):
        """Создание перевода средств между пользователями бота"""
        url = f"{self.base_url}/transfer"
        payload = {
            "user_id": user_id,
            "asset": asset,
            "amount": str(amount) if amount is not None else None,
            "spend_id": spend_id,
            "comment": comment,
            "disable_send_notification": disable_send_notification
        }

        # Убираем None значения
        payload = {k: v for k, v in payload.items() if v is not None}

        print(f"Создание перевода: {payload}")  # Отладка

        try:
            response = requests.post(url, json=payload, headers=self.headers)
            print(f"Ответ от Crypto Bot: {response.status_code}")  # Отладка
            if response.status_code == 200:
                result = response.json()
                print(f"Результат перевода: {result}")  # Отладка
                return result
            else:
                # Обработка ошибок HTTP
                try:
                    error_data = response.json()
                    print(f"Ошибка API: {error_data}")
                    return {"error": error_data}
                except:
                    print(f"Ошибка HTTP {response.status_code}: {response.text}")
                    return {"error": f"HTTP {response.status_code}: {response.text}"}

        except requests.exceptions.RequestException as e:
            print(f"Ошибка сети при создании перевода: {e}")
            return {"error": f"Network error: {str(e)}"}
        except Exception as e:
            print(f"Неожиданная ошибка создания перевода: {e}")
            return {"error": f"Unexpected error: {str(e)}"}
        return None

    def get_balance(self, asset="USDT"):
        """Получение баланса бота"""
        url = f"{self.base_url}/getBalance"
        params = {"asset": asset}

        try:
            response = requests.get(url, params=params, headers=self.headers)
            print(f"Баланс бота: {response.status_code}")  # Отладка
            if response.status_code == 200:
                result = response.json()
                print(f"Баланс: {result}")  # Отладка
                return result
        except Exception as e:
            print(f"Ошибка получения баланса: {e}")
        return None

    def get_wallet_address(self):
        """Получение адреса кошелька бота для внешних переводов"""
        # В реальной ситуации здесь нужно получить адрес кошелька бота
        # из настроек или API Crypto Bot
        # Пока возвращаем заглушку
        return "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWZh"  # Пример TRC20 адреса

    def get_stats(self):
        """Получение статистики приложения (API 1.3+)"""
        url = f"{self.base_url}/getStats"

        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Ошибка получения статистики: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Ошибка получения статистики: {e}")
        return None

    def create_check(self, user_id, asset="USDT", amount=None, pin_to_user_id=None, pin_to_username=None):
        """Создание чека для передачи средств (API 1.2+)"""
        url = f"{self.base_url}/createCheck"
        payload = {
            "asset": asset,
            "amount": str(amount) if amount is not None else None,
        }

        # Добавляем получателя чека
        if pin_to_user_id:
            payload["pin_to_user_id"] = pin_to_user_id
        elif pin_to_username:
            payload["pin_to_username"] = pin_to_username

        # Убираем None значения
        payload = {k: v for k, v in payload.items() if v is not None}

        try:
            response = requests.post(url, json=payload, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Ошибка создания чека: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Ошибка создания чека: {e}")
        return None

    def delete_check(self, check_id):
        """Удаление чека (API 1.2+)"""
        url = f"{self.base_url}/deleteCheck"
        payload = {"check_id": check_id}

        try:
            response = requests.post(url, json=payload, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Ошибка удаления чека: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Ошибка удаления чека: {e}")
        return None

    def get_checks(self, check_ids=None, user_id=None):
        """Получение списка чеков (API 1.2+)"""
        url = f"{self.base_url}/getChecks"
        params = {}

        if check_ids:
            params["check_ids"] = ','.join(check_ids)
        if user_id:
            params["user_id"] = user_id

        try:
            response = requests.get(url, params=params, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Ошибка получения чеков: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Ошибка получения чеков: {e}")
        return None

    def delete_invoice(self, invoice_id):
        """Удаление инвойса (API 1.2+)"""
        url = f"{self.base_url}/deleteInvoice"
        payload = {"invoice_id": invoice_id}

        try:
            response = requests.post(url, json=payload, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Ошибка удаления инвойса: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Ошибка удаления инвойса: {e}")
        return None

    def get_transfers(self, asset="USDT", spend_id=None, offset=None, count=None):
        """Получение списка переводов (API 1.2+)"""
        url = f"{self.base_url}/getTransfers"
        params = {"asset": asset}

        if spend_id:
            params["spend_id"] = spend_id
        if offset:
            params["offset"] = offset
        if count:
            params["count"] = count

        try:
            response = requests.get(url, params=params, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Ошибка получения переводов: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Ошибка получения переводов: {e}")
        return None

    def get_me(self):
        """Получение информации о приложении"""
        url = f"{self.base_url}/getMe"

        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Ошибка получения информации: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Ошибка получения информации: {e}")
        return None

crypto_bot = CryptoBotAPI()