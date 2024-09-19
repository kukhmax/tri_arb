import math
from decimal import Decimal
import logging
from config import web3, QUOTER_ADDRESS, QUOTER_ABI, PRICE_CHANGE_LIMIT_PERCENTAGE
from token_utils import get_token_info

def sqrt_price_x96_to_price(sqrt_price_x96):
    """
    Преобразует sqrtPriceX96 в обычную цену.

    Parameters
    ----------
    sqrt_price_x96 : int
        Текущая цена в формате sqrtPriceX96.

    Returns
    -------
    float
        Преобразованная цена.
    """
    return (sqrt_price_x96 ** 2) / (2 ** 192)

def calculate_sqrt_price_limit_x96(sqrt_price_x96, percentage_limit):
    """
    Рассчитывает лимит изменения цены для sqrtPriceX96.

    Parameters
    ----------
    sqrt_price_x96 : int
        Текущая sqrtPriceX96.
    percentage_limit : float
        Процентное ограничение изменения цены.

    Returns
    -------
    int
        Новый лимит sqrtPriceX96.
    """
    price_change_factor = 1 + (percentage_limit / 100)
    new_sqrt_price_x96 = sqrt_price_x96 * math.sqrt(price_change_factor)
    return int(new_sqrt_price_x96)

def get_quote(quoter_contract, token_in, token_out, fee, amount_in, sqrt_price_limit_x96):
    """
    Получает котировку из контракта Quoter.

    Parameters
    ----------
    quoter_contract : web3.eth.Contract
        Контракт Quoter.
    token_in : str
        Адрес токена, который будет передан.
    token_out : str
        Адрес токена, который будет получен.
    fee : int
        Комиссия пула.
    amount_in : int
        Количество токенов, которые будут переданы.
    sqrt_price_limit_x96 : int
        Ограничение цены в формате sqrtPriceX96.

    Returns
    -------
    int or None
        Количество токенов, которые будут получены. None, если произошла ошибка.
    """
    try:
        quoted_amount_out = quoter_contract.functions.quoteExactInputSingle(
            token_in, 
            token_out, 
            fee, 
            amount_in, 
            sqrt_price_limit_x96
        ).call()
        return quoted_amount_out
    except Exception as e:
        logging.error(f"Ошибка при получении котировки: {e}")
        return None

# Функция для расчёта средней цены свопа с учётом ликвидности и проскальзывания
def calculate_average_price(
    pool_contract, 
    amount_in, 
    trade_direction, 
    quoter_contract, 
    price_change_limit_percentage=PRICE_CHANGE_LIMIT_PERCENTAGE
):
    try:
        # Получаем адреса токенов и комиссию пула
        token0 = pool_contract.functions.token0().call()
        token1 = pool_contract.functions.token1().call()
        fee = pool_contract.functions.fee().call()
        
        # Получаем информацию о токенах
        token0_info = get_token_info(token0)
        token1_info = get_token_info(token1)
        
        # Определяем направление свопа
        if trade_direction == "baseToQuote":
            input_token = token0
            input_decimals = token0_info["decimals"]
            output_token = token1
            output_decimals = token1_info["decimals"]
        elif trade_direction == "quoteToBase":
            input_token = token1
            input_decimals = token1_info["decimals"]
            output_token = token0
            output_decimals = token0_info["decimals"]
        else:
            logging.warning(f"Неизвестное направление свопа: {trade_direction}")
            return None
        
        # Преобразуем amount_in в нужные единицы
        amount_in_wei = web3.to_wei(amount_in, 'ether') if input_decimals == 18 else int(amount_in * (10 ** input_decimals))
        
        # Получаем текущую ликвидность и цену
        liquidity = pool_contract.functions.liquidity().call()
        if liquidity == 0:
            logging.warning(f"Ликвидность пула равна нулю. Пропуск свопа.")
            return None
        
        slot0 = pool_contract.functions.slot0().call()
        sqrt_price_x96 = slot0[0]
        current_price = sqrt_price_x96_to_price(sqrt_price_x96)
        
        # Вычисляем допустимое изменение цены (например, 1%)
        sqrt_price_limit_x96 = calculate_sqrt_price_limit_x96(sqrt_price_x96, price_change_limit_percentage)
        
        # Получаем котировку
        quoted_amount_out = get_quote(
            quoter_contract, 
            input_token, 
            output_token, 
            fee, 
            amount_in_wei, 
            sqrt_price_limit_x96
        )
        
        if quoted_amount_out is None:
            logging.error(f"Не удалось получить котировку.")
            return None
        
        # Преобразуем количество выходных токенов в удобочитаемый формат
        amount_out = web3.from_wei(quoted_amount_out, 'ether') if output_decimals == 18 else quoted_amount_out / (10 ** output_decimals)
        
        # Пример приведения типов перед делением
        amount_out_decimal = Decimal(amount_out)  # Приведение к Decimal
        amount_in_decimal = Decimal(amount_in)  # Приведение к Decimal

        average_price = amount_out_decimal / amount_in_decimal
        
        logging.info(f"Своп {amount_in} {token0_info['symbol'] if trade_direction == 'baseToQuote' else token1_info['symbol']} на {amount_out} {token1_info['symbol'] if trade_direction == 'baseToQuote' else token0_info['symbol']}")
        logging.info(f"Средняя цена {token0_info['symbol']}_{token1_info['symbol']}: {average_price}")
        
        return average_price
    except Exception as e:
        logging.error(f"Ошибка при расчёте средней цены: {e}")
        return None