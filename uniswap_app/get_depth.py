import os
import json
import logging
import sys
import math
from decimal import Decimal
from datetime import datetime
from web3 import Web3
from colorama import Fore, Style, init


# Инициализация colorama для поддержки цвета в консоли на Windows
init(autoreset=True)

# Устанавливаем формат даты для логов
log_filename = datetime.now().strftime("%Y-%m-%d_triangular_arbitrage.log")

# Настройка логирования
logging.basicConfig(
    filename=log_filename,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s - [%(funcName)s:%(lineno)d]",  # Формат логов с именем функции и номером строки
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Добавляем консольный логгер с цветным выводом
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s - [%(funcName)s:%(lineno)d]", "%Y-%m-%d %H:%M:%S")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# Подключение к Ethereum через Infura
INFURA_ID = os.getenv('INFURA_ID')
INFURA_URL = F"https://mainnet.infura.io/v3/{INFURA_ID}"
web3 = Web3(Web3.HTTPProvider(INFURA_URL))

if not web3.is_connected():
    logging.error(f"{Fore.RED}Не удалось подключиться к Ethereum через Infura.")
    sys.exit(1)
else:
    logging.info(f"{Fore.GREEN}Подключение к Ethereum через Infura успешно.")

# ABI для пула Uniswap V3 и контракта Quoter
POOL_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "token0",
        "outputs": [{"name": "", "type": "address"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "token1",
        "outputs": [{"name": "", "type": "address"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "fee",
        "outputs": [{"name": "", "type": "uint24"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "liquidity",
        "outputs": [{"name": "", "type": "uint128"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"name": "sqrtPriceX96", "type": "uint160"},
            {"name": "tick", "type": "int24"},
            {"name": "observationIndex", "type": "uint16"},
            {"name": "observationCardinality", "type": "uint16"},
            {"name": "observationCardinalityNext", "type": "uint16"},
            {"name": "feeProtocol", "type": "uint8"},
            {"name": "unlocked", "type": "bool"}
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    }
]

QUOTER_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "tokenIn", "type": "address"},
            {"internalType": "address", "name": "tokenOut", "type": "address"},
            {"internalType": "uint24", "name": "fee", "type": "uint24"},
            {"internalType": "uint256", "name": "amountIn", "type": "uint256"},
            {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"}
        ],
        "name": "quoteExactInputSingle",
        "outputs": [{"internalType": "uint256", "name": "amountOut", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    }
]

# Адрес контракта Quoter Uniswap V3
QUOTER_ADDRESS = web3.to_checksum_address('0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6')
PRICE_CHANGE_LIMIT_PERCENTAGE = 1  # процент проскальзывания цены для свопа

# Функция для получения информации о токенах
def get_token_info(token_address):
    # Стандартный ABI для ERC20
    ERC20_ABI = [
        {"constant": True, "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
        {"constant": True, "name": "name", "outputs": [{"name": "", "type": "string"}], "type": "function"},
        {"constant": True, "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"}
    ]
    token_contract = web3.eth.contract(address=token_address, abi=ERC20_ABI)
    try:
        symbol = token_contract.functions.symbol().call()
        name = token_contract.functions.name().call()
        decimals = token_contract.functions.decimals().call()
        return {"symbol": symbol, "name": name, "decimals": decimals}
    except Exception as e:
        logging.error(f"{Fore.RED}Ошибка при получении информации о токене {token_address}: {e}")
        return {"symbol": "UNKNOWN", "name": "UNKNOWN", "decimals": 18}
# Функция для получения информации о токене (symbol, name, decimals)
def get_token_info(token_address):
    """
    Функция для получения информации о токене (symbol, name, decimals).

    Parameters
    ----------
    token_address : str
        Адрес токена в формате checksum.

    Returns
    -------
    dict
        Словарь с информацией о токене:
            - symbol : str, символ токена (например, "ETH")
            - name : str, полное имя токена (например, "Ethereum")
            - decimals : int, количество знаков после запятой (например, 18)
    """
    ERC20_ABI = [
        {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
        {"constant": True, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}], "type": "function"},
        {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"}
    ]
    token_contract = web3.eth.contract(address=token_address, abi=ERC20_ABI)
    try:
        symbol = token_contract.functions.symbol().call()
        name = token_contract.functions.name().call()
        decimals = token_contract.functions.decimals().call()
        return {"symbol": symbol, "name": name, "decimals": decimals}
    except Exception as e:
        logging.error(f"{Fore.RED}Ошибка при получении информации о токене {token_address}: {e}")
        return {"symbol": "UNKNOWN", "name": "UNKNOWN", "decimals": 18}

# Функция для преобразования sqrtPriceX96 в обычную цену
def sqrt_price_x96_to_price(sqrt_price_x96):
    return (sqrt_price_x96 ** 2) / (2 ** 192)

# Расчёт лимита для изменения цены
def calculate_sqrt_price_limit_x96(sqrt_price_x96, percentage_limit):
    # Пример: изменение цены на 1% -> percentage_limit = 1
    """
    Calculate sqrtPriceX96 limit for price change.

    Parameters
    ----------
    sqrt_price_x96 : int
        Current sqrtPriceX96 value.
    percentage_limit : float
        Percentage limit for price change (e.g. 1 for 1% change).

    Returns
    -------
    int
        New sqrtPriceX96 value that represents the price change limit.
    """
    price_change_factor = 1 + (percentage_limit / 100)
    new_sqrt_price_x96 = sqrt_price_x96 * math.sqrt(price_change_factor)
    return int(new_sqrt_price_x96)

# Функция для получения котировки из Quoter
def get_quote(quoter_contract, token_in, token_out, fee, amount_in, sqrt_price_limit_x96):
    """
    Функция для получения котировки из Quoter.

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
        logging.error(f"{Fore.RED}Ошибка при получении котировки: {e}")
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
            logging.warning(f"{Fore.YELLOW}Неизвестное направление свопа: {trade_direction}")
            return None
        
        # Преобразуем amount_in в нужные единицы
        amount_in_wei = web3.to_wei(amount_in, 'ether') if input_decimals == 18 else int(amount_in * (10 ** input_decimals))
        
        # Получаем текущую ликвидность и цену
        liquidity = pool_contract.functions.liquidity().call()
        if liquidity == 0:
            logging.warning(f"{Fore.YELLOW}Ликвидность пула равна нулю. Пропуск свопа.")
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
            logging.error(f"{Fore.RED}Не удалось получить котировку.")
            return None
        
        # Преобразуем количество выходных токенов в удобочитаемый формат
        amount_out = web3.from_wei(quoted_amount_out, 'ether') if output_decimals == 18 else quoted_amount_out / (10 ** output_decimals)
        
        # Пример приведения типов перед делением
        amount_out_decimal = Decimal(amount_out)  # Приведение к Decimal
        amount_in_decimal = Decimal(amount_in)  # Приведение к Decimal

        average_price = amount_out_decimal / amount_in_decimal
        
        logging.info(f"{Fore.GREEN}Своп {amount_in} {token0_info['symbol'] if trade_direction == 'baseToQuote' else token1_info['symbol']} на {amount_out} {token1_info['symbol'] if trade_direction == 'baseToQuote' else token0_info['symbol']}")
        logging.info(f"{Fore.GREEN}Средняя цена {token0_info['symbol']}_{token1_info['symbol']}: {average_price}")
        
        return average_price
    except Exception as e:
        logging.error(f"{Fore.RED}Ошибка при расчёте средней цены: {e}")
        return None

# Основная функция для обработки треугольных арбитражных возможностей
def process_triangles(json_file_path, amount_in, limit=1):
    try:
        # Чтение JSON-файла
        with open(json_file_path, 'r') as f:
            triangles = json.load(f)
            logging.info(f"{Fore.LIGHTYELLOW_EX}Загружено {len(triangles)} треугольников.")
    except Exception as e:
        logging.error(f"{Fore.RED}Ошибка при чтении файла {json_file_path}: {e}")
        return
    
    # Ограничение количества обрабатываемых треугольников
    triangles_to_process = triangles[:limit]
    
    # Создание объекта контракта Quoter
    quoter_contract = web3.eth.contract(address=QUOTER_ADDRESS, abi=QUOTER_ABI)
    
    # Итерация по каждому треугольнику
    for idx, triangle in enumerate(triangles_to_process, start=1):
        logging.info(f"{Fore.CYAN}\nОбработка треугольника {idx}:")
        try:
            pool1_address = web3.to_checksum_address(triangle["poolContract1"])
            pool2_address = web3.to_checksum_address(triangle["poolContract2"])
            pool3_address = web3.to_checksum_address(triangle["poolContract3"])
            
            trade1_direction = triangle["poolDirectionTrade1"]
            trade2_direction = triangle["poolDirectionTrade2"]
            trade3_direction = triangle["poolDirectionTrade3"]
            
            # Создание объектов контрактов для каждой пары
            pool1 = web3.eth.contract(address=pool1_address, abi=POOL_ABI)
            pool2 = web3.eth.contract(address=pool2_address, abi=POOL_ABI)
            pool3 = web3.eth.contract(address=pool3_address, abi=POOL_ABI)
            
            # Расчёт средней цены для каждой своп-пары
            average_price1 = calculate_average_price(pool1, amount_in, trade1_direction, quoter_contract)
            if average_price1 is None:
                logging.error(f"{Fore.RED}Ошибка при расчёте свопа для первой пары: {triangle['swap1'], triangle['swap2']}.")
                continue
            
            # Новое количество после первого свопа
            amount_after_swap1 = amount_in * average_price1
            
            average_price2 = calculate_average_price(pool2, amount_after_swap1, trade2_direction, quoter_contract)
            if average_price2 is None:
                logging.error(f"{Fore.RED}Ошибка при расчёте свопа для второй пары: {triangle['swap2'], triangle['swap3']}.")
                continue
            
            # Новое количество после второго свопа
            amount_after_swap2 = amount_after_swap1 * average_price2
            
            average_price3 = calculate_average_price(pool3, amount_after_swap2, trade3_direction, quoter_contract)
            if average_price3 is None:
                logging.error(f"{Fore.RED}Ошибка при расчёте свопа для третьей пары: {triangle['swap3'], triangle['swap1']}.")
                continue
            
            # Финальное количество после третьего свопа
            final_amount = amount_after_swap2 * average_price3
            
            # Расчёт прибыли или убытка
            profit = final_amount - amount_in
            profit_percentage = (profit / amount_in) * 100
            
            logging.info(f"{Fore.YELLOW}Начальный объём: {amount_in}")
            logging.info(f"{Fore.YELLOW}Финальный объём: {final_amount}")
            logging.info(f"{Fore.YELLOW}Прибыль: {profit} ({profit_percentage:.2f}%)")
            
            if profit > 0:
                logging.info(f"{Fore.GREEN}Возможность арбитража: Профит!")
            else:
                logging.info(f"{Fore.RED}Возможность арбитража: Нет профита.")
        except Exception as e:
            logging.error(f"{Fore.RED}Ошибка при обработке треугольника {idx}: {e}")


# Пример использования
if __name__ == "__main__":
    # Путь к вашему JSON-файлу с треугольными арбитражными возможностями
    JSON_FILE_PATH = "uniswap_surface_rates.json"
    
    # Объём для свопа (например, 1 токен base)
    AMOUNT_IN = 1 # Задайте нужный объём
    
    # Лимит количества треугольников для обработки
    LIMIT = 20  # Задайте нужное значение
    
    process_triangles(JSON_FILE_PATH, AMOUNT_IN, LIMIT)












# from web3 import Web3
# from eth_abi import decode
# import json

# # Подключение к Infura (замените YOUR_INFURA_PROJECT_ID на свой)
# infura_url = "https://mainnet.infura.io/v3/556177ea959e4d59bafb898c06f7"
# web3 = Web3(Web3.HTTPProvider(infura_url))

# # ABI для контракта пула Uniswap V3. Это минимальное ABI, содержащее только нужные функции.
# # Вы можете получить полное ABI пула на etherscan.io.
# # Например, для пула: https://etherscan.io/address/0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8#code
# pool_abi = json.loads("""
# [
#     {"constant": true, "inputs": [], "name": "token0", "outputs": [{"name": "", "type": "address"}], "payable": false, "stateMutability": "view", "type": "function"},
#     {"constant": true, "inputs": [], "name": "token1", "outputs": [{"name": "", "type": "address"}], "payable": false, "stateMutability": "view", "type": "function"},
#     {"constant": true, "inputs": [], "name": "fee", "outputs": [{"name": "", "type": "uint24"}], "payable": false, "stateMutability": "view", "type": "function"},
#     {"constant": true, "inputs": [], "name": "liquidity", "outputs": [{"name": "", "type": "uint128"}], "payable": false, "stateMutability": "view", "type": "function"},
#     {"constant": true, "inputs": [], "name": "slot0", "outputs": [
#         {"name": "sqrtPriceX96", "type": "uint160"},
#         {"name": "tick", "type": "int24"},
#         {"name": "observationIndex", "type": "uint16"},
#         {"name": "observationCardinality", "type": "uint16"},
#         {"name": "observationCardinalityNext", "type": "uint16"},
#         {"name": "feeProtocol", "type": "uint8"},
#         {"name": "unlocked", "type": "bool"}
#     ], "payable": false, "stateMutability": "view", "type": "function"}
# ]
# """)

# # Указываем адрес пула. Например, USDC/WETH пул на Uniswap V3 имеет адрес:
# # https://etherscan.io/address/0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8
# pool_address = web3.to_checksum_address('0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8')


# # Создаем объект контракта для пула Uniswap V3
# pool_contract = web3.eth.contract(address=pool_address, abi=pool_abi)

# # Получаем адреса токенов, участвующих в пуле, и комиссию пула
# token0_address = pool_contract.functions.token0().call()
# token1_address = pool_contract.functions.token1().call()
# fee = pool_contract.functions.fee().call()

# print(f"Token0 Address: {token0_address}")
# print(f"Token1 Address: {token1_address}")
# print(f"Fee (in basis points): {fee}")

# # Получаем информацию о ликвидности пула
# liquidity = pool_contract.functions.liquidity().call()
# print(f"Liquidity in pool: {liquidity}")

# # Получаем данные slot0 для вычисления текущей цены
# slot0_data = pool_contract.functions.slot0().call()
# sqrtPriceX96 = slot0_data[0]
# print(f"Current sqrtPriceX96: {sqrtPriceX96}")

# # Преобразуем sqrtPriceX96 в обычную цену
# price = (sqrtPriceX96 ** 2) / (2 ** 192)
# print(f"Current price (token0/token1): {price}")

# # ABI контракта Quoter, который используется для получения котировок. Также можно найти на Etherscan.
# quoter_abi = json.loads("""
# [
#     {
#         "inputs": [
#             {"internalType": "address", "name": "tokenIn", "type": "address"},
#             {"internalType": "address", "name": "tokenOut", "type": "address"},
#             {"internalType": "uint24", "name": "fee", "type": "uint24"},
#             {"internalType": "uint256", "name": "amountIn", "type": "uint256"},
#             {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"}
#         ],
#         "name": "quoteExactInputSingle",
#         "outputs": [{"internalType": "uint256", "name": "amountOut", "type": "uint256"}],
#         "stateMutability": "view",
#         "type": "function"
#     }
# ]
# """)

# # Адрес контракта Quoter Uniswap V3 (постоянный для всех сетей)
# quoter_address = Web3.toChecksumAddress('0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6')
# quoter_contract = web3.eth.contract(address=quoter_address, abi=quoter_abi)

# # Функция для получения котировки из Uniswap V3
# def get_quote(token_in, token_out, fee, amount_in, sqrt_price_limit_x96):
#     try:
#         # Вызов функции quoteExactInputSingle из контракта Quoter
#         quoted_amount_out = quoter_contract.functions.quoteExactInputSingle(
#             token_in, 
#             token_out, 
#             fee, 
#             amount_in, 
#             sqrt_price_limit_x96
#         ).call()
#         return quoted_amount_out
#     except Exception as e:
#         print(f"Error in quoting: {str(e)}")
#         return None

# # Пример использования функции для получения котировки
# # Например, мы хотим получить котировку для обмена 1 токена token0 на token1
# amount_in = Web3.toWei(1, 'ether')  # Примерный объем входного токена (в ETH)
# sqrt_price_limit_x96 = 0  # Лимит цены (может быть 0 для отсутствия ограничения)

# # Получаем котировку
# quoted_amount = get_quote(token0_address, token1_address, fee, amount_in, sqrt_price_limit_x96)
# if quoted_amount:
#     print(f"Quoted amount out: {Web3.fromWei(quoted_amount, 'ether')} token1")
# else:
#     print("Failed to get a quote.")